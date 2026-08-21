package engine

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// counters are the hot-path metrics for one component.
//
// Atomics, not a mutex-guarded struct: at 10k msg/s across many producers, a lock on
// every counter increment is real contention.
//
// Snapshot() reading slightly-skewed values across fields is fine for a dashboard. If a
// genuinely consistent cut is ever needed this can become a seqlock, but nothing needs
// that today.
type counters struct {
	sent      atomic.Int64
	bytes     atomic.Int64
	duplicate atomic.Int64
	consumed  atomic.Int64
	failed    atomic.Int64 // simulated consumer failures
	dlq       atomic.Int64
	retries   atomic.Int64
	commitErr atomic.Int64

	// produceErr and consumeErr are separate, independent counters because TopicStat
	// exposes both. Folding them into one field would surface every fetch error and every
	// failed DLQ write as a produce error while ConsumeErr stayed permanently zero.
	produceErr atomic.Int64
	consumeErr atomic.Int64

	// brokerLag is the most recent real consumer-group lag for this topic, keyed by group
	// id, or nil when lag polling is off or the last poll could not answer for any group.
	//
	// A pointer to a whole map rather than a mutex-guarded map: the lag poller builds each
	// tick's map from scratch and never touches it after Store, so a reader sees one
	// complete reading or the previous complete reading, never a partial one. It lives on
	// counters -- which is per topic, built once and read-only thereafter -- so the poller
	// needs no lock and no new engine-level field. See lag.go.
	brokerLag atomic.Pointer[map[string]int64]
}

// TopicStat is the per-topic slice of a Snapshot.
//
// Everything measured is exposed here, including bytes, errors, duplicates, retries and
// commit failures; the output layer chooses what to render.
type TopicStat struct {
	Topic    string
	Scenario string

	Produced   int64
	Consumed   int64
	Bytes      int64
	ProduceErr int64
	ConsumeErr int64
	Duplicates int64

	// CommitErr counts offsets khaos declined to commit or failed to commit.
	CommitErr int64

	// Failed, DLQ and Retries are only meaningful when failure simulation is enabled.
	Failed  int64
	DLQ     int64
	Retries int64

	// Lag is produced-minus-consumed from khaos's own counters -- a self-report, not
	// Kafka consumer lag. It is meaningless across restarts and wrong whenever anything
	// else produces to or consumes from the topic. It is always populated because it
	// costs nothing; BrokerLag is the real answer when the cluster will give one.
	Lag int64

	// BrokerLag is real committed-offset-vs-end-offset lag per group, summed over the
	// topic's partitions and populated only when lag polling is enabled (Config.LagPoll).
	//
	// Nil means "not measured" and MUST be rendered as unknown rather than zero: polling
	// off, the group not created yet, and a cluster that denies DESCRIBE on consumer
	// groups all land here, and none of them means the group has caught up. A group
	// missing from a non-nil map is unknown for the same reason -- the other groups
	// answered and it did not.
	BrokerLag map[string]int64

	Groups []GroupStat
}

// GroupStat is one consumer group's contribution to a topic.
type GroupStat struct {
	GroupID   string
	Consumers int
	Consumed  int64
	Failed    int64
	DLQ       int64
	Paused    int
}

// FlowStat is the per-flow slice of a Snapshot.
type FlowStat struct {
	Name      string
	Started   int64
	Completed int64
	Messages  int64
	Errors    int64

	// InFlight is the number of flow instances currently executing their step delays.
	// The pool is bounded and this depth is reported live.
	InFlight  int64
	Saturated int64 // times issuance blocked because the pool was full
}

// EventLevel mirrors scenario.EventLevel for output consumers that should not need to
// import the scenario package.
type EventLevel = scenario.EventLevel

// Event is something noteworthy that happened during a run.
//
// Events accumulate in a bounded ring and are read through Snapshot; the engine never
// writes to a terminal directly, so the output layer decides whether an event becomes a
// log line, a TUI row, or nothing at all.
type Event struct {
	At      time.Time
	Message string
	Level   EventLevel
}

// Snapshot is a consistent-enough, fully-owned view of engine state.
//
// This is the engine's ENTIRE read API. The TUI, the headless log loop, the Prometheus
// collector and any future web UI are all consumers of this one method. Everything in it
// is a value or a freshly allocated slice: a consumer can hold a Snapshot indefinitely
// without racing the engine or keeping engine memory alive.
type Snapshot struct {
	At       time.Time
	Elapsed  time.Duration
	Scenario string

	// Deadline is the configured run duration; zero means run until interrupted.
	Deadline time.Duration

	Topics []TopicStat
	Flows  []FlowStat
	Events []Event

	TotalProduced int64
	TotalConsumed int64
	TotalErrors   int64
	Rebalances    int64

	// Healthy is false when the engine has recorded a condition it cannot recover from
	// on its own. It drives /healthz.
	Healthy  bool
	Stopping bool
}

// eventRing is a fixed-capacity ring of recent events.
//
// Bounded on purpose: this process is expected to run for weeks, so an unbounded event
// log is a slow memory leak.
type eventRing struct {
	mu   sync.Mutex
	buf  []Event
	next int
	full bool
}

func newEventRing(capacity int) *eventRing {
	return &eventRing{buf: make([]Event, capacity)}
}

func (r *eventRing) add(e Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf[r.next] = e
	r.next = (r.next + 1) % len(r.buf)
	if r.next == 0 {
		r.full = true
	}
}

// snapshot returns the ring's contents oldest-first.
func (r *eventRing) snapshot() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := r.next
	if r.full {
		n = len(r.buf)
	}
	out := make([]Event, 0, n)
	if r.full {
		out = append(out, r.buf[r.next:]...)
	}
	out = append(out, r.buf[:r.next]...)
	return out
}
