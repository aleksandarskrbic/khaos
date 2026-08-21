package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aleksandarskrbic/khaos/internal/kafka"
	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// stubLagSource is a lagSource whose answer per group is scripted.
//
// The failure modes that decide whether this feature is safe -- DESCRIBE denied, the
// request timing out, a half-resolved answer -- cannot be provoked from kfake, which
// authorises everything and always replies. They are exactly the modes a managed cluster
// hits daily, so they are tested here and the broker-backed test covers the happy path.
type stubLagSource struct {
	mu    sync.Mutex
	calls int
	fn    func(ctx context.Context, call int, group string) (map[string]map[int32]int64, error)
}

func (s *stubLagSource) GroupLag(ctx context.Context, group string) (map[string]map[int32]int64, error) {
	s.mu.Lock()
	call := s.calls
	s.calls++
	fn := s.fn
	s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return fn(ctx, call, group)
}

func (s *stubLagSource) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// lagEngine builds the smallest Engine that Snapshot works against: no clients, no
// broker, no run. Snapshot reads the registry, the scheduler's counter, the event ring and
// the topic maps, so those and nothing else are populated.
func lagEngine(t *testing.T, topics ...string) *Engine {
	t.Helper()

	e := &Engine{
		log:        slog.New(slog.DiscardHandler),
		reg:        newRegistry(),
		sched:      &scheduler{},
		events:     newEventRing(64),
		topicStats: make(map[string]*counters, len(topics)),
		topicMeta:  make(map[string]topicMeta, len(topics)),
	}
	for _, name := range topics {
		e.topicStats[name] = &counters{}
		e.topicOrder = append(e.topicOrder, name)
		e.topicMeta[name] = topicMeta{
			scenarioName: "lag",
			groups:       []string{name + "-group-1"},
		}
	}
	return e
}

// pollerFor wires a stub source to an engine's topics the way newLagPoller wires the real
// admin client.
func pollerFor(t *testing.T, e *Engine, src lagSource) *lagPoller {
	t.Helper()

	var groups []string
	for _, name := range e.topicOrder {
		groups = append(groups, e.topicMeta[name].groups...)
	}
	return &lagPoller{
		src:      src,
		interval: time.Millisecond,
		timeout:  lagPollTimeout(time.Millisecond),
		groups:   groups,
		topics:   e.topicStats,
		log:      e.log,
		events:   e.events,
		reported: make(map[string]string),
	}
}

// countEvents reports how many events in the ring contain substr.
func countEvents(e *Engine, substr string) int {
	n := 0
	for _, ev := range e.events.snapshot() {
		if strings.Contains(ev.Message, substr) {
			n++
		}
	}
	return n
}

func TestLagPollTimeoutIsBounded(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		{"absurdly fast interval gets the floor", time.Millisecond, lagPollMinTimeout},
		{"at the floor", time.Second, time.Second},
		{"a sensible interval is its own timeout", 5 * time.Second, 5 * time.Second},
		{"at the ceiling", 10 * time.Second, 10 * time.Second},
		{"a slow interval is capped", time.Hour, lagPollMaxTimeout},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := lagPollTimeout(tt.interval); got != tt.want {
				t.Fatalf("lagPollTimeout(%s) = %s, want %s", tt.interval, got, tt.want)
			}
		})
	}
}

// TestLagPollerPublishesPerGroupLag is the core mapping: partitions are summed per topic
// and filed under the group that was asked.
func TestLagPollerPublishesPerGroupLag(t *testing.T) {
	e := lagEngine(t, "orders")
	src := &stubLagSource{fn: func(_ context.Context, _ int, group string) (map[string]map[int32]int64, error) {
		return map[string]map[int32]int64{
			"orders": {0: 10, 1: 32, 2: 0},
			// A topic this run does not track must not invent a row.
			"someone-elses-topic": {0: 999},
		}, nil
	}}

	pollerFor(t, e, src).pollOnce(context.Background())

	snap := e.Snapshot()
	if len(snap.Topics) != 1 {
		t.Fatalf("snapshot has %d topics, want 1", len(snap.Topics))
	}
	got := snap.Topics[0].BrokerLag
	if got == nil {
		t.Fatal("BrokerLag is nil after a successful poll; the reading was lost")
	}
	if want := int64(42); got["orders-group-1"] != want {
		t.Fatalf("BrokerLag = %v, want %d for orders-group-1", got, want)
	}
	if len(got) != 1 {
		t.Fatalf("BrokerLag = %v, want exactly one group", got)
	}
}

// TestLagPollerLeavesLagUnmeasuredOnFailure is the whole safety contract in one test:
// a cluster that refuses must produce "unknown", never a zero, and never a second word
// about it.
func TestLagPollerLeavesLagUnmeasuredOnFailure(t *testing.T) {
	denied := errors.New("GROUP_AUTHORIZATION_FAILED: not authorized to access group")

	tests := []struct {
		name string
		fn   func(ctx context.Context, call int, group string) (map[string]map[int32]int64, error)
	}{
		{
			name: "permission denied",
			fn: func(context.Context, int, string) (map[string]map[int32]int64, error) {
				return nil, denied
			},
		},
		{
			name: "timed out",
			fn: func(context.Context, int, string) (map[string]map[int32]int64, error) {
				return nil, context.DeadlineExceeded
			},
		},
		{
			// GroupLag returns what it could resolve alongside its error. Summing that
			// would report a confidently wrong number, so it is dropped.
			name: "partial result with an error",
			fn: func(context.Context, int, string) (map[string]map[int32]int64, error) {
				return map[string]map[int32]int64{"orders": {0: 7}},
					errors.New("lag is incomplete: orders[3]: UNKNOWN_TOPIC_OR_PARTITION")
			},
		},
		{
			// A group nobody has joined yet: no error, nothing to report.
			name: "empty answer",
			fn: func(context.Context, int, string) (map[string]map[int32]int64, error) {
				return map[string]map[int32]int64{}, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := lagEngine(t, "orders")
			p := pollerFor(t, e, &stubLagSource{fn: tt.fn})

			for i := 0; i < 5; i++ {
				p.pollOnce(context.Background())
			}

			if got := e.Snapshot().Topics[0].BrokerLag; got != nil {
				t.Fatalf("BrokerLag = %v, want nil so the output layer renders unknown", got)
			}
			// Five identical failures, one report. A poller that logs an authorization
			// error every tick for a week is its own outage.
			if n := countEvents(e, "lag unavailable"); n > 1 {
				t.Fatalf("reported the same failure %d times, want at most once", n)
			}
		})
	}
}

// TestLagPollerForgetsStaleReadings: once the cluster stops answering, the last good
// number must go away. A reading from a minute ago presented as current is the exact
// failure mode real lag is meant to remove.
func TestLagPollerForgetsStaleReadings(t *testing.T) {
	e := lagEngine(t, "orders")
	src := &stubLagSource{fn: func(_ context.Context, call int, _ string) (map[string]map[int32]int64, error) {
		if call == 0 {
			return map[string]map[int32]int64{"orders": {0: 500}}, nil
		}
		return nil, errors.New("coordinator not available")
	}}
	p := pollerFor(t, e, src)

	p.pollOnce(context.Background())
	if got := e.Snapshot().Topics[0].BrokerLag["orders-group-1"]; got != 500 {
		t.Fatalf("first poll: BrokerLag = %d, want 500", got)
	}

	p.pollOnce(context.Background())
	if got := e.Snapshot().Topics[0].BrokerLag; got != nil {
		t.Fatalf("second poll: BrokerLag = %v, want nil rather than the stale 500", got)
	}
}

// TestLagPollerAnnouncesRecovery: a group that starts answering again says so once, so an
// operator watching the event log knows the "unknown" cells are about to fill in.
func TestLagPollerAnnouncesRecovery(t *testing.T) {
	e := lagEngine(t, "orders")
	src := &stubLagSource{fn: func(_ context.Context, call int, _ string) (map[string]map[int32]int64, error) {
		if call < 2 {
			return nil, errors.New("coordinator not available")
		}
		return map[string]map[int32]int64{"orders": {0: 1}}, nil
	}}
	p := pollerFor(t, e, src)

	for i := 0; i < 4; i++ {
		p.pollOnce(context.Background())
	}

	if n := countEvents(e, "lag unavailable"); n != 1 {
		t.Errorf("failure reported %d times, want once", n)
	}
	if n := countEvents(e, "lag available again"); n != 1 {
		t.Errorf("recovery reported %d times, want once", n)
	}
	if got := e.Snapshot().Topics[0].BrokerLag["orders-group-1"]; got != 1 {
		t.Errorf("BrokerLag = %d after recovery, want 1", got)
	}
}

// TestLagPollerStopsMidPoll proves cancellation is observed while a poll is in flight and
// that shutting down is not mistaken for a lag failure.
//
// The package-wide goleak check in TestMain is the other half: a poller that ignored ctx
// would fail this package rather than quietly outliving the run.
func TestLagPollerStopsMidPoll(t *testing.T) {
	e := lagEngine(t, "orders")

	entered := make(chan struct{})
	var once sync.Once
	src := &stubLagSource{fn: func(ctx context.Context, _ int, _ string) (map[string]map[int32]int64, error) {
		once.Do(func() { close(entered) })
		// Hangs the way an unreachable coordinator does, and unblocks the way franz-go
		// does: on the context. That is why fetch puts a deadline on every call and why
		// the run context has to reach this far down -- without either, the poll outlives
		// the run.
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	p := pollerFor(t, e, src)
	p.timeout = time.Minute

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.run(ctx) }()

	<-entered
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() = %v, want nil: the poller must never fail the run", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("poller did not return after its context was cancelled")
	}

	if n := countEvents(e, "lag unavailable"); n != 0 {
		t.Errorf("shutdown produced %d lag failure events, want none", n)
	}
	if got := e.Snapshot().Topics[0].BrokerLag; got != nil {
		t.Errorf("BrokerLag = %v after an abandoned poll, want nil", got)
	}
}

// TestLagPollerConcurrentWithSnapshot is the race test.
//
// The poller writes from its own goroutine while the TUI, the headless log loop and the
// Prometheus collector all call Snapshot from theirs. Several fields in this package have
// already been caught racing in exactly this shape, so the overlap is exercised directly
// rather than hoped about.
func TestLagPollerConcurrentWithSnapshot(t *testing.T) {
	e := lagEngine(t, "orders", "payments")
	src := &stubLagSource{fn: func(_ context.Context, call int, group string) (map[string]map[int32]int64, error) {
		if call%7 == 0 {
			return nil, fmt.Errorf("transient failure %d", call)
		}
		return map[string]map[int32]int64{
			"orders":   {0: int64(call), 1: 3},
			"payments": {0: int64(call % 5)},
		}, nil
	}}
	p := pollerFor(t, e, src)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = p.run(ctx)
	}()

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				for _, ts := range e.Snapshot().Topics {
					for group, lag := range ts.BrokerLag {
						_, _ = group, lag
					}
					// Mutating the returned map must not be visible to the poller or to
					// the next reader: Snapshot hands back data the caller owns.
					if ts.BrokerLag != nil {
						ts.BrokerLag["clobbered"] = -1
					}
				}
			}
		}()
	}

	cancel()
	wg.Wait()

	for _, ts := range e.Snapshot().Topics {
		if _, bad := ts.BrokerLag["clobbered"]; bad {
			t.Fatalf("topic %s: Snapshot aliases the poller's live map", ts.Topic)
		}
	}
	if src.Calls() == 0 {
		t.Fatal("the poller never ran")
	}
}

// TestBrokerLagFromRealBrokers is the end-to-end proof against a real Kafka protocol
// implementation: an engine run with polling enabled reports the group's actual lag.
//
// The consumers auto-commit every 5s (internal/kafka/consumer.go), so for the first five
// seconds of a run the group has read everything and committed nothing -- genuinely behind
// by the whole log. The self-reported figure cannot show this: it reads roughly zero,
// because khaos's own consumers did read the records.
//
// The assertion samples DURING the run rather than after it, and deliberately does not
// assert on the final snapshot. Lag is a live measurement: the last poll before shutdown
// can legitimately land mid-rebalance, when the group holds no assignment and there is
// nothing to be behind on, which correctly publishes "unknown". Asserting on whichever
// poll happened to be last would be asserting on the scheduler.
func TestBrokerLagFromRealBrokers(t *testing.T) {
	addrs := newFakeCluster(t)

	tp := jsonTopic("brokerlag-topic")
	tp.ProducerRate = 400

	eng, err := New(context.Background(), Config{
		Kafka:     kafka.Config{BootstrapServers: addrs},
		Scenarios: []*scenario.Scenario{{Name: "lag", Topics: []scenario.Topic{tp}}},
		Duration:  0, // stopped as soon as the measurement lands
		LagPoll:   250 * time.Millisecond,
		Seed:      5,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()

	const group = "brokerlag-topic-group-1"
	var (
		lag      int64
		produced int64
		selfLag  int64
	)
	deadline := time.Now().Add(30 * time.Second)
	for lag == 0 {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("no broker lag measured in 30s; events: %v", eng.Snapshot().Events)
		}
		select {
		case err := <-done:
			t.Fatalf("run ended before any lag was measured: %v", err)
		case <-time.After(50 * time.Millisecond):
		}

		ts := eng.Snapshot().Topics[0]
		if v, ok := ts.BrokerLag[group]; ok && v > 0 {
			lag, produced, selfLag = v, ts.Produced, ts.Lag
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	// A band, never an equality: the reading is from the last completed poll and the
	// producer kept going after it.
	if lag > produced {
		t.Errorf("broker lag = %d, above the %d records produced by the time it was read",
			lag, produced)
	}
	t.Logf("produced=%d self-reported lag=%d broker lag=%d", produced, selfLag, lag)
}

// TestBrokerLagNilWhenPollingDisabled pins the default: no --lag-poll, no admin traffic,
// no BrokerLag, and the self-reported column unaffected.
func TestBrokerLagNilWhenPollingDisabled(t *testing.T) {
	addrs := newFakeCluster(t)

	eng, err := New(context.Background(), Config{
		Kafka:     kafka.Config{BootstrapServers: addrs},
		Scenarios: []*scenario.Scenario{{Name: "nolag", Topics: []scenario.Topic{jsonTopic("nolag-topic")}}},
		Duration:  time.Second,
		Seed:      6,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if eng.newLagPoller() != nil {
		t.Error("a poller was built with LagPoll unset")
	}
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := eng.Snapshot().Topics[0].BrokerLag; got != nil {
		t.Errorf("BrokerLag = %v with polling disabled, want nil", got)
	}
}

// TestNoLagPollerWithoutConsumerGroups: --no-consumers leaves no group whose lag could
// exist, so polling would be admin traffic for a permanently empty answer.
func TestNoLagPollerWithoutConsumerGroups(t *testing.T) {
	addrs := newFakeCluster(t)

	eng, err := New(context.Background(), Config{
		Kafka:       kafka.Config{BootstrapServers: addrs},
		Scenarios:   []*scenario.Scenario{{Name: "nc", Topics: []scenario.Topic{jsonTopic("nc-topic")}}},
		Duration:    time.Second,
		NoConsumers: true,
		LagPoll:     100 * time.Millisecond,
		Seed:        8,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(eng.closeAll)
	t.Cleanup(eng.admin.Close)

	if p := eng.newLagPoller(); p != nil {
		t.Errorf("built a poller for %d groups with --no-consumers", len(p.groups))
	}
}
