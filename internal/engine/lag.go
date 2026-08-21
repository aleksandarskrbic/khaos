package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// Bounds on the per-poll admin timeout. The poll interval is the user's knob; these keep
// the derived timeout sane at both extremes.
//
// A poll must not be able to outlive its own tick by much, or a cluster that answers
// slowly turns a 1s interval into a permanent backlog of in-flight admin requests. It
// must also not be so tight that a healthy managed cluster -- where DescribeGroups +
// OffsetFetch + ListOffsets is three round trips over the internet -- reports a timeout
// every tick and hides real lag behind "unknown".
const (
	lagPollMinTimeout = 1 * time.Second
	lagPollMaxTimeout = 10 * time.Second
)

// lagSource is the slice of kafka.Admin the poller needs.
//
// An interface, not the concrete *kafka.Admin, so the failure paths that matter most --
// permission denied, timeout, partial results -- are testable without a broker that can
// be made to deny anything.
type lagSource interface {
	GroupLag(ctx context.Context, group string) (map[string]map[int32]int64, error)
}

// lagPoller asks the brokers, on a slow ticker, how far each consumer group actually is
// behind, and publishes the answer where Snapshot can read it.
//
// The self-reported Lag figure elsewhere in this package is produced-minus-consumed from
// khaos's own in-process counters -- a self-report that is wrong the moment anything else
// produces to or consumes from the topic, resets on restart, and says nothing about
// whether a consumer group is committing. Real lag is committed-offset versus
// log-end-offset, which only the brokers know.
//
// Three properties are non-negotiable, in this order:
//
//  1. It cannot fail the run. Reading consumer-group lag needs DESCRIBE on the group,
//     which Confluent Cloud and Aiven frequently do not grant. A denial, a timeout or any
//     other error leaves the affected group's lag unmeasured and the run untouched.
//  2. It cannot become a log firehose. A poller that logs the same authorization error
//     every 5 seconds for a week is its own incident. Each distinct reason is reported
//     once per group, and once more only if the reason changes or the group recovers.
//  3. It cannot touch the data path. It owns one goroutine, its own ticker and its own
//     admin client calls; producers and consumers never see it.
type lagPoller struct {
	src      lagSource
	interval time.Duration
	timeout  time.Duration

	// groups is every consumer group in the run, in a stable order.
	groups []string

	// topics is the engine's topic -> counters map. It is written only during build and
	// read-only thereafter, so indexing it from this goroutine is safe; the mutable state
	// is the per-topic atomic pointer inside each counters.
	topics map[string]*counters

	log    *slog.Logger
	events *eventRing

	// reported remembers the last reason each group failed for, so a persistent failure
	// is announced once rather than once per tick. Touched only by the poll goroutine.
	reported map[string]string
}

// newLagPoller builds the poller for this run, or returns nil when there is nothing to
// poll.
//
// Nil is the normal case: LagPoll defaults to zero, so there is no admin traffic and no
// DESCRIBE requirement -- BrokerLag stays nil and the output layer keeps showing the
// self-reported column alone.
func (e *Engine) newLagPoller() *lagPoller {
	if e.cfg.LagPoll <= 0 || e.admin == nil {
		return nil
	}

	// topicOrder rather than ranging topicMeta: map order is random, and the group order
	// decides the order of admin calls and therefore of any reported failures.
	seen := make(map[string]bool)
	var groups []string
	for _, name := range e.topicOrder {
		for _, g := range e.topicMeta[name].groups {
			if seen[g] {
				continue
			}
			seen[g] = true
			groups = append(groups, g)
		}
	}
	if len(groups) == 0 {
		// --no-consumers, or a scenario with no consumer groups. There is no group whose
		// lag could be measured, and asking about groups that do not exist would be pure
		// admin traffic for a permanently empty answer.
		return nil
	}

	return &lagPoller{
		src:      e.admin,
		interval: e.cfg.LagPoll,
		timeout:  lagPollTimeout(e.cfg.LagPoll),
		groups:   groups,
		topics:   e.topicStats,
		log:      e.log,
		events:   e.events,
		reported: make(map[string]string),
	}
}

// lagPollTimeout derives the per-poll admin timeout from the poll interval.
//
// Clamped rather than proportional: the useful range is narrow, and a user who asks for
// --lag-poll 100ms wants frequent samples, not a 100ms deadline that every real cluster
// misses.
func lagPollTimeout(interval time.Duration) time.Duration {
	switch {
	case interval < lagPollMinTimeout:
		return lagPollMinTimeout
	case interval > lagPollMaxTimeout:
		return lagPollMaxTimeout
	default:
		return interval
	}
}

// run polls until ctx is cancelled. It always returns nil: this goroutine is a member of
// the run's errgroup, and lag is an observability nicety -- failing the run because the
// cluster would not answer a DescribeGroups is precisely the outcome that must not happen.
func (p *lagPoller) run(ctx context.Context) error {
	// One poll straight away, so a short run with a long interval still gets a
	// measurement instead of exiting with everything unknown.
	p.pollOnce(ctx)

	t := time.NewTicker(p.interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			p.pollOnce(ctx)
		}
	}
}

// pollOnce fetches every group's lag and republishes the per-topic view.
//
// Groups are polled sequentially. Concurrency here would only matter with dozens of
// groups, and it would trade a slower tick for a burst of admin requests at a fixed
// interval forever -- the wrong side of that trade for a process that runs for weeks.
func (p *lagPoller) pollOnce(ctx context.Context) {
	byTopic := make(map[string]map[string]int64, len(p.topics))

	for _, group := range p.groups {
		lag, err := p.fetch(ctx, group)
		if err != nil {
			// Shutdown is not a lag failure. Without this, every run that ends while a
			// poll is in flight closes with a spurious "lag unavailable: context
			// canceled" in the event log.
			if ctx.Err() == nil {
				p.report(group, err)
			}
			continue
		}
		p.recovered(group)

		for topic, parts := range lag {
			if _, ok := p.topics[topic]; !ok {
				// A topic this run does not track: nothing would ever render it.
				continue
			}
			var total int64
			for _, l := range parts {
				total += l
			}
			m := byTopic[topic]
			if m == nil {
				m = make(map[string]int64, len(p.groups))
				byTopic[topic] = m
			}
			m[group] = total
		}
	}

	p.publish(byTopic)
}

// fetch runs one GroupLag call under its own deadline.
//
// The timeout is the whole point: kadm.Lag is three fanned-out request families, and on a
// partitioned or overloaded cluster any of them can hang for far longer than a poll
// interval. Without a bound the poller would stop sampling entirely and BrokerLag would
// freeze at whatever it last read -- a stale number presented as current, which is worse
// than "unknown".
//
// Partial results are deliberately discarded. GroupLag returns the partitions it could
// resolve alongside its error; summing those into a per-topic figure would produce a
// number that looks authoritative and understates the truth by however many partitions
// failed. Unknown is the honest answer.
func (p *lagPoller) fetch(ctx context.Context, group string) (map[string]map[int32]int64, error) {
	callCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	lag, err := p.src.GroupLag(callCtx, group)
	if err != nil {
		return nil, err
	}
	return lag, nil
}

// publish swaps in this tick's result for every tracked topic.
//
// The map handed to Store is freshly built by pollOnce and never mutated afterwards, so
// readers either see the previous map or this one, never a half-written one. Snapshot
// still copies it, because a Snapshot is documented as fully owned by its caller.
//
// A topic no group could answer for is reset to nil, not left holding the previous
// reading. Lag from thirty seconds ago rendered as the current value is exactly the kind
// of quietly-wrong number this decision exists to eliminate.
func (p *lagPoller) publish(byTopic map[string]map[string]int64) {
	for name, c := range p.topics {
		m := byTopic[name]
		if len(m) == 0 {
			c.brokerLag.Store(nil)
			continue
		}
		c.brokerLag.Store(&m)
	}
}

// report announces a group's lag failure at most once per distinct reason.
func (p *lagPoller) report(group string, err error) {
	reason := err.Error()
	if p.reported[group] == reason {
		return
	}
	p.reported[group] = reason

	p.log.Warn("consumer-group lag unavailable",
		"group", group, "interval", p.interval, "err", err)
	p.events.add(Event{
		At:      time.Now(),
		Message: fmt.Sprintf("lag unavailable for group %s: %v", group, err),
		Level:   scenario.EventWarn,
	})
}

// recovered announces, once, that a previously failing group is answering again.
func (p *lagPoller) recovered(group string) {
	if _, failing := p.reported[group]; !failing {
		return
	}
	delete(p.reported, group)

	p.log.Info("consumer-group lag available again", "group", group)
	p.events.add(Event{
		At:      time.Now(),
		Message: fmt.Sprintf("lag available again for group %s", group),
		Level:   scenario.EventRecovery,
	})
}
