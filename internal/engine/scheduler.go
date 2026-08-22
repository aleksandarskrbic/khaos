package engine

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
	"github.com/aleksandarskrbic/khaos/internal/telemetry"
	"golang.org/x/sync/errgroup"
)

// BrokerController stops and starts brokers.
//
// Two real implementations -- the local Docker cluster and a no-op for external clusters
// -- which is exactly the bar for defining an interface.
type BrokerController interface {
	StopBroker(ctx context.Context, name string) error
	StartBroker(ctx context.Context, name string) error
}

// noopBrokers is used against external clusters, where broker faults are not available.
// The attempt is recorded as an event so the user learns why nothing happened.
type noopBrokers struct{ events *eventRing }

func (n noopBrokers) StopBroker(_ context.Context, name string) error {
	n.events.add(Event{
		At:      time.Now(),
		Message: fmt.Sprintf("broker fault skipped: cannot stop %s on an external cluster", name),
		Level:   scenario.EventWarn,
	})
	return nil
}

func (n noopBrokers) StartBroker(_ context.Context, name string) error {
	n.events.add(Event{
		At:      time.Now(),
		Message: fmt.Sprintf("broker fault skipped: cannot start %s on an external cluster", name),
		Level:   scenario.EventWarn,
	})
	return nil
}

// consumerFactory creates a replacement consumer during a rebalance.
type consumerFactory func(ctx context.Context, groupID string, topics []string, delayMS int, conf scenario.ConsumerConf) error

// scheduler fires incidents and applies the commands they expand into.
type scheduler struct {
	reg     *registry
	events  *eventRing
	brokers BrokerController
	create  consumerFactory
	rnd     *rand.Rand
	metrics *telemetry.Metrics

	rebalances atomic.Int64

	// applyMu serialises mutations so two concurrently-firing incidents cannot interleave
	// halfway through a command sequence.
	//
	// Note it is NOT held across a Delay command. Command sequences embed blocking delays
	// -- PauseConsumers emits [StopConsumers, Delay(duration), ResumeConsumers] -- so
	// holding the lock across one would stall every other due incident for the length of
	// this one's delay and change scenario timing. Each incident is scheduled
	// independently, and that independence must be preserved.
	applyMu sync.Mutex

	// rndMu guards rnd. Run gives every incident and every group its own goroutine, and
	// target selection draws from this one generator (scenario.Context.Rand, used by
	// sample and RebalanceConsumer at internal/scenario/incident.go:289,386). A
	// math/rand/v2 *Rand is not safe for concurrent use, so two incidents firing at the
	// same instant race its PCG state -- and unlike most races this one silently biases
	// which components a chaos run actually hits.
	//
	// The lock covers expansion only, never command application: expansion is pure and
	// microseconds long, whereas a command sequence embeds multi-second delays.
	rndMu sync.Mutex
}

// Run fires every incident and incident group, one goroutine each, until ctx is cancelled.
func (s *scheduler) Run(ctx context.Context, incidents []scenario.Incident, groups []scenario.IncidentGroup) error {
	g, gctx := errgroup.WithContext(ctx)

	for _, inc := range incidents {
		g.Go(func() error { return s.runIncident(gctx, inc) })
	}
	for _, grp := range groups {
		g.Go(func() error { return s.runGroup(gctx, grp) })
	}

	err := g.Wait()
	if ctx.Err() != nil {
		return nil
	}
	return err
}

// runIncident owns one incident's timer and command sequence for the whole run.
func (s *scheduler) runIncident(ctx context.Context, inc scenario.Incident) error {
	sched := inc.Sched()

	if sched.InitialDelaySeconds > 0 {
		if !sleepCtx(ctx, time.Duration(sched.InitialDelaySeconds)*time.Second) {
			return nil
		}
	}

	// at_seconds is an offset from run start, measured after the initial delay has
	// already been applied.
	if sched.AtSeconds != nil {
		if !sleepCtx(ctx, time.Duration(*sched.AtSeconds)*time.Second) {
			return nil
		}
		if err := s.fire(ctx, inc); err != nil {
			return err
		}
		if sched.EverySeconds == nil {
			return nil
		}
	}

	if sched.EverySeconds == nil {
		// Neither at_seconds nor every_seconds: fire once, immediately.
		if sched.AtSeconds == nil {
			return s.fire(ctx, inc)
		}
		return nil
	}

	interval := time.Duration(*sched.EverySeconds) * time.Second
	if interval <= 0 {
		return nil
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.fire(ctx, inc); err != nil {
				return err
			}
		}
	}
}

// runGroup repeats a set of incidents on an interval.
func (s *scheduler) runGroup(ctx context.Context, grp scenario.IncidentGroup) error {
	repeat := grp.Repeat
	if repeat <= 0 {
		repeat = 1
	}

	for i := 0; i < repeat; i++ {
		for _, inc := range grp.Incidents {
			if err := s.runIncident(ctx, inc); err != nil {
				return err
			}
			if ctx.Err() != nil {
				return nil
			}
		}
		if i < repeat-1 {
			if !sleepCtx(ctx, time.Duration(grp.IntervalSeconds)*time.Second) {
				return nil
			}
		}
	}
	return nil
}

// fire expands an incident against a fresh view of the registry and applies the result.
func (s *scheduler) fire(ctx context.Context, inc scenario.Incident) error {
	return s.apply(ctx, s.expand(inc))
}

// expand turns an incident into commands against a fresh view of the registry.
//
// Split out from fire purely so the shared RNG is held for the expansion and released
// before any command -- including a Delay -- is applied. See scheduler.rndMu.
func (s *scheduler) expand(inc scenario.Incident) []scenario.Command {
	ictx := s.reg.incidentContext(int(s.rebalances.Load()))

	s.rndMu.Lock()
	defer s.rndMu.Unlock()
	ictx.Rand = s.rnd
	return inc.Commands(ictx)
}

// apply executes a command sequence in order.
func (s *scheduler) apply(ctx context.Context, cmds []scenario.Command) error {
	for _, cmd := range cmds {
		if ctx.Err() != nil {
			return nil
		}

		// Delay is deliberately outside the mutation lock: holding it here is what would
		// serialise unrelated incidents behind this one.
		if d, ok := cmd.(scenario.Delay); ok {
			if !sleepCtx(ctx, time.Duration(d.Seconds*float64(time.Second))) {
				return nil
			}
			continue
		}

		// StopConsumer is the other command that must not hold the lock for its whole
		// duration: closing a consumer leaves the group, a network round trip that
		// normally costs single-digit milliseconds but has been measured at two seconds
		// while the group is churning. Under applyMu that stalls every concurrently
		// firing incident for the same two seconds -- exactly the coupling the lock's
		// comment says it exists to avoid.
		if sc, ok := cmd.(scenario.StopConsumer); ok {
			s.stopConsumer(sc.ID)
			continue
		}

		if err := s.applyOne(ctx, cmd); err != nil {
			return err
		}
	}
	return nil
}

func (s *scheduler) applyOne(ctx context.Context, cmd scenario.Command) error {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	switch c := cmd.(type) {
	case scenario.EmitEvent:
		s.events.add(Event{At: time.Now(), Message: c.Message, Level: c.Level})

	case scenario.IncrementRebalanceCount:
		s.rebalances.Add(1)
		if s.metrics != nil && c.GroupID != "" {
			s.metrics.Rebalances.WithLabelValues(c.GroupID).Inc()
		}

	case scenario.SetConsumerDelay:
		if con, ok := s.reg.consumer(c.ID); ok {
			con.SetDelay(c.DelayMS)
		}

	case scenario.SetProducerRate:
		if p, ok := s.reg.producer(c.ID); ok {
			p.SetRate(c.Rate)
		}

	case scenario.StopConsumers:
		for _, id := range c.IDs {
			if con, ok := s.reg.consumer(id); ok {
				con.Pause()
			}
		}

	case scenario.ResumeConsumers:
		for _, id := range c.IDs {
			if con, ok := s.reg.consumer(id); ok {
				con.Resume()
			}
		}

	case scenario.CreateConsumer:
		if s.create == nil {
			return nil
		}
		if err := s.create(ctx, c.GroupID, c.Topics, c.ProcessingDelayMS, c.Conf); err != nil {
			// A replacement that fails to join is worth surfacing, but it must not abort
			// the run -- that is precisely the condition a chaos scenario is exercising.
			s.events.add(Event{
				At:      time.Now(),
				Message: fmt.Sprintf("failed to recreate consumer in group %s: %v", c.GroupID, err),
				Level:   scenario.EventAlert,
			})
		}

	case scenario.StopBroker:
		if err := s.brokers.StopBroker(ctx, c.Broker); err != nil {
			s.events.add(Event{
				At:      time.Now(),
				Message: fmt.Sprintf("failed to stop broker %s: %v", c.Broker, err),
				Level:   scenario.EventAlert,
			})
		}

	case scenario.StartBroker:
		if err := s.brokers.StartBroker(ctx, c.Broker); err != nil {
			s.events.add(Event{
				At:      time.Now(),
				Message: fmt.Sprintf("failed to start broker %s: %v", c.Broker, err),
				Level:   scenario.EventAlert,
			})
		}

	default:
		return fmt.Errorf("engine: unhandled command %T", cmd)
	}
	return nil
}

// stopConsumer deregisters a consumer and then closes it.
//
// Deregistration happens under the mutation lock so that no incident firing concurrently
// can select a consumer that is on its way out. The close itself is outside the lock: it
// is the only command that performs a blocking network round trip, and holding applyMu
// across it would stall every other incident. Closing after deregistration is safe
// precisely because nothing can look the consumer up any more.
func (s *scheduler) stopConsumer(id scenario.ID) {
	s.applyMu.Lock()
	con, ok := s.reg.consumer(id)
	if ok {
		s.reg.removeConsumer(id)
	}
	s.applyMu.Unlock()

	if ok {
		con.Close()
	}
}

// sleepCtx waits for d, reporting false if the context was cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
