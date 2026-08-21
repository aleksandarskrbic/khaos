package engine

import (
	"context"
	"fmt"
	"maps"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

// Run drives the scenario until the duration expires or ctx is cancelled, then tears
// everything down deterministically.
//
// Two structural properties matter here:
//
//  1. The run deadline lives in a context, not in the render loop, so a blocking render
//     can never starve the signal handler and make an infinite run unstoppable.
//
//  2. The output layer is NOT a member of this errgroup and never appears here at all.
//     The caller runs it separately against Snapshot. If it were a member, a TUI panic
//     would cancel the group and kill the engine -- the same coupling in fail-fast form.
func (e *Engine) Run(ctx context.Context) error {
	e.started.Store(time.Now().UnixNano())

	runCtx := ctx
	if e.cfg.Duration > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, e.cfg.Duration)
		defer cancel()
	}

	e.pending = make(chan *Consumer, 16)

	g, gctx := errgroup.WithContext(runCtx)

	for _, p := range e.reg.allProducers() {
		g.Go(func() error { return p.Run(gctx) })
	}
	for _, c := range e.reg.allConsumers() {
		g.Go(func() error { return c.Run(gctx) })
	}
	for _, f := range e.flows {
		g.Go(func() error { return f.Run(gctx) })
	}

	incidents, groups := e.allIncidents()
	g.Go(func() error { return e.sched.Run(gctx, incidents, groups) })

	// Real consumer-group lag, when it was asked for. A member of the group so it is
	// awaited at shutdown and cannot outlive the admin client that drain closes; its run
	// never returns an error, so it can never cancel the run. Nil when --lag-poll is
	// unset, which is the default. See lag.go.
	if lp := e.newLagPoller(); lp != nil {
		g.Go(func() error { return lp.run(gctx) })
	}

	// Supervise consumers created mid-run so their goroutines are owned by this group
	// and awaited at shutdown.
	//
	// Calling g.Go from inside a member of g is only sound because THIS goroutine is
	// itself a member: errgroup's WaitGroup counter therefore never reaches zero between
	// the supervisor deciding to spawn and the spawn happening, so g.Wait cannot return
	// early and sync.WaitGroup's "no Add from zero concurrent with Wait" rule is not
	// violated. Anything that lets this goroutine return before its last g.Go -- an early
	// `return` in the loop, moving the spawn to a helper goroutine -- breaks that and
	// reintroduces an untracked, unawaited consumer goroutine.
	g.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				return nil
			case c := <-e.pending:
				g.Go(func() error { return c.Run(gctx) })
			}
		}
	})

	err := g.Wait()
	e.stopping.Store(true)

	// A deadline-exceeded run is a completed run, not a failure.
	if ctx.Err() == nil && runCtx.Err() == context.DeadlineExceeded {
		err = nil
	}
	if err == context.Canceled || err == context.DeadlineExceeded {
		err = nil
	}

	e.teardown()
	return err
}

// allIncidents flattens incidents and groups across every scenario. Scenarios lose their
// identity at this point -- their incidents are simply concatenated.
func (e *Engine) allIncidents() ([]scenario.Incident, []scenario.IncidentGroup) {
	var incidents []scenario.Incident
	var groups []scenario.IncidentGroup
	for _, sc := range e.cfg.Scenarios {
		incidents = append(incidents, sc.Incidents...)
		groups = append(groups, sc.IncidentGroups...)
	}
	return incidents, groups
}

// teardown flushes and closes everything, under a hard deadline.
//
// This is the one place khaos deliberately abandons a goroutine: if drain has not
// finished within teardownDeadline, teardown returns and leaves it running. That is a
// considered trade -- the alternative is hanging past a container's grace period and
// being SIGKILLed with no diagnostics -- and it is why the individual steps inside drain
// are all separately bounded, so the abandonment is a last resort rather than the normal
// path.
func (e *Engine) teardown() {
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.drain()
	}()

	deadline := time.NewTimer(teardownDeadline)
	defer deadline.Stop()

	select {
	case <-done:
	case <-deadline.C:
		// Hanging past the container's grace period is worse than dying loudly. Dump
		// goroutines so the hang is diagnosable rather than mysterious.
		fmt.Fprintf(os.Stderr, "khaos: teardown exceeded %s, dumping goroutines\n", teardownDeadline)
		_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
	}
}

// drain flushes buffered records and closes every client.
func (e *Engine) drain() {
	// A FRESH context is mandatory here. kgo.Flush(ctx) returns immediately on an
	// already-cancelled context and silently DROPS buffered records -- so reusing the run
	// context would lose exactly the messages the flush exists to deliver.
	flushCtx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()

	// Flush CONCURRENTLY, not sequentially: with a downed broker and twenty producers, a
	// shared deadline spent sequentially means the first slow producer consumes the whole
	// budget and every later one gets an expired context.
	e.clientsMu.Lock()
	clients := make([]*kgo.Client, len(e.producerClients))
	copy(clients, e.producerClients)
	e.clientsMu.Unlock()

	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Flush is bounded by flushCtx; Close is bounded internally by kgo (it fails
			// every still-buffered record with ErrClientClosed rather than waiting), so a
			// producer whose broker is down cannot outlast the teardown deadline.
			_ = c.Flush(flushCtx)
			c.Close()
		}()
	}

	// Consumers leave the group cleanly rather than waiting out session.timeout.ms.
	//
	// Concurrently, and in the same wait group as the producers: LeaveGroup runs against
	// the client's own context, so with a downed coordinator each Close costs a request
	// timeout. Sequentially that is N timeouts and blows the teardown deadline; the
	// deadline then only dumps goroutines and leaves this one running.
	for _, c := range e.reg.allConsumers() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Close()
		}()
	}
	wg.Wait()

	if e.admin != nil {
		e.admin.Close()
	}
}

// closeAll tears down partially-constructed state when New fails midway.
func (e *Engine) closeAll() {
	e.clientsMu.Lock()
	clients := e.producerClients
	e.producerClients = nil
	e.clientsMu.Unlock()

	for _, c := range clients {
		c.Close()
	}
	for _, c := range e.reg.allConsumers() {
		c.Close()
	}
}

// Snapshot returns a fully-owned view of current engine state.
//
// This is the engine's entire read API. Everything returned is a value or a freshly
// allocated slice, so a consumer may hold it indefinitely without racing the engine.
// Consumers PULL on their own schedule rather than the engine pushing: a wedged UI then
// produces stale numbers instead of backpressure into the run.
func (e *Engine) Snapshot() Snapshot {
	now := time.Now()
	snap := Snapshot{
		At:       now,
		Elapsed:  now.Sub(time.Unix(0, e.started.Load())),
		Deadline: e.cfg.Duration,
		Healthy:  e.healthy.Load(),
		Stopping: e.stopping.Load(),
		Events:   e.events.snapshot(),
	}
	if len(e.cfg.Scenarios) > 0 {
		snap.Scenario = e.cfg.Scenarios[0].Name
	}

	// Group live consumers by topic and group so per-group rows can be rendered. This is
	// derived on read rather than maintained as a standing index, which is what keeps the
	// registry from diverging.
	type groupKey struct{ topic, group string }
	byGroup := make(map[groupKey]*GroupStat)
	for _, c := range e.reg.allConsumers() {
		k := groupKey{c.Topic, c.GroupID}
		gs := byGroup[k]
		if gs == nil {
			gs = &GroupStat{GroupID: c.GroupID}
			byGroup[k] = gs
		}
		gs.Consumers++
		gs.Consumed += c.stats.consumed.Load()
		gs.Failed += c.stats.failed.Load()
		gs.DLQ += c.stats.dlq.Load()
		if c.Paused() {
			gs.Paused++
		}
	}

	for _, name := range e.topicOrder {
		cs := e.topicStats[name]
		if cs == nil {
			continue
		}
		meta := e.topicMeta[name]

		ts := TopicStat{
			Topic:      name,
			Scenario:   meta.scenarioName,
			Produced:   cs.sent.Load(),
			Consumed:   cs.consumed.Load(),
			Bytes:      cs.bytes.Load(),
			ProduceErr: cs.produceErr.Load(),
			ConsumeErr: cs.consumeErr.Load(),
			Duplicates: cs.duplicate.Load(),
			CommitErr:  cs.commitErr.Load(),
			Failed:     cs.failed.Load(),
			DLQ:        cs.dlq.Load(),
			Retries:    cs.retries.Load(),
		}
		ts.Lag = ts.Produced - ts.Consumed

		// Real lag, if the poller measured any. Copied, never aliased: the poller keeps
		// the published map immutable for its own safety, but a Snapshot is documented as
		// fully owned by its caller, and the TUI and the Prometheus collector both hold
		// one across the next tick.
		if bl := cs.brokerLag.Load(); bl != nil {
			ts.BrokerLag = maps.Clone(*bl)
		}

		for _, gid := range meta.groups {
			if gs, ok := byGroup[groupKey{name, gid}]; ok {
				ts.Groups = append(ts.Groups, *gs)
			}
		}

		snap.Topics = append(snap.Topics, ts)
		snap.TotalProduced += ts.Produced
		snap.TotalConsumed += ts.Consumed
		snap.TotalErrors += ts.ProduceErr + ts.ConsumeErr
	}

	for _, name := range e.flowOrder {
		fc := e.flowStats[name]
		if fc == nil {
			continue
		}
		snap.Flows = append(snap.Flows, FlowStat{
			Name:      name,
			Started:   fc.started.Load(),
			Completed: fc.completed.Load(),
			Messages:  fc.messages.Load(),
			Errors:    fc.errors.Load(),
			InFlight:  fc.inflight.Load(),
			Saturated: fc.saturated.Load(),
		})
	}

	snap.Rebalances = e.sched.rebalances.Load()
	return snap
}

// Healthy reports whether the engine is in a state it can recover from. It backs /healthz.
func (e *Engine) Healthy() error {
	if !e.healthy.Load() {
		return fmt.Errorf("engine unhealthy")
	}
	return nil
}
