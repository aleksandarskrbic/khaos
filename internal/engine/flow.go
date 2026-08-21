package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aleksandarskrbic/khaos/internal/generate"
	"github.com/aleksandarskrbic/khaos/internal/kafka"
	"github.com/aleksandarskrbic/khaos/internal/scenario"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// DefaultFlowConcurrency bounds how many flow instances may be mid-flight at once. Zero
// means unbounded -- the default, because bounding is an observable change: at
// saturation the pool must either block issuance (actual flow rate drops below
// configured), drop work, or queue unboundedly, so opting into a bound is explicit.
// When bounded, this implementation BLOCKS and counts the event, visible in the
// snapshot as Saturated rather than silent. At rate=50 with multi-second step delays
// the steady state is a few hundred instances.
var DefaultFlowConcurrency = 0

// flowDrainSlack is added to the sum of a flow's step delays to bound the post-cancellation
// join; the 500ms covers produce and flush latency on top of the configured delays.
const flowDrainSlack = 500 * time.Millisecond

// drainBudget is the ceiling on how long Run waits for in-flight instances after the run
// is cancelled.
//
// It is a ceiling, not a sleep: the join returns as soon as the instances finish. The
// ceiling has to exist because the instances run on a context detached from the run's, so
// a ProduceSync against a downed broker would otherwise block Run -- and therefore the
// engine's whole errgroup, and therefore teardown, which never gets to start -- forever.
// The teardown deadline cannot help: teardown happens after Run returns.
//
// The budget is the SUM of the step delays, not the max, because an instance issued the
// instant before cancellation still has every step ahead of it.
func drainBudget(f scenario.Flow) time.Duration {
	total := flowDrainSlack
	for _, s := range f.Steps {
		if s.DelayMS > 0 {
			total += time.Duration(s.DelayMS) * time.Millisecond
		}
	}
	return total
}

// newSemaphore returns a counting semaphore, or nil when unbounded.
func newSemaphore(n int) chan struct{} {
	if n <= 0 {
		return nil
	}
	return make(chan struct{}, n)
}

// flowRunner emits correlated multi-step flow instances at a fixed rate.
type flowRunner struct {
	name    string
	gen     *generate.FlowGen
	client  *kgo.Client
	limiter *rate.Limiter
	stats   *flowCounters
	events  *eventRing

	// concurrency is a counting semaphore bounding in-flight instances.
	concurrency chan struct{}

	// drain bounds the post-cancellation join. See drainBudget.
	drain time.Duration
}

func newFlowRunner(f scenario.Flow, gen *generate.FlowGen, client *kgo.Client, stats *flowCounters, events *eventRing) *flowRunner {
	return &flowRunner{
		name:        f.Name,
		gen:         gen,
		client:      client,
		limiter:     newLimiter(f.Rate),
		stats:       stats,
		events:      events,
		concurrency: newSemaphore(DefaultFlowConcurrency),
		drain:       drainBudget(f),
	}
}

// Run issues flow instances until ctx is cancelled, then waits for in-flight instances to
// finish with a real join.
func (f *flowRunner) Run(ctx context.Context) error {
	// Instances run on a context detached from the run deadline so a flow that is
	// mid-sequence when the duration expires still finishes rather than emitting a
	// truncated, uncorrelated flow. Detached, however, is not unbounded: cancelDrain is
	// armed once issuance stops so a wedged step cannot hold the run open forever.
	drainCtx, cancelDrain := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelDrain()

	g, gctx := errgroup.WithContext(drainCtx)

	for {
		if err := f.limiter.Wait(ctx); err != nil {
			break // run cancelled; fall through to the join
		}

		// Acquire before issuing, when bounded. Unbounded (the default) skips this
		// entirely and instances simply accumulate.
		if f.concurrency != nil {
			select {
			case f.concurrency <- struct{}{}:
			default:
				f.stats.saturated.Add(1)
				select {
				case f.concurrency <- struct{}{}:
				case <-ctx.Done():
				}
			}
			if ctx.Err() != nil {
				break
			}
		}

		// Generate on THIS goroutine, before handing the work off.
		//
		// generate.FlowGen is not safe for concurrent use: it owns a *rand.Rand and the
		// per-step field generators, whose cardinality caches are deliberately shared
		// across every instance. Calling Instance() from the worker goroutines would race
		// both -- the race detector catches it, and in production it corrupts generated
		// values. Only the step delays and the produce calls run concurrently.
		msgs, err := f.gen.Instance()
		if err != nil {
			f.stats.errors.Add(1)
			f.events.add(Event{
				At:      time.Now(),
				Message: fmt.Sprintf("flow %s: %v", f.name, err),
				Level:   scenario.EventWarn,
			})
			if f.concurrency != nil {
				<-f.concurrency
			}
			continue
		}

		f.stats.started.Add(1)
		f.stats.inflight.Add(1)

		g.Go(func() error {
			defer func() {
				if f.concurrency != nil {
					<-f.concurrency
				}
				f.stats.inflight.Add(-1)
			}()
			f.emit(gctx, msgs)
			return nil
		})
	}

	stop := time.AfterFunc(f.drain, cancelDrain)
	defer stop.Stop()

	return g.Wait()
}

// emit produces one already-generated instance, honouring per-step delays.
//
// Everything this touches is either the instance's own messages or an atomic counter, so
// it is safe to run many of these concurrently. Generation happens on the issuing
// goroutine -- see Run.
func (f *flowRunner) emit(ctx context.Context, msgs []generate.FlowMessage) {
	for _, m := range msgs {
		if m.DelayMS > 0 && !sleepCtx(ctx, time.Duration(m.DelayMS)*time.Millisecond) {
			return
		}

		// Flow payloads are always plain JSON and ignore the topic's data_format entirely.
		body, err := json.Marshal(m.Doc)
		if err != nil {
			f.stats.errors.Add(1)
			return
		}

		res := f.client.ProduceSync(ctx, &kgo.Record{Topic: m.Topic, Key: m.Key, Value: body})
		if err := res.FirstErr(); err != nil {
			f.stats.errors.Add(1)
			return
		}
		f.stats.messages.Add(1)
	}

	f.stats.completed.Add(1)
}

// buildFlow constructs the runner for one flow.
func (e *Engine) buildFlow(f scenario.Flow) error {
	gen, err := generate.NewFlowGen(f, e.rngFor("flow", f.Name, 0))
	if err != nil {
		return fmt.Errorf("flow generator: %w", err)
	}

	client, err := kafka.NewProducer(e.cfg.Kafka, defaultFlowProducerConf())
	if err != nil {
		return fmt.Errorf("flow producer client: %w", err)
	}
	e.trackClient(client)

	stats := &flowCounters{}
	e.flowStats[f.Name] = stats
	e.flowOrder = append(e.flowOrder, f.Name)
	e.flows = append(e.flows, newFlowRunner(f, gen, client, stats, e.events))
	return nil
}

// defaultFlowProducerConf is the client config flows produce with.
//
// Flows have no producer_config of their own in the YAML, so the scenario defaults apply.
func defaultFlowProducerConf() scenario.ProducerConf {
	return scenario.ProducerConf{
		BatchSize:       16384,
		LingerMS:        5,
		Acks:            "1",
		CompressionType: "lz4",
	}
}
