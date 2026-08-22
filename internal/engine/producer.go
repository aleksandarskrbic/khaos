package engine

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync/atomic"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
	"github.com/aleksandarskrbic/khaos/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/time/rate"
)

// Producer drives one stream of generated records at a controlled rate.
//
// One goroutine, calling the Kafka client directly -- no worker pool and no ceiling on
// concurrent producers.
type Producer struct {
	ID    scenario.ID
	Topic string

	client *kgo.Client

	// nextValue and nextKey are supplied by the engine so this type depends on neither
	// the generator nor the codec package.
	nextValue func() ([]byte, error)
	nextKey   func() []byte

	// limiter is per-producer, which is load-bearing: a change_producer_rate incident
	// targeted at one producer must retune only that producer, never every producer
	// sharing its topic.
	limiter *rate.Limiter

	dupRate float64
	rnd     *rand.Rand

	stats  *counters
	topicC *counters
	events *eventRing

	// Prometheus counters resolved once at construction (WithLabelValues per message
	// would mean a map lookup under lock on every send). Nil when Config.Metrics is nil,
	// checked before every use.
	promGenerated  prometheus.Counter
	promBytes      prometheus.Counter
	promProduceErr prometheus.Counter

	// inflight tracks records handed to the client but not yet acknowledged, so
	// shutdown can report whether the flush actually drained.
	inflight atomic.Int64
}

type producerOpts struct {
	id        scenario.ID
	topic     string
	client    *kgo.Client
	nextValue func() ([]byte, error)
	nextKey   func() []byte
	rate      float64
	dupRate   float64
	rnd       *rand.Rand
	topicC    *counters
	events    *eventRing
	metrics   *telemetry.Metrics
}

func newProducer(o producerOpts) *Producer {
	p := &Producer{
		ID:        o.id,
		Topic:     o.topic,
		client:    o.client,
		nextValue: o.nextValue,
		nextKey:   o.nextKey,
		limiter:   newLimiter(o.rate),
		dupRate:   o.dupRate,
		rnd:       o.rnd,
		stats:     &counters{},
		topicC:    o.topicC,
		events:    o.events,
	}
	if o.metrics != nil {
		p.promGenerated = o.metrics.GeneratedMessages.WithLabelValues(o.topic)
		p.promBytes = o.metrics.GeneratedBytes.WithLabelValues(o.topic)
		p.promProduceErr = o.metrics.ProduceErrors.WithLabelValues(o.topic)
	}
	return p
}

// newLimiter builds a rate limiter for a target messages-per-second.
//
// A rate of zero means unlimited. Burst is 1 so pacing is even rather than bursty. A
// token bucket actually reaches the configured rate even at high settings, where
// sleep-based pacing silently under-delivers -- so a scenario tuned against a
// sleep-paced producer may push materially more traffic at the same YAML values.
func newLimiter(perSecond float64) *rate.Limiter {
	if perSecond <= 0 {
		return rate.NewLimiter(rate.Inf, 1)
	}
	return rate.NewLimiter(rate.Limit(perSecond), 1)
}

// SetRate retunes the running producer to a new target rate, live. Relies on the
// per-producer limiter above so a targeted rate change retunes only the targeted
// producer, not every producer on its topic.
func (p *Producer) SetRate(perSecond float64) {
	if perSecond <= 0 {
		p.limiter.SetLimit(rate.Inf)
		return
	}
	p.limiter.SetLimit(rate.Limit(perSecond))
}

// Rate reports the current target rate, with +Inf meaning unlimited.
func (p *Producer) Rate() float64 { return float64(p.limiter.Limit()) }

// Run produces until ctx is cancelled.
//
// Returns nil on cancellation: a cancelled run is a successful run, not an error. Only a
// genuine generator failure propagates, because that means the scenario itself is
// unusable and every other producer will hit the same thing.
func (p *Producer) Run(ctx context.Context) error {
	for {
		if err := p.limiter.Wait(ctx); err != nil {
			return nil // context cancelled or deadline exceeded
		}

		value, err := p.nextValue()
		if err != nil {
			p.stats.produceErr.Add(1)
			p.topicC.produceErr.Add(1)
			if p.promProduceErr != nil {
				p.promProduceErr.Inc()
			}
			return err
		}
		var key []byte
		if p.nextKey != nil {
			key = p.nextKey()
		}

		p.produce(ctx, key, value)

		// Duplicates re-send the identical key and value and count toward the rate
		// budget, so a duplicate_rate of 1.0 halves the effective distinct-message rate.
		if p.dupRate > 0 && p.rnd.Float64() < p.dupRate {
			if err := p.limiter.Wait(ctx); err != nil {
				return nil
			}
			p.produce(ctx, key, value)
			p.stats.duplicate.Add(1)
			p.topicC.duplicate.Add(1)
		}
	}
}

// produce hands one record to the client. Delivery is asynchronous; the promise updates
// counters when the broker acknowledges, so the reported "produced" figure is DELIVERED
// count rather than attempted.
func (p *Producer) produce(ctx context.Context, key, value []byte) {
	rec := &kgo.Record{Topic: p.Topic, Key: key, Value: value}
	size := int64(len(value) + len(key))

	p.inflight.Add(1)
	p.client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		p.inflight.Add(-1)
		if err != nil {
			// A cancelled context during shutdown is expected and not an error worth
			// counting or reporting.
			//
			// kgo.ErrClientClosed is NOT filtered here. Client.Close fails every record
			// still buffered after the flush deadline, and those are genuinely undelivered
			// messages. The promise fires exactly once per record, so a record cannot be
			// counted as both sent and errored.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			p.stats.produceErr.Add(1)
			p.topicC.produceErr.Add(1)
			if p.promProduceErr != nil {
				p.promProduceErr.Inc()
			}
			return
		}
		p.stats.sent.Add(1)
		p.stats.bytes.Add(size)
		p.topicC.sent.Add(1)
		p.topicC.bytes.Add(size)
		if p.promGenerated != nil {
			p.promGenerated.Inc()
			p.promBytes.Add(float64(size))
		}
	})
}

// Inflight reports records handed to the client but not yet acknowledged.
func (p *Producer) Inflight() int64 { return p.inflight.Load() }
