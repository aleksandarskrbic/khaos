package engine

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
	"github.com/aleksandarskrbic/khaos/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// pauseGate blocks a goroutine until it is resumed, without cancelling it.
//
// Pause is its own primitive, independent of cancellation: the single owning goroutine
// parks on a channel and resumes in place, so a paused consumer can never end up running a
// second loop and shutdown stays exclusively the job of context.
type pauseGate struct {
	mu sync.Mutex
	ch chan struct{} // non-nil means paused; closing it resumes
}

func (g *pauseGate) pause() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ch == nil {
		g.ch = make(chan struct{})
	}
}

func (g *pauseGate) resume() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ch != nil {
		close(g.ch)
		g.ch = nil
	}
}

func (g *pauseGate) paused() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ch != nil
}

// wait blocks while paused. It returns ctx.Err() if the run is cancelled while parked, so
// a paused consumer still shuts down promptly.
func (g *pauseGate) wait(ctx context.Context) error {
	g.mu.Lock()
	ch := g.ch
	g.mu.Unlock()
	if ch == nil {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Consumer polls one topic as part of a consumer group, optionally simulating failures.
type Consumer struct {
	ID      scenario.ID
	Topic   string
	GroupID string
	Topics  []string
	Conf    scenario.ConsumerConf

	client *kgo.Client
	gate   pauseGate

	// delayMS is the artificial per-message processing delay, mutable live by the
	// increase_consumer_delay incident.
	delayMS atomic.Int64

	rnd    *rand.Rand
	stats  *counters
	topicC *counters
	events *eventRing

	// dlq is lazily created, and only when on_failure is "dlq".
	dlq *dlqProducer

	// Prometheus counters resolved once at construction, topic+group labelled. Nil when
	// Config.Metrics is nil, checked before every use.
	promConsumed   prometheus.Counter
	promConsumeErr prometheus.Counter
}

type consumerOpts struct {
	id      scenario.ID
	topic   string
	groupID string
	topics  []string
	conf    scenario.ConsumerConf
	client  *kgo.Client
	delayMS int
	rnd     *rand.Rand
	topicC  *counters
	events  *eventRing
	dlq     *dlqProducer
	metrics *telemetry.Metrics
}

func newConsumer(o consumerOpts) *Consumer {
	c := &Consumer{
		ID:      o.id,
		Topic:   o.topic,
		GroupID: o.groupID,
		Topics:  o.topics,
		Conf:    o.conf,
		client:  o.client,
		rnd:     o.rnd,
		stats:   &counters{},
		topicC:  o.topicC,
		events:  o.events,
		dlq:     o.dlq,
	}
	c.delayMS.Store(int64(o.delayMS))
	if o.metrics != nil {
		c.promConsumed = o.metrics.ConsumedMessages.WithLabelValues(o.topic, o.groupID)
		c.promConsumeErr = o.metrics.ConsumeErrors.WithLabelValues(o.topic, o.groupID)
	}
	return c
}

func (c *Consumer) processingDelayMS() int { return int(c.delayMS.Load()) }

// SetDelay changes the artificial processing delay live.
func (c *Consumer) SetDelay(ms int) { c.delayMS.Store(int64(ms)) }

// Pause stops consuming without tearing the consumer down.
func (c *Consumer) Pause() { c.gate.pause() }

// Resume unblocks a paused consumer; the original goroutine simply continues.
func (c *Consumer) Resume() { c.gate.resume() }

// Paused reports whether this consumer is currently parked.
func (c *Consumer) Paused() bool { return c.gate.paused() }

// Run consumes until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		if err := c.gate.wait(ctx); err != nil {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}

		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}

		// Fetch-level errors are counted and the loop continues; a broker being down
		// must not kill the consumer.
		fetches.EachError(func(_ string, _ int32, err error) {
			if errors.Is(err, context.Canceled) {
				return
			}
			c.stats.consumeErr.Add(1)
			c.topicC.consumeErr.Add(1)
			if c.promConsumeErr != nil {
				c.promConsumeErr.Inc()
			}
		})

		fetches.EachRecord(func(rec *kgo.Record) {
			c.handle(ctx, rec)
		})
	}
}

// handle processes one record, applying failure simulation if configured.
func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) {
	if d := c.processingDelayMS(); d > 0 {
		select {
		case <-time.After(time.Duration(d) * time.Millisecond):
		case <-ctx.Done():
			return
		}
	}

	if !c.Conf.FailureSimulationEnabled() {
		c.stats.consumed.Add(1)
		c.topicC.consumed.Add(1)
		if c.promConsumed != nil {
			c.promConsumed.Inc()
		}
		return
	}

	// Failure simulation: roll for failure, dispatch on on_failure, and only commit on
	// success. The commit itself is subject to an independent commit_failure_rate that
	// "fails" simply by not committing.
	attempts := 0
	for {
		if c.rnd.Float64() >= c.Conf.FailureRate {
			break // processed successfully
		}

		c.stats.failed.Add(1)
		c.topicC.failed.Add(1)

		switch c.Conf.OnFailure {
		case "skip":
			return
		case "dlq":
			c.sendDLQ(ctx, rec)
			return
		case "retry":
			attempts++
			if attempts >= c.Conf.MaxRetries {
				return
			}
			c.stats.retries.Add(1)
			c.topicC.retries.Add(1)
			continue
		default:
			return
		}
	}

	c.stats.consumed.Add(1)
	c.topicC.consumed.Add(1)
	if c.promConsumed != nil {
		c.promConsumed.Inc()
	}
	c.maybeCommit(ctx, rec)
}

// maybeCommit commits the record unless the simulated commit failure fires.
//
// A "commit failure" here is not an error from the broker -- it is khaos declining to
// commit at all, which is what produces re-delivery on the next rebalance.
func (c *Consumer) maybeCommit(ctx context.Context, rec *kgo.Record) {
	if c.Conf.CommitFailureRate > 0 && c.rnd.Float64() < c.Conf.CommitFailureRate {
		c.stats.commitErr.Add(1)
		c.topicC.commitErr.Add(1)
		return
	}
	if err := c.client.CommitRecords(ctx, rec); err != nil {
		if !errors.Is(err, context.Canceled) {
			c.stats.commitErr.Add(1)
			c.topicC.commitErr.Add(1)
		}
	}
}

func (c *Consumer) sendDLQ(ctx context.Context, rec *kgo.Record) {
	if c.dlq == nil {
		return
	}
	if err := c.dlq.send(ctx, rec, "simulated processing failure"); err != nil {
		c.stats.consumeErr.Add(1)
		c.topicC.consumeErr.Add(1)
		if c.promConsumeErr != nil {
			c.promConsumeErr.Inc()
		}
		return
	}
	c.stats.dlq.Add(1)
	c.topicC.dlq.Add(1)
}

// Close releases the underlying client, leaving the group cleanly.
func (c *Consumer) Close() {
	if c.client != nil {
		c.client.CloseAllowingRebalance()
	}
}

// dlqProducer writes failed records to "{topic}-dlq".
//
// The DLQ topic is never created by khaos: it relies on broker auto-creation, which is
// disabled on most managed clusters, so on_failure: dlq silently fails against Confluent
// Cloud or Aiven. That failure is counted and surfaced rather than swallowed.
type dlqProducer struct {
	client *kgo.Client
}

// dlqMessage is the JSON envelope written to the DLQ topic.
type dlqMessage struct {
	OriginalTopic     string `json:"original_topic"`
	OriginalPartition int32  `json:"original_partition"`
	OriginalOffset    int64  `json:"original_offset"`
	Error             string `json:"error"`
	Timestamp         int64  `json:"timestamp"`
	Payload           string `json:"payload"`
	PayloadEncoding   string `json:"payload_encoding,omitempty"`
}

func (d *dlqProducer) send(ctx context.Context, rec *kgo.Record, reason string) error {
	msg := dlqMessage{
		OriginalTopic:     rec.Topic,
		OriginalPartition: rec.Partition,
		OriginalOffset:    rec.Offset,
		Error:             reason,
		Timestamp:         time.Now().UnixMilli(),
	}

	// The payload is base64-encoded when it is not valid UTF-8.
	if utf8.Valid(rec.Value) {
		msg.Payload = string(rec.Value)
	} else {
		msg.Payload = base64.StdEncoding.EncodeToString(rec.Value)
		msg.PayloadEncoding = "base64"
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal dlq message: %w", err)
	}

	res := d.client.ProduceSync(ctx, &kgo.Record{
		Topic: rec.Topic + "-dlq",
		Key:   rec.Key,
		Value: body,
	})
	return res.FirstErr()
}
