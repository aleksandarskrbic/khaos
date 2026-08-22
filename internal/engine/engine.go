package engine

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aleksandarskrbic/khaos/internal/codec"
	"github.com/aleksandarskrbic/khaos/internal/generate"
	"github.com/aleksandarskrbic/khaos/internal/kafka"
	"github.com/aleksandarskrbic/khaos/internal/scenario"
	"github.com/aleksandarskrbic/khaos/internal/telemetry"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Defaults governing engine lifecycle. Deliberately not configurable: they are safety
// rails, not tuning knobs.
const (
	// flushTimeout bounds the post-cancellation drain of buffered records, per producer.
	flushTimeout = 5 * time.Second

	// teardownDeadline is the hard ceiling on the entire shutdown sequence. Past this the
	// process dumps goroutines and exits non-zero rather than hanging: for a daemon in
	// Kubernetes it is far better to be SIGKILLed holding a stack trace than to hang past
	// terminationGracePeriodSeconds.
	teardownDeadline = 15 * time.Second

	// eventCapacity bounds the in-memory event ring. This process is expected to run for
	// weeks, so unbounded history is a slow leak.
	eventCapacity = 256
)

// Config is everything the engine needs to run.
type Config struct {
	Kafka     kafka.Config
	Scenarios []*scenario.Scenario

	// Duration of zero means run until the context is cancelled.
	Duration time.Duration

	NoConsumers    bool
	RecreateTopics bool

	// SkipTopicCreation leaves topic management entirely alone: nothing is created and
	// nothing is deleted.
	//
	// It exists for clusters where khaos has no CreateTopics permission, which is the
	// normal case on a managed cluster, so conflating it with RecreateTopics (only
	// skipping the DELETE half) leaves exactly that user failing at startup on the
	// CreateTopics call.
	SkipTopicCreation bool

	// Seed makes generated data reproducible. A zero Seed means "pick one", and the
	// chosen value is reported so a run can be replayed.
	Seed uint64

	// Brokers is nil for external clusters, where broker faults are unavailable.
	Brokers BrokerController

	// Registry is the Schema Registry endpoint and its credentials.
	//
	// A bare URL is enough for the bundled local registry; Confluent Cloud and Aiven
	// require auth. An empty Registry.URL falls back to the scenario's own
	// `schema_registry:` block.
	Registry codec.RegistryConfig

	// LagPoll is how often to ask the brokers for real consumer-group lag. Zero disables
	// polling, which is the default.
	//
	// The self-reported Lag figure is produced-minus-consumed from khaos's own in-process
	// counters -- wrong the moment anything else touches the topic, and meaningless
	// across restarts. Real lag needs admin traffic and DESCRIBE rights, which a managed
	// cluster may not grant, so it is opt-in and MUST degrade to "unknown" rather than
	// failing the run.
	LagPoll time.Duration

	Logger *slog.Logger

	// Metrics is where per-message Prometheus counters are recorded, keyed by topic and
	// group as telemetry.Metrics defines. Nil disables it entirely -- every call site
	// checks before touching it, so an engine built without --metrics-addr pays nothing
	// beyond the nil check.
	Metrics *telemetry.Metrics
}

// Engine owns a run: every producer, consumer, the incident scheduler, and the counters
// they feed.
//
// The engine has exactly one read API -- Snapshot -- and knows nothing about terminals,
// colours or rendering. The duration deadline and shutdown trigger live in the engine
// itself, never in an output loop.
type Engine struct {
	cfg Config
	log *slog.Logger

	reg    *registry
	sched  *scheduler
	events *eventRing

	admin *kafka.Admin

	// topicStats is keyed by topic name and shared by every producer and consumer on
	// that topic.
	topicStats map[string]*counters
	topicMeta  map[string]topicMeta
	topicOrder []string

	flowStats map[string]*flowCounters
	flowOrder []string
	flows     []*flowRunner

	// clients are tracked so teardown can flush and close them all.
	producerClients []*kgo.Client
	clientsMu       sync.Mutex

	// consumerSpec lets the scheduler rebuild a consumer during a rebalance.
	consumerSpecs map[string]consumerSpec
	specMu        sync.Mutex

	// pending carries consumers created mid-run (by a rebalance incident) to the run
	// supervisor, so their goroutines join the run's errgroup and are awaited at
	// shutdown.
	pending chan *Consumer

	// started is unix nanos, not a time.Time, because Run writes it while the caller's
	// output loop is already polling Snapshot -- that is the documented usage, and a
	// 24-byte struct field cannot be written and read concurrently without a race.
	started  atomic.Int64
	stopping atomic.Bool
	healthy  atomic.Bool

	seed uint64
}

type topicMeta struct {
	scenarioName string
	groups       []string
}

type flowCounters struct {
	started   atomic.Int64
	completed atomic.Int64
	messages  atomic.Int64
	errors    atomic.Int64
	inflight  atomic.Int64
	saturated atomic.Int64
}

// consumerSpec is everything needed to construct a replacement consumer.
type consumerSpec struct {
	topic string
	conf  scenario.ConsumerConf
}

// New builds an engine: it creates topics, constructs codecs and clients, and registers
// every producer and consumer. It performs I/O and can fail.
//
// Construction is deliberately separate from Run so that a configuration or connectivity
// problem surfaces before any traffic is generated and before any lifecycle exists to
// cancel it.
func New(ctx context.Context, cfg Config) (*Engine, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	seed := cfg.Seed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}

	events := newEventRing(eventCapacity)

	e := &Engine{
		cfg:           cfg,
		log:           cfg.Logger,
		reg:           newRegistry(),
		events:        events,
		topicStats:    make(map[string]*counters),
		topicMeta:     make(map[string]topicMeta),
		flowStats:     make(map[string]*flowCounters),
		consumerSpecs: make(map[string]consumerSpec),
		seed:          seed,
	}
	e.healthy.Store(true)
	// Seeded here so a Snapshot taken before Run reports an elapsed of roughly zero
	// rather than the time since the epoch.
	e.started.Store(time.Now().UnixNano())

	brokers := cfg.Brokers
	if brokers == nil {
		brokers = noopBrokers{events: events}
	}
	e.sched = &scheduler{
		reg:     e.reg,
		events:  events,
		brokers: brokers,
		create:  e.recreateConsumer,
		rnd:     rand.New(rand.NewPCG(seed, seed>>1)),
		metrics: cfg.Metrics,
	}

	admin, err := kafka.NewAdmin(cfg.Kafka)
	if err != nil {
		return nil, fmt.Errorf("connect to cluster: %w", err)
	}
	e.admin = admin

	if err := e.setupTopics(ctx); err != nil {
		admin.Close()
		return nil, err
	}
	if err := e.build(ctx); err != nil {
		admin.Close()
		e.closeAll()
		return nil, err
	}

	return e, nil
}

// Flow step topics are not declared in `topics:` and so carry no partition count or
// replication factor of their own; these two constants supply the defaults.
const (
	flowTopicPartitions        = 12
	flowTopicReplicationFactor = 3
)

// setupTopics creates every topic the scenarios reference, declared or implied.
//
// Flow step topics are included explicitly: on any cluster with
// auto.create.topics.enable=false -- the default for Confluent Cloud, Aiven and MSK --
// omitting them means every flow produce fails with UNKNOWN_TOPIC_OR_PARTITION and the
// flow counters stay at zero with no explanation.
func (e *Engine) setupTopics(ctx context.Context) error {
	if e.cfg.SkipTopicCreation {
		// No admin calls at all. The user is telling us the topics already exist and that
		// we may not have the rights to touch them.
		return nil
	}

	var topics []scenario.Topic
	declared := make(map[string]bool)
	for _, sc := range e.cfg.Scenarios {
		for _, t := range sc.Topics {
			topics = append(topics, t)
			declared[t.Name] = true
		}
	}
	// After every declaration, so a flow step naming a declared topic uses that topic's
	// own settings rather than the flow defaults.
	for _, sc := range e.cfg.Scenarios {
		for _, f := range sc.Flows {
			for _, name := range f.Topics() {
				if declared[name] {
					continue
				}
				declared[name] = true
				topics = append(topics, scenario.Topic{
					Name:              name,
					Partitions:        flowTopicPartitions,
					ReplicationFactor: flowTopicReplicationFactor,
				})
			}
		}
	}
	if len(topics) == 0 {
		return nil
	}
	if err := e.admin.EnsureTopics(ctx, topics, e.cfg.RecreateTopics); err != nil {
		return fmt.Errorf("create topics: %w", err)
	}
	return nil
}

// schemaRegistryURL resolves which registry to talk to.
//
// A scenario carries its own `schema_registry:` block. Reading only the CLI flag would
// silently downgrade every avro/protobuf scenario to the bare no-registry encoding: it
// produces happily, registers nothing, and emits bytes no registry-aware consumer can
// read. The flag stays as an override so a scenario can be pointed at a different
// registry without editing it.
func (e *Engine) schemaRegistryURL() string {
	if e.cfg.Registry.URL != "" {
		return e.cfg.Registry.URL
	}
	for _, sc := range e.cfg.Scenarios {
		if sc.SchemaRegistry != nil && sc.SchemaRegistry.URL != "" {
			return sc.SchemaRegistry.URL
		}
	}
	return ""
}

// build constructs producers, consumers and flow workers for every scenario.
func (e *Engine) build(ctx context.Context) error {
	var reg *codec.Registry
	if url := e.schemaRegistryURL(); url != "" {
		rc := e.cfg.Registry
		rc.URL = url // the scenario's own block wins when no --schema-registry-url was given
		r, err := codec.NewRegistryWithConfig(rc)
		if err != nil {
			// The codec error already names the URL and, for a 401/403, which flag is
			// probably wrong. Repeating the URL here just doubled it in the output.
			return fmt.Errorf("schema registry: %w", err)
		}
		reg = r
	}

	for _, sc := range e.cfg.Scenarios {
		for _, topic := range sc.Topics {
			if err := e.buildTopic(ctx, sc.Name, topic, reg); err != nil {
				return fmt.Errorf("topic %q: %w", topic.Name, err)
			}
		}
		for _, flow := range sc.Flows {
			if err := e.buildFlow(flow); err != nil {
				return fmt.Errorf("flow %q: %w", flow.Name, err)
			}
		}
	}
	return nil
}

func (e *Engine) buildTopic(ctx context.Context, scenarioName string, t scenario.Topic, reg *codec.Registry) error {
	stats := &counters{}
	e.topicStats[t.Name] = stats
	e.topicOrder = append(e.topicOrder, t.Name)

	// One codec per topic, shared by every producer on it. Unlike the generators, a
	// codec.Codec is safe to share: New resolves the schema text, the parsed descriptor
	// and the registry id once, and each Encode only reads them (jsonCodec is a stateless
	// value, avroCodec holds an immutable *avro.RecordSchema, protoCodec builds a fresh
	// dynamicpb message per call). Building one per producer would instead mean one
	// registry registration per producer.
	cdc, err := codec.New(ctx, t, reg)
	if err != nil {
		return fmt.Errorf("codec: %w", err)
	}

	// Each producer gets its own RNG so that generation is reproducible per component and
	// there is no shared global to contend on.
	for i := 0; i < t.NumProducers; i++ {
		rnd := e.rngFor("producer", t.Name, i)

		nextValue, err := valueFunc(t, cdc, rnd)
		if err != nil {
			return err
		}
		nextKey, err := generate.NewKeyGen(t.MessageSchema, e.rngFor("key", t.Name, i))
		if err != nil {
			return fmt.Errorf("key generator: %w", err)
		}

		client, err := kafka.NewProducer(e.cfg.Kafka, t.ProducerConfig)
		if err != nil {
			return fmt.Errorf("producer client: %w", err)
		}
		e.trackClient(client)

		e.reg.addProducer(newProducer(producerOpts{
			id:        e.reg.mintID("producer"),
			topic:     t.Name,
			client:    client,
			nextValue: nextValue,
			nextKey:   nextKey,
			rate:      t.ProducerRate,
			dupRate:   t.ProducerConfig.DuplicateRate,
			rnd:       rnd,
			topicC:    stats,
			events:    e.events,
			metrics:   e.cfg.Metrics,
		}))
	}

	meta := topicMeta{scenarioName: scenarioName}

	if !e.cfg.NoConsumers {
		for gi := 0; gi < t.NumConsumerGroups; gi++ {
			groupID := fmt.Sprintf("%s-group-%d", t.Name, gi+1)
			meta.groups = append(meta.groups, groupID)

			e.specMu.Lock()
			e.consumerSpecs[groupID] = consumerSpec{topic: t.Name, conf: t.ConsumerConfig}
			e.specMu.Unlock()

			for ci := 0; ci < t.ConsumersPerGroup; ci++ {
				if _, err := e.addConsumer(groupID, t.Name, []string{t.Name}, t.ConsumerDelayMS, t.ConsumerConfig); err != nil {
					return err
				}
			}
		}
	}

	e.topicMeta[t.Name] = meta
	return nil
}

// addConsumer constructs and registers one consumer, returning it so the caller can start
// its goroutine.
//
// It is called both from build (single-threaded) and from the scheduler mid-run, which is
// why it must not touch any engine map that Snapshot reads without a lock.
func (e *Engine) addConsumer(groupID, topic string, topics []string, delayMS int, conf scenario.ConsumerConf) (*Consumer, error) {
	client, err := kafka.NewConsumer(e.cfg.Kafka, groupID, topics, conf)
	if err != nil {
		return nil, fmt.Errorf("consumer client: %w", err)
	}

	// topicStats is written only during build and read by Snapshot for the whole run, so
	// a mid-run rebalance must not insert into it. A topic absent from the map is also
	// absent from topicOrder, so a fresh entry would never be rendered anyway -- the
	// consumer gets unpublished counters instead of a map write racing every Snapshot.
	stats := e.topicStats[topic]
	if stats == nil {
		stats = &counters{}
	}

	var dlq *dlqProducer
	if conf.OnFailure == "dlq" {
		// One DLQ client per consumer rather than one per group: a *kgo.Client is safe to
		// share, but sharing would make a single slow DLQ write batch behind every other
		// consumer's, and the failure path is exactly where that matters.
		dlqClient, err := kafka.NewProducer(e.cfg.Kafka, scenario.ProducerConf{Acks: "all", CompressionType: "none"})
		if err != nil {
			// The consumer client is not registered anywhere yet, so nothing else will
			// ever close it.
			client.Close()
			return nil, fmt.Errorf("dlq producer: %w", err)
		}
		e.trackClient(dlqClient)
		dlq = &dlqProducer{client: dlqClient}
	}

	id := e.reg.mintID("consumer")
	c := newConsumer(consumerOpts{
		id:      id,
		topic:   topic,
		groupID: groupID,
		topics:  topics,
		conf:    conf,
		client:  client,
		delayMS: delayMS,
		rnd:     e.rngFor("consumer", string(id), 0),
		topicC:  stats,
		events:  e.events,
		dlq:     dlq,
		metrics: e.cfg.Metrics,
	})
	e.reg.addConsumer(c)
	return c, nil
}

// recreateConsumer is the scheduler's hook for the rebalance incident.
//
// The replacement is registered under a NEW id rather than overwriting a slot, so a
// stale-index divergence between the registry and any by-* index cannot occur.
//
// The conf passed in is whatever RebalanceConsumer put in the CreateConsumer command,
// which is the ZERO ConsumerConf: a rebuilt consumer carries only group_id and
// processing_delay_ms, so it loses failure_rate, commit_failure_rate, on_failure and
// max_retries (see scenario.ConsumerRef.Conf). consumerSpecs is consulted only to find
// which topic the group belongs to.
func (e *Engine) recreateConsumer(ctx context.Context, groupID string, topics []string, delayMS int, conf scenario.ConsumerConf) error {
	e.specMu.Lock()
	spec, ok := e.consumerSpecs[groupID]
	e.specMu.Unlock()

	topic := spec.topic
	if !ok && len(topics) > 0 {
		topic = topics[0]
	}

	c, err := e.addConsumer(groupID, topic, topics, delayMS, conf)
	if err != nil {
		return err
	}

	// The new consumer needs its own goroutine, which the run loop cannot know about.
	// It is launched into the run's errgroup via the pending channel so it is still
	// awaited at shutdown.
	//
	// If the run is already shutting down nobody is reading pending. The consumer is
	// registered either way, so drain still closes its client; it simply never polls.
	if e.pending != nil {
		select {
		case e.pending <- c:
		case <-ctx.Done():
		}
	}
	return nil
}

func (e *Engine) trackClient(c *kgo.Client) {
	e.clientsMu.Lock()
	defer e.clientsMu.Unlock()
	e.producerClients = append(e.producerClients, c)
}

// rngFor derives a deterministic per-component RNG from the run seed.
func (e *Engine) rngFor(kind, name string, index int) *rand.Rand {
	h := uint64(14695981039346656037)
	mix := func(s string) {
		for i := 0; i < len(s); i++ {
			h ^= uint64(s[i])
			h *= 1099511628211
		}
	}
	mix(kind)
	mix(name)
	h ^= uint64(index) * 0x9E3779B97F4A7C15
	return rand.New(rand.NewPCG(e.seed, h))
}

// valueFunc builds the per-message value producer for a topic.
//
// Two paths: when message_schema.fields is set the document is built from the field
// definitions and encoded by the codec; otherwise a synthetic JSON document is emitted
// with size padding and the codec is bypassed entirely.
func valueFunc(t scenario.Topic, cdc codec.Codec, rnd *rand.Rand) (func() ([]byte, error), error) {
	if len(t.MessageSchema.Fields) > 0 {
		gen, err := generate.NewDocGen(t.MessageSchema.Fields, rnd)
		if err != nil {
			return nil, fmt.Errorf("document generator: %w", err)
		}
		return func() ([]byte, error) { return cdc.Encode(toCodecDoc(gen.Next())) }, nil
	}

	raw := generate.NewRawJSONGen(t.MessageSchema, rnd)
	return func() ([]byte, error) { return raw.Next(), nil }, nil
}

// Seed reports the seed this run used, so a run can be reproduced.
func (e *Engine) Seed() uint64 { return e.seed }

// toCodecDoc bridges the generator's ordered document to the codec's.
//
// The two packages each define an ordered document with an identical method set --
// generate needs one to build payloads, codec needs one to describe wire shape, and
// neither should depend on the other. Converting here keeps that separation at the cost
// of one allocation per message, and preserves key order, which is contractual: keys must
// stay in YAML declaration order, since a Go map would sort them and silently change
// every payload khaos emits.
func toCodecDoc(src *generate.Doc) *codec.Doc {
	dst := codec.NewDoc()
	for _, k := range src.Keys() {
		v, _ := src.Get(k)
		dst.Set(k, v)
	}
	return dst
}
