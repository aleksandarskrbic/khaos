package telemetry

import "github.com/prometheus/client_golang/prometheus"

// Namespace prefixes every metric so a shared Prometheus can tell khaos series apart.
const Namespace = "khaos"

// Metrics is the complete khaos metric set.
//
// The collectors are plain exported fields rather than being hidden behind methods: they
// are already the API Prometheus intends callers to use, and a wrapper method per counter
// would be pure ceremony. The one exception is SetPhase, which needs the reset semantics
// documented on it.
//
// The set is deliberately minimal -- exactly the signals the run loop produces today.
// Adding one later means adding a field, registering it in NewMetrics and nothing else.
type Metrics struct {
	// GeneratedMessages counts messages handed to the producer, per topic. This is the
	// generator's output, so it counts messages that later fail to produce too;
	// ProduceErrors is the difference.
	GeneratedMessages *prometheus.CounterVec

	// GeneratedBytes counts serialised payload bytes per topic.
	GeneratedBytes *prometheus.CounterVec

	// ProduceErrors counts failed produce attempts per topic.
	ProduceErrors *prometheus.CounterVec

	// ConsumedMessages counts records delivered to a consumer, per topic and group.
	ConsumedMessages *prometheus.CounterVec

	// ConsumeErrors counts consume-side failures per topic and group. It includes the
	// injected failures from ConsumerConf.FailureRate, which are indistinguishable from
	// real ones by design -- that is the point of the chaos simulation.
	ConsumeErrors *prometheus.CounterVec

	// Reconnects counts client reconnections to a broker.
	Reconnects *prometheus.CounterVec

	// Rebalances counts consumer group rebalances, per group.
	Rebalances *prometheus.CounterVec

	// ScenarioTransitions counts scenario phase changes.
	ScenarioTransitions prometheus.Counter

	// currentPhase is 1 for the active (scenario, phase) pair and absent otherwise.
	// Unexported because it is only correct when mutated through SetPhase.
	currentPhase *prometheus.GaugeVec
}

// Label names, kept as constants so a typo cannot silently create a second series.
const (
	labelTopic    = "topic"
	labelGroup    = "group"
	labelBroker   = "broker"
	labelScenario = "scenario"
	labelPhase    = "phase"
)

// NewMetrics creates the metric set and registers it with reg.
//
// It takes an explicit registry rather than using prometheus.DefaultRegisterer so tests
// and multiple in-process runs cannot collide on duplicate registration. It panics on a
// registration error via MustRegister: a duplicate metric name is a programming mistake
// in this file, not a runtime condition a caller could handle.
func NewMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		GeneratedMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "messages_generated_total",
			Help:      "Messages generated and handed to a producer.",
		}, []string{labelTopic}),

		GeneratedBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "bytes_generated_total",
			Help:      "Serialized payload bytes generated.",
		}, []string{labelTopic}),

		ProduceErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "produce_errors_total",
			Help:      "Failed produce attempts.",
		}, []string{labelTopic}),

		ConsumedMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "messages_consumed_total",
			Help:      "Records delivered to a consumer.",
		}, []string{labelTopic, labelGroup}),

		ConsumeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "consume_errors_total",
			Help:      "Consume-side failures, including simulated ones.",
		}, []string{labelTopic, labelGroup}),

		Reconnects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "reconnects_total",
			Help:      "Client reconnections to a broker.",
		}, []string{labelBroker}),

		Rebalances: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "rebalances_total",
			Help:      "Consumer group rebalances.",
		}, []string{labelGroup}),

		ScenarioTransitions: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "scenario_transitions_total",
			Help:      "Scenario phase transitions.",
		}),

		currentPhase: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "current_phase",
			Help:      "1 for the scenario phase currently running.",
		}, []string{labelScenario, labelPhase}),
	}

	reg.MustRegister(
		m.GeneratedMessages,
		m.GeneratedBytes,
		m.ProduceErrors,
		m.ConsumedMessages,
		m.ConsumeErrors,
		m.Reconnects,
		m.Rebalances,
		m.ScenarioTransitions,
		m.currentPhase,
	)
	return m
}

// SetPhase marks (scenario, phase) as the one currently running.
//
// The gauge is reset first so exactly one series exists at any time. Without the reset,
// every phase a run passes through would stay pinned at 1 and `khaos_current_phase == 1`
// would match the whole history instead of the present.
func (m *Metrics) SetPhase(scenario, phase string) {
	m.currentPhase.Reset()
	m.currentPhase.WithLabelValues(scenario, phase).Set(1)
}
