package scenario

import (
	"strings"
	"testing"
)

// decodeOK decodes a document that is expected to be free of errors.
func decodeOK(t *testing.T, doc string) *Scenario {
	t.Helper()
	sc, diags := Decode([]byte(doc))
	if diags.HasErrors() {
		t.Fatalf("unexpected errors:\n%s", diags.Errors())
	}
	if sc == nil {
		t.Fatal("no errors reported but scenario is nil")
	}
	return sc
}

// TestDecodeAppliesDefaults pins every default that an omitted key must produce.
func TestDecodeAppliesDefaults(t *testing.T) {
	sc := decodeOK(t, "name: s\ntopics:\n  - name: orders\n")
	if len(sc.Topics) != 1 {
		t.Fatalf("want 1 topic, got %d", len(sc.Topics))
	}
	topic := sc.Topics[0]

	ints := []struct {
		name string
		got  int
		want int
	}{
		{"partitions", topic.Partitions, defaultPartitions},
		{"replication_factor", topic.ReplicationFactor, defaultReplicationFactor},
		{"num_producers", topic.NumProducers, defaultNumProducers},
		{"num_consumer_groups", topic.NumConsumerGroups, defaultConsumerGroups},
		{"consumers_per_group", topic.ConsumersPerGroup, defaultConsumersPerGroup},
		{"consumer_delay_ms", topic.ConsumerDelayMS, defaultConsumerDelayMS},
		{"key_cardinality", topic.MessageSchema.KeyCardinality, defaultKeyCardinality},
		{"min_size_bytes", topic.MessageSchema.MinSizeBytes, defaultMinSizeBytes},
		{"max_size_bytes", topic.MessageSchema.MaxSizeBytes, defaultMaxSizeBytes},
		{"batch_size", topic.ProducerConfig.BatchSize, defaultBatchSize},
		{"linger_ms", topic.ProducerConfig.LingerMS, defaultLingerMS},
		{"max_retries", topic.ConsumerConfig.MaxRetries, defaultMaxRetries},
	}
	for _, tc := range ints {
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.want)
		}
	}

	strs := []struct {
		name string
		got  string
		want string
	}{
		{"schema_provider", topic.SchemaProvider, defaultSchemaProvider},
		{"key_distribution", topic.MessageSchema.KeyDistribution, defaultKeyDistribution},
		{"data_format", topic.MessageSchema.DataFormat, defaultDataFormat},
		{"acks", topic.ProducerConfig.Acks, defaultAcks},
		{"compression_type", topic.ProducerConfig.CompressionType, defaultCompressionType},
		{"on_failure", topic.ConsumerConfig.OnFailure, defaultOnFailure},
	}
	for _, tc := range strs {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}

	if topic.ProducerRate != defaultProducerRate {
		t.Errorf("producer_rate = %v, want %v", topic.ProducerRate, defaultProducerRate)
	}
	if topic.ConsumerConfig.FailureRate != defaultFailureRate ||
		topic.ConsumerConfig.CommitFailureRate != defaultCommitFailureRate {
		t.Errorf("consumer failure rates = %v/%v, want zero",
			topic.ConsumerConfig.FailureRate, topic.ConsumerConfig.CommitFailureRate)
	}
}

// TestDecodePartialBlocksKeepDefaults pins that a block setting one key must not zero
// the others.
func TestDecodePartialBlocksKeepDefaults(t *testing.T) {
	sc := decodeOK(t, `
name: s
topics:
  - name: orders
    partitions: 3
    producer_config:
      acks: all
    consumer_config:
      failure_rate: 0.25
    message_schema:
      key_distribution: zipfian
`)
	topic := sc.Topics[0]
	if topic.Partitions != 3 {
		t.Errorf("partitions = %d, want 3", topic.Partitions)
	}
	if topic.ReplicationFactor != defaultReplicationFactor {
		t.Errorf("replication_factor = %d, want the default %d", topic.ReplicationFactor, defaultReplicationFactor)
	}
	if topic.ProducerConfig.Acks != "all" {
		t.Errorf("acks = %q, want all", topic.ProducerConfig.Acks)
	}
	if topic.ProducerConfig.CompressionType != defaultCompressionType {
		t.Errorf("compression_type = %q, want the default %q", topic.ProducerConfig.CompressionType, defaultCompressionType)
	}
	if topic.ConsumerConfig.FailureRate != 0.25 {
		t.Errorf("failure_rate = %v, want 0.25", topic.ConsumerConfig.FailureRate)
	}
	if topic.ConsumerConfig.MaxRetries != defaultMaxRetries {
		t.Errorf("max_retries = %d, want the default %d", topic.ConsumerConfig.MaxRetries, defaultMaxRetries)
	}
	if topic.MessageSchema.KeyCardinality != defaultKeyCardinality {
		t.Errorf("key_cardinality = %d, want the default %d", topic.MessageSchema.KeyCardinality, defaultKeyCardinality)
	}
	if !topic.ConsumerConfig.FailureSimulationEnabled() {
		t.Error("a non-zero failure_rate must switch the consumer to manual commit")
	}
}

// TestDecodeFieldDefaultsAtEveryDepth pins that Field's defaults survive recursion into
// nested object fields and array item schemas.
func TestDecodeFieldDefaultsAtEveryDepth(t *testing.T) {
	sc := decodeOK(t, `
name: s
topics:
  - name: orders
    message_schema:
      fields:
        - name: outer
          type: object
          fields:
            - name: inner
              type: array
              items:
                type: array
                items:
                  type: string
`)
	outer := sc.Topics[0].MessageSchema.Fields[0]
	if outer.MinItems != defaultMinItems || outer.MaxItems != defaultMaxItems {
		t.Errorf("outer items bounds = %d/%d, want %d/%d",
			outer.MinItems, outer.MaxItems, defaultMinItems, defaultMaxItems)
	}
	inner := outer.Fields[0]
	if inner.MinItems != defaultMinItems || inner.MaxItems != defaultMaxItems {
		t.Errorf("nested field bounds = %d/%d, want %d/%d",
			inner.MinItems, inner.MaxItems, defaultMinItems, defaultMaxItems)
	}
	if inner.Items == nil {
		t.Fatal("array items were not decoded")
	}
	if inner.Items.MinItems != defaultMinItems || inner.Items.MaxItems != defaultMaxItems {
		t.Errorf("array item bounds = %d/%d, want %d/%d",
			inner.Items.MinItems, inner.Items.MaxItems, defaultMinItems, defaultMaxItems)
	}
	if inner.Items.Items == nil || inner.Items.Items.Type != FieldString {
		t.Error("the innermost item schema was lost")
	}
}

// TestDecodeFieldPointersDistinguishZeroFromAbsent pins that `min: 0` decodes as a real
// constraint, not an absent one.
func TestDecodeFieldPointersDistinguishZeroFromAbsent(t *testing.T) {
	sc := decodeOK(t, `
name: s
topics:
  - name: orders
    message_schema:
      fields:
        - name: bounded
          type: int
          min: 0
          max: 10
        - name: unbounded
          type: int
`)
	fields := sc.Topics[0].MessageSchema.Fields
	if fields[0].Min == nil || *fields[0].Min != 0 {
		t.Errorf("min = %v, want a pointer to 0", fields[0].Min)
	}
	if fields[1].Min != nil || fields[1].Max != nil {
		t.Errorf("omitted bounds decoded as %v/%v, want nil", fields[1].Min, fields[1].Max)
	}
}

func TestDecodeFlowDefaults(t *testing.T) {
	sc := decodeOK(t, `
name: s
flows:
  - name: order-processing
    steps:
      - topic: orders
        event_type: created
        consumers: {}
      - topic: payments
        event_type: paid
`)
	flow := sc.Flows[0]
	if flow.Rate != defaultFlowRate {
		t.Errorf("rate = %v, want %v", flow.Rate, defaultFlowRate)
	}
	if flow.Correlation.Type != CorrelationUUID {
		t.Errorf("correlation type = %q, want %q", flow.Correlation.Type, CorrelationUUID)
	}
	consumers := flow.Steps[0].Consumers
	if consumers == nil {
		t.Fatal("step consumers were not decoded")
	}
	if consumers.Groups != defaultStepGroups || consumers.PerGroup != defaultStepPerGroup || consumers.DelayMS != defaultStepDelayMS {
		t.Errorf("step consumers = %+v, want the defaults %d/%d/%d",
			*consumers, defaultStepGroups, defaultStepPerGroup, defaultStepDelayMS)
	}
	if flow.Steps[1].Consumers != nil {
		t.Error("a step without a consumers block must decode to nil, not to defaults")
	}
	if got := flow.Topics(); len(got) != 2 || got[0] != "orders" || got[1] != "payments" {
		t.Errorf("Topics() = %v, want [orders payments] in first-appearance order", got)
	}
}

// TestDecodeCorrelationDefaultsWithinABlock pins that a correlation block omitting
// `type` still gets uuid rather than the empty string.
func TestDecodeCorrelationDefaultsWithinABlock(t *testing.T) {
	sc := decodeOK(t, `
name: s
flows:
  - name: f
    correlation:
      field: order_id
    steps:
      - topic: a
        event_type: created
      - topic: b
        event_type: done
`)
	corr := sc.Flows[0].Correlation
	if corr.Type != CorrelationUUID {
		t.Errorf("type = %q, want %q", corr.Type, CorrelationUUID)
	}
	if corr.Field != "order_id" {
		t.Errorf("field = %q, want order_id", corr.Field)
	}
}

// TestDecodeSplitsIncidentsFromGroups pins the split of the heterogeneous `incidents:`
// list into standalone incidents and groups.
func TestDecodeSplitsIncidentsFromGroups(t *testing.T) {
	sc := decodeOK(t, `
name: s
topics:
  - name: orders
incidents:
  - type: stop_broker
    at_seconds: 10
    broker: kafka-1
  - group:
      repeat: 3
      interval_seconds: 60
      incidents:
        - type: rebalance_consumer
          at_seconds: 0
        - type: start_broker
          at_seconds: 30
          broker: kafka-2
  - type: rebalance_consumer
    every_seconds: 20
    initial_delay_seconds: 15
`)
	if len(sc.Incidents) != 2 {
		t.Fatalf("want 2 standalone incidents, got %d", len(sc.Incidents))
	}
	if len(sc.IncidentGroups) != 1 {
		t.Fatalf("want 1 incident group, got %d", len(sc.IncidentGroups))
	}

	stop, ok := sc.Incidents[0].(StopBrokerIncident)
	if !ok {
		t.Fatalf("first incident is %T, want StopBrokerIncident", sc.Incidents[0])
	}
	if stop.Broker != "kafka-1" {
		t.Errorf("broker = %q, want kafka-1", stop.Broker)
	}
	// Schedule keys live at the same flat level as the incident.
	if stop.Sched().AtSeconds == nil || *stop.Sched().AtSeconds != 10 {
		t.Errorf("at_seconds = %v, want 10", stop.Sched().AtSeconds)
	}
	if stop.Sched().EverySeconds != nil {
		t.Errorf("every_seconds = %v, want nil", stop.Sched().EverySeconds)
	}

	repeating := sc.Incidents[1].Sched()
	if repeating.EverySeconds == nil || *repeating.EverySeconds != 20 {
		t.Errorf("every_seconds = %v, want 20", repeating.EverySeconds)
	}
	if repeating.InitialDelaySeconds != 15 {
		t.Errorf("initial_delay_seconds = %d, want 15", repeating.InitialDelaySeconds)
	}
	if repeating.AtSeconds != nil {
		t.Errorf("at_seconds = %v, want nil", repeating.AtSeconds)
	}

	group := sc.IncidentGroups[0]
	if group.Repeat != 3 || group.IntervalSeconds != 60 {
		t.Errorf("group = %d x %ds, want 3 x 60s", group.Repeat, group.IntervalSeconds)
	}
	if len(group.Incidents) != 2 {
		t.Fatalf("want 2 members, got %d", len(group.Incidents))
	}
	if group.Incidents[0].Type() != IncidentRebalanceConsumer {
		t.Errorf("member 0 type = %q, want %q", group.Incidents[0].Type(), IncidentRebalanceConsumer)
	}
	if group.Incidents[1].Type() != IncidentStartBroker {
		t.Errorf("member 1 type = %q, want %q", group.Incidents[1].Type(), IncidentStartBroker)
	}
}

// TestDecodeIncidentTypes pins the dispatch over all six kinds and their parameters.
func TestDecodeIncidentTypes(t *testing.T) {
	sc := decodeOK(t, `
name: s
topics:
  - name: orders
incidents:
  - type: stop_broker
    at_seconds: 1
    broker: kafka-1
  - type: start_broker
    at_seconds: 2
    broker: kafka-1
  - type: pause_consumer
    at_seconds: 3
    duration_seconds: 15
  - type: rebalance_consumer
    at_seconds: 4
  - type: increase_consumer_delay
    at_seconds: 5
    delay_ms: 200
  - type: change_producer_rate
    at_seconds: 6
    rate: 500
`)
	wantTypes := []string{
		IncidentStopBroker, IncidentStartBroker, IncidentPauseConsumer,
		IncidentRebalanceConsumer, IncidentIncreaseConsumerDlay, IncidentChangeProducerRate,
	}
	if len(sc.Incidents) != len(wantTypes) {
		t.Fatalf("want %d incidents, got %d", len(wantTypes), len(sc.Incidents))
	}
	for i, want := range wantTypes {
		if got := sc.Incidents[i].Type(); got != want {
			t.Errorf("incident %d type = %q, want %q", i, got, want)
		}
	}
	if pause := sc.Incidents[2].(PauseConsumers); pause.DurationSeconds != 15 {
		t.Errorf("duration_seconds = %d, want 15", pause.DurationSeconds)
	}
	if delay := sc.Incidents[4].(IncreaseConsumerDelay); delay.DelayMS != 200 {
		t.Errorf("delay_ms = %d, want 200", delay.DelayMS)
	}
	if rate := sc.Incidents[5].(ChangeProducerRate); rate.Rate != 500 {
		t.Errorf("rate = %v, want 500", rate.Rate)
	}
}

func TestDecodeIncidentTargets(t *testing.T) {
	sc := decodeOK(t, `
name: s
topics:
  - name: orders
incidents:
  - type: pause_consumer
    at_seconds: 1
    duration_seconds: 5
    target:
      topic: orders
      group: orders-group-1
      count: 2
  - type: rebalance_consumer
    at_seconds: 2
    target:
      indices: [0, 2]
  - type: change_producer_rate
    at_seconds: 3
    rate: 100
    target:
      topic: orders
      percentage: 50
  - type: rebalance_consumer
    at_seconds: 4
`)
	pause := sc.Incidents[0].(PauseConsumers)
	if pause.Target.Topic != "orders" || pause.Target.Group != "orders-group-1" {
		t.Errorf("target = %+v, want topic orders / group orders-group-1", pause.Target)
	}
	if pause.Target.Count == nil || *pause.Target.Count != 2 {
		t.Errorf("count = %v, want 2", pause.Target.Count)
	}
	if pause.Target.Percentage != nil {
		t.Errorf("percentage = %v, want nil", pause.Target.Percentage)
	}

	rebalance := sc.Incidents[1].(RebalanceConsumer)
	if len(rebalance.Target.Indices) != 2 || rebalance.Target.Indices[0] != 0 || rebalance.Target.Indices[1] != 2 {
		t.Errorf("indices = %v, want [0 2]", rebalance.Target.Indices)
	}

	producer := sc.Incidents[2].(ChangeProducerRate)
	if producer.Target.Topic != "orders" {
		t.Errorf("producer target topic = %q, want orders", producer.Target.Topic)
	}
	if producer.Target.Percentage == nil || *producer.Target.Percentage != 50 {
		t.Errorf("percentage = %v, want 50", producer.Target.Percentage)
	}

	// An absent target matches everything.
	empty := sc.Incidents[3].(RebalanceConsumer).Target
	if empty.Topic != "" || empty.Group != "" || empty.Count != nil || empty.Percentage != nil || empty.Indices != nil {
		t.Errorf("absent target decoded as %+v, want the zero value", empty)
	}
}

// TestDecodeAcceptsGroupIDAlias pins that both `group` and `group_id` populate the
// target group, with `group` winning when both are present.
func TestDecodeAcceptsGroupIDAlias(t *testing.T) {
	tests := []struct {
		name string
		doc  string
		want string
	}{
		{
			name: "group",
			doc:  "      group: payments-group-1\n",
			want: "payments-group-1",
		},
		{
			name: "group_id",
			doc:  "      group_id: payments-group-1\n",
			want: "payments-group-1",
		},
		{
			name: "group wins when both are present",
			doc:  "      group: preferred\n      group_id: ignored\n",
			want: "preferred",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := decodeOK(t, "name: s\ntopics:\n  - name: orders\nincidents:\n"+
				"  - type: rebalance_consumer\n    at_seconds: 5\n    target:\n"+tt.doc)
			got := sc.Incidents[0].(RebalanceConsumer).Target.Group
			if got != tt.want {
				t.Errorf("target group = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestDecodeStrict covers the opt-in unknown-key mode. It must stay opt-in: the shipped
// protobuf-example scenario carries a `pattern` key that no model reads.
func TestDecodeStrict(t *testing.T) {
	doc := `
name: s
topics:
  - name: orders
    message_schema:
      fields:
        - name: tracking
          type: faker
          provider: bothify
          pattern: "??-#####"
`
	if _, diags := Decode([]byte(doc)); diags.HasErrors() {
		t.Fatalf("lenient decode must accept unknown keys:\n%s", diags.Errors())
	}

	_, diags := DecodeStrict([]byte(doc))
	if !diags.HasErrors() {
		t.Fatal("strict decode must reject an unknown key")
	}
	found := false
	for _, d := range diags.Errors() {
		if d.Path == "topics[0].message_schema.fields[0].pattern" && strings.Contains(d.Message, "pattern") {
			found = true
		}
	}
	if !found {
		t.Errorf("want an unknown-key error at the pattern key, got:\n%s", diags.Errors())
	}
}

func TestDecodeStrictAcceptsAKnownDocument(t *testing.T) {
	sc, diags := DecodeStrict([]byte("name: s\ntopics:\n  - name: orders\n    partitions: 3\n"))
	if diags.HasErrors() {
		t.Fatalf("strict decode rejected a fully known document:\n%s", diags.Errors())
	}
	if sc.Topics[0].Partitions != 3 {
		t.Errorf("partitions = %d, want 3", sc.Topics[0].Partitions)
	}
}

// TestDecodeHonoursMergeKeys checks that pass one and pass two agree about anchors: the
// validator expands `<<` itself, and yaml.Unmarshal does so natively.
func TestDecodeHonoursMergeKeys(t *testing.T) {
	sc := decodeOK(t, `
name: s
defaults: &defaults
  partitions: 3
  replication_factor: 2
  producer_rate: 50
topics:
  - <<: *defaults
    name: orders
  - <<: *defaults
    name: payments
    partitions: 6
`)
	if len(sc.Topics) != 2 {
		t.Fatalf("want 2 topics, got %d", len(sc.Topics))
	}
	if sc.Topics[0].Partitions != 3 || sc.Topics[0].ReplicationFactor != 2 || sc.Topics[0].ProducerRate != 50 {
		t.Errorf("merged topic = %+v, want the anchored values", sc.Topics[0])
	}
	// An explicit key must beat the merged one.
	if sc.Topics[1].Partitions != 6 {
		t.Errorf("partitions = %d, want the explicit 6", sc.Topics[1].Partitions)
	}
}

func TestDecodeReturnsNilScenarioOnError(t *testing.T) {
	sc, diags := Decode([]byte("topics: []\n"))
	if !diags.HasErrors() {
		t.Fatal("want errors")
	}
	if sc != nil {
		t.Error("want a nil scenario when validation failed")
	}
}

func TestDecodeReturnsScenarioWithWarnings(t *testing.T) {
	sc, diags := Decode([]byte("name: s\ntopics:\n  - name: orders\n    partitions: 500\n"))
	if diags.HasErrors() {
		t.Fatalf("unexpected errors:\n%s", diags.Errors())
	}
	if len(diags.Warnings()) == 0 {
		t.Fatal("want a warning")
	}
	if sc == nil {
		t.Fatal("a warning must not suppress the scenario")
	}
}

func TestDecodeEmptyDocument(t *testing.T) {
	sc, diags := Decode(nil)
	if sc != nil {
		t.Error("want a nil scenario for an empty document")
	}
	if !diags.HasErrors() {
		t.Fatal("want an error for an empty document")
	}
	if got := diags.Errors()[0].Path; got != "root" {
		t.Errorf("path = %q, want root", got)
	}
}

func TestDecodeSchemaRegistry(t *testing.T) {
	sc := decodeOK(t, `
name: s
schema_registry:
  url: http://localhost:8081
topics:
  - name: orders
    schema_provider: registry
    subject_name: orders-value
`)
	if sc.SchemaRegistry == nil {
		t.Fatal("schema_registry was not decoded")
	}
	if sc.SchemaRegistry.URL != "http://localhost:8081" {
		t.Errorf("url = %q", sc.SchemaRegistry.URL)
	}
	if sc.Topics[0].SubjectName != "orders-value" {
		t.Errorf("subject_name = %q, want orders-value", sc.Topics[0].SubjectName)
	}
}

func TestDiagnosticsHelpers(t *testing.T) {
	diags := Diagnostics{
		{Path: "a", Message: "first", Severity: SeverityError, Line: 3},
		{Path: "b", Message: "second", Severity: SeverityWarning, Line: 7},
		{Path: "c", Message: "third", Severity: SeverityError},
	}
	if !diags.HasErrors() {
		t.Error("HasErrors = false, want true")
	}
	if got := len(diags.Errors()); got != 2 {
		t.Errorf("len(Errors()) = %d, want 2", got)
	}
	if got := len(diags.Warnings()); got != 1 {
		t.Errorf("len(Warnings()) = %d, want 1", got)
	}
	// Errors are rendered before warnings regardless of discovery order, and a
	// line-less finding omits the line prefix.
	want := "error: line 3: a: first\nerror: c: third\nwarning: line 7: b: second"
	if got := diags.String(); got != want {
		t.Errorf("String() =\n%s\nwant\n%s", got, want)
	}
	if err := diags.Err(); err == nil || !strings.Contains(err.Error(), "a: first") {
		t.Errorf("Err() = %v, want it to carry the errors", err)
	}

	var none Diagnostics
	if none.HasErrors() || none.String() != "" || none.Err() != nil {
		t.Error("an empty Diagnostics must be quiet")
	}
	if warnOnly := (Diagnostics{{Path: "x", Message: "w", Severity: SeverityWarning}}); warnOnly.Err() != nil {
		t.Error("warnings alone must not produce an error")
	}
}
