// Package scenario holds the khaos scenario domain model together with the YAML
// decoding and validation that produces it.
//
// Every default value used when a key is omitted from a scenario file is collected in
// one place below, so there is exactly one answer per field.
package scenario

// Defaults, collected in one place so there is exactly one answer per field: a value is
// defined once here or not at all.
const (
	// Topic
	defaultPartitions        = 12
	defaultReplicationFactor = 3
	defaultNumProducers      = 2
	defaultConsumerGroups    = 1
	defaultConsumersPerGroup = 2
	defaultProducerRate      = 1000.0
	defaultConsumerDelayMS   = 0
	defaultSchemaProvider    = "inline"

	// MessageSchema
	defaultKeyDistribution = "uniform"
	defaultKeyCardinality  = 50
	defaultMinSizeBytes    = 200
	defaultMaxSizeBytes    = 500
	defaultDataFormat      = "json"

	// ProducerConf
	defaultBatchSize       = 16384
	defaultLingerMS        = 5
	defaultAcks            = "1"
	defaultCompressionType = "lz4"
	defaultDuplicateRate   = 0.0

	// ConsumerConf
	defaultFailureRate       = 0.0
	defaultCommitFailureRate = 0.0
	defaultOnFailure         = "skip"
	defaultMaxRetries        = 3

	// Field
	defaultMinItems = 1
	defaultMaxItems = 5

	// StepConsumer
	defaultStepGroups   = 1
	defaultStepPerGroup = 1
	defaultStepDelayMS  = 0

	// Flow
	defaultFlowRate = 10.0

	// Correlation
	defaultCorrelationType = "uuid"

	// IncidentGroup
	defaultGroupRepeat   = 1
	defaultGroupInterval = 60
)

// Scenario is one parsed scenario file.
//
// `incidents:` in YAML is a single heterogeneous list, split into standalone incidents
// and groups depending on whether an entry carries a `group` key; that split happens
// during decode, not here.
type Scenario struct {
	Name           string          `yaml:"name"`
	Description    string          `yaml:"description"`
	Topics         []Topic         `yaml:"topics"`
	Flows          []Flow          `yaml:"flows"`
	SchemaRegistry *SchemaRegistry `yaml:"schema_registry"`

	// Populated by decode from the raw `incidents:` list.
	Incidents      []Incident
	IncidentGroups []IncidentGroup
}

// SchemaRegistry configures the schema registry a scenario fetches schemas from.
//
// Only `url` is modelled in YAML; credentials for a secured registry (Confluent Cloud,
// Aiven) come from the --schema-registry-* flags, never from the scenario file.
type SchemaRegistry struct {
	URL string `yaml:"url"`
}

// Topic configures one Kafka topic and its producers and consumers.
type Topic struct {
	Name              string        `yaml:"name"`
	Partitions        int           `yaml:"partitions"`
	ReplicationFactor int           `yaml:"replication_factor"`
	NumProducers      int           `yaml:"num_producers"`
	NumConsumerGroups int           `yaml:"num_consumer_groups"`
	ConsumersPerGroup int           `yaml:"consumers_per_group"`
	ProducerRate      float64       `yaml:"producer_rate"`
	ConsumerDelayMS   int           `yaml:"consumer_delay_ms"`
	MessageSchema     MessageSchema `yaml:"message_schema"`
	ProducerConfig    ProducerConf  `yaml:"producer_config"`
	ConsumerConfig    ConsumerConf  `yaml:"consumer_config"`

	// SchemaProvider is "inline" or "registry". When "registry", SubjectName is
	// required and MessageSchema.Fields must be empty -- the schema is fetched from
	// the registry and converted into field definitions at runtime.
	SchemaProvider string `yaml:"schema_provider"`
	SubjectName    string `yaml:"subject_name"`
}

func defaultTopic() Topic {
	return Topic{
		Partitions:        defaultPartitions,
		ReplicationFactor: defaultReplicationFactor,
		NumProducers:      defaultNumProducers,
		NumConsumerGroups: defaultConsumerGroups,
		ConsumersPerGroup: defaultConsumersPerGroup,
		ProducerRate:      defaultProducerRate,
		ConsumerDelayMS:   defaultConsumerDelayMS,
		SchemaProvider:    defaultSchemaProvider,
		MessageSchema:     defaultMessageSchema(),
		ProducerConfig:    defaultProducerConf(),
		ConsumerConfig:    defaultConsumerConf(),
	}
}

// MessageSchema configures the shape and size of generated messages. When Fields is
// empty, the payload generator emits a synthetic JSON document ({id, timestamp,
// sequence} plus padding) sized between MinSizeBytes and MaxSizeBytes; when Fields is
// set, the document is built from the field definitions instead and the size bounds
// are ignored.
type MessageSchema struct {
	KeyDistribution string  `yaml:"key_distribution"`
	KeyCardinality  int     `yaml:"key_cardinality"`
	MinSizeBytes    int     `yaml:"min_size_bytes"`
	MaxSizeBytes    int     `yaml:"max_size_bytes"`
	DataFormat      string  `yaml:"data_format"`
	Fields          []Field `yaml:"fields"`
}

func defaultMessageSchema() MessageSchema {
	return MessageSchema{
		KeyDistribution: defaultKeyDistribution,
		KeyCardinality:  defaultKeyCardinality,
		MinSizeBytes:    defaultMinSizeBytes,
		MaxSizeBytes:    defaultMaxSizeBytes,
		DataFormat:      defaultDataFormat,
	}
}

// ProducerConf configures a topic's producers.
//
// BatchSize has no exact kgo equivalent: it names a byte budget per partition batch,
// whereas kgo's ProducerBatchMaxBytes is a whole-request cap. The YAML key is kept for
// backward compatibility even though the semantics drift.
type ProducerConf struct {
	BatchSize       int     `yaml:"batch_size"`
	LingerMS        int     `yaml:"linger_ms"`
	Acks            string  `yaml:"acks"`
	CompressionType string  `yaml:"compression_type"`
	DuplicateRate   float64 `yaml:"duplicate_rate"`
}

func defaultProducerConf() ProducerConf {
	return ProducerConf{
		BatchSize:       defaultBatchSize,
		LingerMS:        defaultLingerMS,
		Acks:            defaultAcks,
		CompressionType: defaultCompressionType,
		DuplicateRate:   defaultDuplicateRate,
	}
}

// ConsumerConf configures a topic's consumers.
//
// Any non-zero FailureRate or CommitFailureRate switches the consumer to manual
// commit, which is load-bearing for the failure-simulation scenarios.
type ConsumerConf struct {
	FailureRate       float64 `yaml:"failure_rate"`
	CommitFailureRate float64 `yaml:"commit_failure_rate"`
	OnFailure         string  `yaml:"on_failure"` // skip | dlq | retry
	MaxRetries        int     `yaml:"max_retries"`
}

func defaultConsumerConf() ConsumerConf {
	return ConsumerConf{
		FailureRate:       defaultFailureRate,
		CommitFailureRate: defaultCommitFailureRate,
		OnFailure:         defaultOnFailure,
		MaxRetries:        defaultMaxRetries,
	}
}

// FailureSimulationEnabled reports whether the consumer must use manual commit instead
// of auto-commit.
func (c ConsumerConf) FailureSimulationEnabled() bool {
	return c.FailureRate > 0 || c.CommitFailureRate > 0
}

// Field types accepted in `fields:`.
const (
	FieldString    = "string"
	FieldInt       = "int"
	FieldFloat     = "float"
	FieldBoolean   = "boolean"
	FieldUUID      = "uuid"
	FieldTimestamp = "timestamp"
	FieldEnum      = "enum"
	FieldObject    = "object"
	FieldArray     = "array"
	FieldFaker     = "faker"
)

// ValidFieldTypes is the closed set of accepted `type:` values.
var ValidFieldTypes = map[string]bool{
	FieldString: true, FieldInt: true, FieldFloat: true, FieldBoolean: true,
	FieldUUID: true, FieldTimestamp: true, FieldEnum: true, FieldObject: true,
	FieldArray: true, FieldFaker: true,
}

// Field is one entry in a `fields:` list.
//
// The struct is flat and carries every parameter of every type, so "a string with
// min_items" parses happily; per-type rules about which parameters are meaningful are
// enforced in validation instead (see validate.go).
type Field struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`

	// Numeric constraints (int, float). Pointers because "absent" and "zero" differ:
	// `min: 0` is a real constraint and must not be confused with an omitted key.
	Min *float64 `yaml:"min"`
	Max *float64 `yaml:"max"`

	// String constraints.
	MinLength *int `yaml:"min_length"`
	MaxLength *int `yaml:"max_length"`

	// Cardinality bounds the number of distinct values produced. The first N calls
	// yield N distinct values, and every call after that cycles them in
	// first-generated order (see internal/generate). That round-robin tail is
	// contractual.
	Cardinality *int `yaml:"cardinality"`

	// Enum values.
	Values []string `yaml:"values"`

	// Nested object fields.
	Fields []Field `yaml:"fields"`

	// Array item schema and length bounds.
	Items    *Field `yaml:"items"`
	MinItems int    `yaml:"min_items"`
	MaxItems int    `yaml:"max_items"`

	// Faker provider. Provider names map through an explicit alias table (see
	// internal/generate); Locale has no gofakeit equivalent and is currently unused.
	Provider string `yaml:"provider"`
	Locale   string `yaml:"locale"`
}

func defaultField() Field {
	return Field{
		MinItems: defaultMinItems,
		MaxItems: defaultMaxItems,
	}
}

// Flow is an ordered sequence of steps emitted as one correlated "instance" at Rate
// instances per second. The correlation id is used as the Kafka message key for every
// step, so all events of one instance land on one partition per topic.
type Flow struct {
	Name        string      `yaml:"name"`
	Rate        float64     `yaml:"rate"`
	Steps       []FlowStep  `yaml:"steps"`
	Correlation Correlation `yaml:"correlation"`
}

func defaultFlow() Flow {
	return Flow{
		Rate:        defaultFlowRate,
		Correlation: Correlation{Type: defaultCorrelationType},
	}
}

// Topics returns the distinct topics touched by this flow, in first-appearance order.
func (f Flow) Topics() []string {
	seen := make(map[string]bool, len(f.Steps))
	out := make([]string, 0, len(f.Steps))
	for _, s := range f.Steps {
		if !seen[s.Topic] {
			seen[s.Topic] = true
			out = append(out, s.Topic)
		}
	}
	return out
}

// FlowStep is one step of a flow.
type FlowStep struct {
	Topic     string        `yaml:"topic"`
	EventType string        `yaml:"event_type"`
	DelayMS   int           `yaml:"delay_ms"` // delay after the previous step
	Fields    []Field       `yaml:"fields"`
	Consumers *StepConsumer `yaml:"consumers"`
}

// StepConsumer configures the consumers spawned for one flow step.
type StepConsumer struct {
	Groups   int `yaml:"groups"`
	PerGroup int `yaml:"per_group"`
	DelayMS  int `yaml:"delay_ms"`
}

func defaultStepConsumer() StepConsumer {
	return StepConsumer{
		Groups:   defaultStepGroups,
		PerGroup: defaultStepPerGroup,
		DelayMS:  defaultStepDelayMS,
	}
}

// Correlation types.
const (
	CorrelationUUID     = "uuid"
	CorrelationFieldRef = "field_ref"
)

// Correlation configures how a flow instance's correlation id is derived.
//
// For CorrelationFieldRef, Field names a field of the FIRST step whose generated value
// becomes the correlation id; the first step's message is then re-serialised with
// correlation_id overwritten.
type Correlation struct {
	Type  string `yaml:"type"`
	Field string `yaml:"field"`
}
