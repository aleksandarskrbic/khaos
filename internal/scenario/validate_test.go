package scenario

import (
	"slices"
	"strings"
	"testing"
)

// The tests in this file pin the validation contract. Messages are asserted verbatim
// because they are the user-facing surface of `khaos validate`, and several cases exist
// only to lock a quirk in place so that a later "cleanup" has to be deliberate.

// validationCase drives the table runner below.
//
// errs and warns list "path: message" strings that MUST appear. clean additionally
// asserts that nothing was reported at error severity.
type validationCase struct {
	name  string
	doc   string
	errs  []string
	warns []string
	clean bool
}

func runValidationCases(t *testing.T, cases []validationCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, diags := Decode([]byte(tc.doc))
			gotErrs := renderDiagnostics(diags, SeverityError)
			gotWarns := renderDiagnostics(diags, SeverityWarning)

			for _, want := range tc.errs {
				if !slices.Contains(gotErrs, want) {
					t.Errorf("missing error %q\ngot errors:\n  %s", want, strings.Join(gotErrs, "\n  "))
				}
			}
			for _, want := range tc.warns {
				if !slices.Contains(gotWarns, want) {
					t.Errorf("missing warning %q\ngot warnings:\n  %s", want, strings.Join(gotWarns, "\n  "))
				}
			}
			if tc.clean && len(gotErrs) > 0 {
				t.Errorf("want no errors, got:\n  %s", strings.Join(gotErrs, "\n  "))
			}
			// Every finding produced from a node must carry a source line.
			for _, d := range diags {
				if d.Line <= 0 {
					t.Errorf("diagnostic without a line number: %s", d)
				}
			}
		})
	}
}

func renderDiagnostics(diags Diagnostics, sev Severity) []string {
	var out []string
	for _, d := range diags {
		if d.Severity == sev {
			out = append(out, d.Path+": "+d.Message)
		}
	}
	return out
}

func TestValidateRoot(t *testing.T) {
	runValidationCases(t, []validationCase{
		{
			name:  "minimal scenario is valid",
			doc:   "name: s\ntopics:\n  - name: orders\n",
			clean: true,
		},
		{
			name: "missing name",
			doc:  "topics:\n  - name: orders\n",
			errs: []string{"name: Missing required field 'name'"},
		},
		{
			name: "name must be a string",
			doc:  "name: 12\ntopics:\n  - name: orders\n",
			errs: []string{"name: Field 'name' must be a string"},
		},
		{
			// An empty name cannot be addressed by any CLI command.
			name: "empty name",
			doc:  "name: \"  \"\ntopics:\n  - name: orders\n",
			errs: []string{"name: Field 'name' must not be empty"},
		},
		{
			name: "neither topics nor flows",
			doc:  "name: s\ndescription: nothing here\n",
			errs: []string{"topics: Scenario must have at least 'topics' or 'flows'"},
		},
		{
			// A null `topics:` trips both rules.
			name: "topics present but not a list",
			doc:  "name: s\ntopics: nope\n",
			errs: []string{
				"topics: Scenario must have at least 'topics' or 'flows'",
				"topics: Field 'topics' must be a list",
			},
		},
		{
			name: "empty topic list with flows is fine",
			doc: `
name: s
topics: []
flows:
  - name: f
    steps:
      - topic: a
        event_type: created
      - topic: b
        event_type: done
`,
			clean: true,
		},
		{
			name: "root is not a mapping",
			doc:  "- one\n- two\n",
			errs: []string{"root: Scenario must be a YAML object/dict"},
		},
		{
			name: "schema registry needs an http url",
			doc: `
name: s
schema_registry:
  url: localhost:8081
topics:
  - name: orders
`,
			errs: []string{"schema_registry.url: Field 'url' must be a valid HTTP(S) URL"},
		},
		{
			name: "schema registry url is required",
			doc: `
name: s
schema_registry: {}
topics:
  - name: orders
`,
			errs: []string{"schema_registry.url: Missing required field 'url'"},
		},
	})
}

func TestValidateTopic(t *testing.T) {
	runValidationCases(t, []validationCase{
		{
			name: "missing topic name",
			doc:  "name: s\ntopics:\n  - partitions: 3\n",
			errs: []string{"topics[0].name: Missing required field 'name'"},
		},
		{
			name: "non positive counts",
			doc: `
name: s
topics:
  - name: orders
    partitions: 0
    replication_factor: 0
    num_consumer_groups: 0
    consumers_per_group: 0
    producer_rate: 0
    consumer_delay_ms: -1
    num_producers: -1
`,
			errs: []string{
				"topics[0].partitions: Field 'partitions' must be a positive integer",
				"topics[0].replication_factor: Field 'replication_factor' must be a positive integer",
				"topics[0].num_consumer_groups: Field 'num_consumer_groups' must be a positive integer",
				"topics[0].consumers_per_group: Field 'consumers_per_group' must be a positive integer",
				"topics[0].producer_rate: Field 'producer_rate' must be a positive number",
				"topics[0].consumer_delay_ms: Field 'consumer_delay_ms' must be a non-negative integer",
				"topics[0].num_producers: Field 'num_producers' must be a non-negative integer",
			},
		},
		{
			// num_producers is the one count allowed to be zero.
			name:  "zero producers is allowed",
			doc:   "name: s\ntopics:\n  - name: orders\n    num_producers: 0\n",
			clean: true,
		},
		{
			// Cluster assumption: three brokers, so rf > 3 is rejected outright.
			name: "replication factor above cluster size",
			doc:  "name: s\ntopics:\n  - name: orders\n    replication_factor: 4\n",
			errs: []string{"topics[0].replication_factor: Replication factor cannot exceed 3 (cluster has 3 brokers)"},
		},
		{
			name:  "high partition count is a warning only",
			doc:   "name: s\ntopics:\n  - name: orders\n    partitions: 200\n",
			warns: []string{"topics[0].partitions: High partition count (200) may impact performance"},
			clean: true,
		},
		{
			name: "unknown schema provider",
			doc:  "name: s\ntopics:\n  - name: orders\n    schema_provider: guess\n",
			errs: []string{"topics[0].schema_provider: Invalid schema_provider 'guess'. Valid values: inline, registry"},
		},
		{
			name: "registry provider requires a subject",
			doc:  "name: s\ntopics:\n  - name: orders\n    schema_provider: registry\n",
			errs: []string{"topics[0].subject_name: Field 'subject_name' is required when schema_provider is 'registry'"},
		},
		{
			name: "registry provider forbids inline fields",
			doc: `
name: s
topics:
  - name: orders
    schema_provider: registry
    subject_name: orders-value
    message_schema:
      fields:
        - name: id
          type: uuid
`,
			errs: []string{
				"topics[0]: Cannot define 'message_schema.fields' when schema_provider is 'registry' " +
					"(schema will be fetched from Schema Registry)",
			},
		},
		{
			name: "avro without fields",
			doc: `
name: s
topics:
  - name: orders
    message_schema:
      data_format: avro
`,
			errs: []string{
				"topics[0].message_schema.fields: data_format 'avro' requires 'fields' to be defined " +
					"(or use schema_provider: registry to fetch from Schema Registry)",
			},
		},
		{
			name: "unknown data format",
			doc:  "name: s\ntopics:\n  - name: orders\n    message_schema:\n      data_format: xml\n",
			errs: []string{"topics[0].message_schema.data_format: Invalid data_format 'xml'. Valid values: avro, json, protobuf"},
		},
		{
			name: "binary format without a registry warns",
			doc: `
name: s
topics:
  - name: orders
    message_schema:
      data_format: avro
      fields:
        - name: id
          type: uuid
`,
			warns: []string{"schema_registry: Using Avro without Schema Registry (schemaless mode)"},
			clean: true,
		},
		{
			name: "key distribution and cardinality",
			doc: `
name: s
topics:
  - name: orders
    message_schema:
      key_distribution: random
      key_cardinality: 0
`,
			errs: []string{
				"topics[0].message_schema.key_distribution: Invalid key_distribution 'random'. " +
					"Valid values: round_robin, single_key, uniform, zipfian",
				"topics[0].message_schema.key_cardinality: Field 'key_cardinality' must be a positive integer",
			},
		},
		{
			name: "size bounds inverted",
			doc: `
name: s
topics:
  - name: orders
    message_schema:
      min_size_bytes: 900
      max_size_bytes: 100
`,
			errs: []string{"topics[0].message_schema: min_size_bytes cannot be greater than max_size_bytes"},
		},
		{
			// The pairing check uses the model default for the omitted bound, so a lone
			// min above 500 is already an error.
			name: "min size above the default max",
			doc:  "name: s\ntopics:\n  - name: orders\n    message_schema:\n      min_size_bytes: 900\n",
			errs: []string{"topics[0].message_schema: min_size_bytes cannot be greater than max_size_bytes"},
		},
	})
}

func TestValidateProducerAndConsumerConfig(t *testing.T) {
	runValidationCases(t, []validationCase{
		{
			name: "producer config values",
			doc: `
name: s
topics:
  - name: orders
    producer_config:
      acks: "2"
      compression_type: brotli
      duplicate_rate: 1.5
      batch_size: -1
      linger_ms: -1
`,
			errs: []string{
				"topics[0].producer_config.acks: Invalid acks '2'. Valid values: -1, 0, 1, all",
				"topics[0].producer_config.compression_type: Invalid compression_type 'brotli'. " +
					"Valid values: gzip, lz4, none, snappy, zstd",
				"topics[0].producer_config.duplicate_rate: Field 'duplicate_rate' must be a number between 0.0 and 1.0",
				"topics[0].producer_config.batch_size: Field 'batch_size' must be a non-negative integer",
				"topics[0].producer_config.linger_ms: Field 'linger_ms' must be a non-negative integer",
			},
		},
		{
			// acks is compared as a string, so an unquoted int is accepted.
			name:  "unquoted acks is accepted",
			doc:   "name: s\ntopics:\n  - name: orders\n    producer_config:\n      acks: 1\n",
			clean: true,
		},
		{
			name:  "high duplicate rate warns",
			doc:   "name: s\ntopics:\n  - name: orders\n    producer_config:\n      duplicate_rate: 0.9\n",
			warns: []string{"topics[0].producer_config.duplicate_rate: High duplicate rate (90%) will produce many duplicate messages"},
			clean: true,
		},
		{
			name: "consumer config values",
			doc: `
name: s
topics:
  - name: orders
    consumer_config:
      failure_rate: 2.0
      on_failure: ignore
      max_retries: -1
`,
			errs: []string{
				"topics[0].consumer_config.failure_rate: Field 'failure_rate' must be a number between 0.0 and 1.0",
				"topics[0].consumer_config.on_failure: Invalid on_failure 'ignore'. Valid values: dlq, retry, skip",
				"topics[0].consumer_config.max_retries: Field 'max_retries' must be a non-negative integer",
			},
		},
		{
			name: "high failure rates and dlq warn",
			doc: `
name: s
topics:
  - name: orders
    consumer_config:
      failure_rate: 0.6
      commit_failure_rate: 0.7
      on_failure: dlq
`,
			warns: []string{
				"topics[0].consumer_config.failure_rate: High failure rate (60%) may cause significant message loss",
				"topics[0].consumer_config.commit_failure_rate: High commit failure rate (70%) may cause significant reprocessing",
				"topics[0].consumer_config.on_failure: DLQ mode enabled - ensure DLQ topics exist or are auto-created",
			},
			clean: true,
		},
	})
}

func TestValidateFields(t *testing.T) {
	const prefix = "name: s\ntopics:\n  - name: orders\n    message_schema:\n      fields:\n"
	runValidationCases(t, []validationCase{
		{
			name: "missing name and type",
			doc:  prefix + "        - cardinality: 5\n",
			errs: []string{
				"topics[0].message_schema.fields[0].name: Missing required field 'name'",
				"topics[0].message_schema.fields[0].type: Missing required field 'type'",
			},
		},
		{
			name: "unknown type",
			doc:  prefix + "        - name: f\n          type: blob\n",
			errs: []string{
				"topics[0].message_schema.fields[0].type: Unknown field type 'blob'. " +
					"Valid types: array, boolean, enum, faker, float, int, object, string, timestamp, uuid",
			},
		},
		{
			name: "enum requires values",
			doc:  prefix + "        - name: f\n          type: enum\n",
			errs: []string{"topics[0].message_schema.fields[0].values: Enum field requires 'values' list"},
		},
		{
			name: "enum values must be non-empty strings",
			doc:  prefix + "        - name: f\n          type: enum\n          values: [ok, 7]\n",
			errs: []string{"topics[0].message_schema.fields[0].values[1]: Enum values must be strings"},
		},
		{
			name: "empty enum",
			doc:  prefix + "        - name: f\n          type: enum\n          values: []\n",
			errs: []string{"topics[0].message_schema.fields[0].values: Enum must have at least one value"},
		},
		{
			name: "object requires fields",
			doc:  prefix + "        - name: f\n          type: object\n",
			errs: []string{"topics[0].message_schema.fields[0].fields: Object field requires 'fields' list"},
		},
		{
			name: "empty object",
			doc:  prefix + "        - name: f\n          type: object\n          fields: []\n",
			errs: []string{"topics[0].message_schema.fields[0].fields: Object must have at least one field"},
		},
		{
			name: "array requires items",
			doc:  prefix + "        - name: f\n          type: array\n",
			errs: []string{"topics[0].message_schema.fields[0].items: Array field requires 'items' schema"},
		},
		{
			name: "faker requires provider",
			doc:  prefix + "        - name: f\n          type: faker\n",
			errs: []string{"topics[0].message_schema.fields[0].provider: Faker field requires 'provider'"},
		},
		{
			name: "string bounds inverted",
			doc:  prefix + "        - name: f\n          type: string\n          min_length: 20\n          max_length: 5\n",
			errs: []string{"topics[0].message_schema.fields[0]: min_length cannot be greater than max_length"},
		},
		{
			name: "cardinality must be positive",
			doc:  prefix + "        - name: f\n          type: string\n          cardinality: 0\n",
			errs: []string{"topics[0].message_schema.fields[0].cardinality: Field 'cardinality' must be a positive integer"},
		},
		{
			name: "numeric range inverted",
			doc:  prefix + "        - name: f\n          type: int\n          min: 10\n          max: 1\n",
			errs: []string{"topics[0].message_schema.fields[0]: min cannot be greater than max"},
		},
		{
			name: "array item bounds inverted",
			doc: prefix + "        - name: f\n          type: array\n          min_items: 9\n          max_items: 2\n" +
				"          items:\n            type: uuid\n",
			errs: []string{"topics[0].message_schema.fields[0]: min_items cannot be greater than max_items"},
		},
		{
			// Recursion: nested object fields are full fields and need names.
			name: "nested object field is validated",
			doc: prefix + "        - name: outer\n          type: object\n          fields:\n" +
				"            - type: blob\n",
			errs: []string{
				"topics[0].message_schema.fields[0].fields[0].name: Missing required field 'name'",
				"topics[0].message_schema.fields[0].fields[0].type: Unknown field type 'blob'. " +
					"Valid types: array, boolean, enum, faker, float, int, object, string, timestamp, uuid",
			},
		},
		{
			// Recursion through an array item into an object and back out again.
			name: "array item object field is validated",
			doc: prefix + "        - name: items\n          type: array\n          items:\n" +
				"            type: object\n            fields:\n              - name: q\n                type: int\n" +
				"                min: 9\n                max: 1\n",
			errs: []string{"topics[0].message_schema.fields[0].items.fields[0]: min cannot be greater than max"},
		},
		{
			// An array item needs no name -- _validate_array_item never checks for one.
			name:  "array item needs no name",
			doc:   prefix + "        - name: tags\n          type: array\n          items:\n            type: string\n",
			clean: true,
		},
		{
			// QUIRK: the array-item dispatch has no faker branch, so a provider-less
			// faker item is silently accepted while the same definition as a named
			// field is rejected.
			name:  "faker array item escapes the provider check",
			doc:   prefix + "        - name: tags\n          type: array\n          items:\n            type: faker\n",
			clean: true,
		},
	})
}

func TestValidateIncidents(t *testing.T) {
	const prefix = "name: s\ntopics:\n  - name: orders\nincidents:\n"
	runValidationCases(t, []validationCase{
		{
			name: "unknown incident type",
			doc:  prefix + "  - type: meltdown\n    at_seconds: 5\n",
			errs: []string{
				"incidents[0].type: Unknown incident type 'meltdown'. Valid types: change_producer_rate, " +
					"increase_consumer_delay, pause_consumer, rebalance_consumer, start_broker, stop_broker",
			},
		},
		{
			name: "incident without a schedule",
			doc:  prefix + "  - type: rebalance_consumer\n",
			errs: []string{"incidents[0]: Incident must have either 'at_seconds' or 'every_seconds'"},
		},
		{
			name: "every_seconds must be positive",
			doc:  prefix + "  - type: rebalance_consumer\n    every_seconds: 0\n",
			errs: []string{"incidents[0].every_seconds: Field 'every_seconds' must be a positive integer"},
		},
		{
			name: "stop_broker requires a broker",
			doc:  prefix + "  - type: stop_broker\n    at_seconds: 5\n",
			errs: []string{"incidents[0].broker: Incident type 'stop_broker' requires 'broker' field"},
		},
		{
			// Cluster assumption again: only the three compose brokers are nameable.
			name: "unknown broker",
			doc:  prefix + "  - type: stop_broker\n    at_seconds: 5\n    broker: kafka-9\n",
			errs: []string{"incidents[0].broker: Invalid broker. Valid: kafka-1, kafka-2, kafka-3"},
		},
		{
			name: "pause_consumer requires a duration",
			doc:  prefix + "  - type: pause_consumer\n    at_seconds: 5\n",
			errs: []string{"incidents[0].duration_seconds: Incident type 'pause_consumer' requires 'duration_seconds' field"},
		},
		{
			name: "pause_consumer duration must be positive",
			doc:  prefix + "  - type: pause_consumer\n    at_seconds: 5\n    duration_seconds: 0\n",
			errs: []string{"incidents[0].duration_seconds: Field 'duration_seconds' must be a positive integer"},
		},
		{
			name: "increase_consumer_delay requires delay_ms",
			doc:  prefix + "  - type: increase_consumer_delay\n    at_seconds: 5\n",
			errs: []string{"incidents[0].delay_ms: Incident type 'increase_consumer_delay' requires 'delay_ms' field"},
		},
		{
			name: "negative delay_ms",
			doc:  prefix + "  - type: increase_consumer_delay\n    at_seconds: 5\n    delay_ms: -1\n",
			errs: []string{"incidents[0].delay_ms: Field 'delay_ms' must be a non-negative integer"},
		},
		{
			name: "change_producer_rate requires a rate",
			doc:  prefix + "  - type: change_producer_rate\n    at_seconds: 5\n",
			errs: []string{"incidents[0].rate: Incident type 'change_producer_rate' requires 'rate' field"},
		},
		{
			name: "negative rate",
			doc:  prefix + "  - type: change_producer_rate\n    at_seconds: 5\n    rate: -3\n",
			errs: []string{"incidents[0].rate: Field 'rate' must be a non-negative number"},
		},
	})
}

func TestValidateIncidentTarget(t *testing.T) {
	const prefix = "name: s\ntopics:\n  - name: orders\nincidents:\n  - type: rebalance_consumer\n    at_seconds: 5\n    target:\n"
	runValidationCases(t, []validationCase{
		{
			name:  "topic and group",
			doc:   prefix + "      topic: orders\n      group: orders-group-1\n",
			clean: true,
		},
		{
			// group_id is accepted as an alias, so it must validate too.
			name:  "group_id alias",
			doc:   prefix + "      group_id: orders-group-1\n",
			clean: true,
		},
		{
			name: "group_id must be a string",
			doc:  prefix + "      group_id: [a]\n",
			errs: []string{"incidents[0].target.group_id: Field 'group_id' must be a string"},
		},
		{
			name: "percentage out of range",
			doc:  prefix + "      percentage: 0\n",
			errs: []string{"incidents[0].target.percentage: Field 'percentage' must be an integer between 1 and 100"},
		},
		{
			name: "percentage above 100",
			doc:  prefix + "      percentage: 101\n",
			errs: []string{"incidents[0].target.percentage: Field 'percentage' must be an integer between 1 and 100"},
		},
		{
			name: "count must be positive",
			doc:  prefix + "      count: 0\n",
			errs: []string{"incidents[0].target.count: Field 'count' must be a positive integer"},
		},
		{
			name: "indices must be non-negative integers",
			doc:  prefix + "      indices: [0, -1]\n",
			errs: []string{"incidents[0].target.indices: Field 'indices' must be a list of non-negative integers"},
		},
		{
			name: "count and percentage are mutually exclusive",
			doc:  prefix + "      count: 1\n      percentage: 50\n",
			errs: []string{"incidents[0].target: Only one of percentage, count, indices can be set, got: percentage, count"},
		},
		{
			name: "indices is exclusive with count too",
			doc:  prefix + "      count: 1\n      indices: [0]\n",
			errs: []string{"incidents[0].target: Only one of percentage, count, indices can be set, got: count, indices"},
		},
	})
}

func TestValidateIncidentGroups(t *testing.T) {
	runValidationCases(t, []validationCase{
		{
			name: "valid group",
			doc: `
name: s
topics:
  - name: orders
incidents:
  - group:
      repeat: 3
      interval_seconds: 60
      incidents:
        - type: stop_broker
          at_seconds: 0
          broker: kafka-1
        - type: start_broker
          at_seconds: 30
          broker: kafka-1
`,
			clean: true,
		},
		{
			name: "group requires repeat, interval and incidents",
			doc:  "name: s\ntopics:\n  - name: orders\nincidents:\n  - group: {}\n",
			errs: []string{
				"incidents[0].group.repeat: Missing required field 'repeat'",
				"incidents[0].group.interval_seconds: Missing required field 'interval_seconds'",
				"incidents[0].group.incidents: Missing required field 'incidents'",
			},
		},
		{
			name: "group counters must be positive",
			doc: `
name: s
topics:
  - name: orders
incidents:
  - group:
      repeat: 0
      interval_seconds: 0
      incidents: []
`,
			errs: []string{
				"incidents[0].group.repeat: Field 'repeat' must be a positive integer",
				"incidents[0].group.interval_seconds: Field 'interval_seconds' must be a positive integer",
				"incidents[0].group.incidents: Incident group must have at least one incident",
			},
		},
		{
			name: "group member without at_seconds warns",
			doc: `
name: s
topics:
  - name: orders
incidents:
  - group:
      repeat: 2
      interval_seconds: 20
      incidents:
        - type: rebalance_consumer
`,
			warns: []string{"incidents[0].group.incidents[0]: Group incidents should use 'at_seconds' (relative to cycle start)"},
			clean: true,
		},
		{
			name: "group member scheduled past the interval warns",
			doc: `
name: s
topics:
  - name: orders
incidents:
  - group:
      repeat: 2
      interval_seconds: 20
      incidents:
        - type: rebalance_consumer
          at_seconds: 30
`,
			warns: []string{"incidents[0].group.incidents[0].at_seconds: at_seconds (30) >= interval (20), may overlap"},
			clean: true,
		},
		{
			// QUIRK: group members use a relaxed check, so value checks on delay_ms,
			// rate and duration_seconds are skipped INSIDE a group while the identical
			// standalone incident below is rejected. Presence is still required.
			name: "group members skip value checks",
			doc: `
name: s
topics:
  - name: orders
incidents:
  - group:
      repeat: 1
      interval_seconds: 10
      incidents:
        - type: increase_consumer_delay
          at_seconds: 0
          delay_ms: -5
        - type: change_producer_rate
          at_seconds: 1
          rate: -5
        - type: pause_consumer
          at_seconds: 2
          duration_seconds: 0
`,
			clean: true,
		},
		{
			name: "group members still require the key itself",
			doc: `
name: s
topics:
  - name: orders
incidents:
  - group:
      repeat: 1
      interval_seconds: 10
      incidents:
        - type: increase_consumer_delay
          at_seconds: 0
`,
			errs: []string{"incidents[0].group.incidents[0].delay_ms: Incident type 'increase_consumer_delay' requires 'delay_ms' field"},
		},
		{
			name: "same value standalone is rejected",
			doc:  "name: s\ntopics:\n  - name: orders\nincidents:\n  - type: increase_consumer_delay\n    at_seconds: 0\n    delay_ms: -5\n",
			errs: []string{"incidents[0].delay_ms: Field 'delay_ms' must be a non-negative integer"},
		},
	})
}

func TestValidateFlows(t *testing.T) {
	runValidationCases(t, []validationCase{
		{
			name: "flow essentials",
			doc: `
name: s
flows:
  - rate: 0
`,
			errs: []string{
				"flows[0].name: Missing required field 'name'",
				"flows[0].rate: Field 'rate' must be a positive number",
				"flows[0].steps: Missing required field 'steps'",
			},
		},
		{
			name: "single step flow warns",
			doc: `
name: s
flows:
  - name: f
    steps:
      - topic: a
        event_type: created
`,
			warns: []string{"flows[0].steps: Flow has fewer than 2 steps - consider using regular topics instead"},
			clean: true,
		},
		{
			name: "step essentials",
			doc: `
name: s
flows:
  - name: f
    steps:
      - delay_ms: -1
      - topic: b
        event_type: done
`,
			errs: []string{
				"flows[0].steps[0].topic: Missing required field 'topic'",
				"flows[0].steps[0].event_type: Missing required field 'event_type'",
				"flows[0].steps[0].delay_ms: Field 'delay_ms' must be a non-negative integer",
			},
		},
		{
			name: "delay on the first step warns",
			doc: `
name: s
flows:
  - name: f
    steps:
      - topic: a
        event_type: created
        delay_ms: 50
      - topic: b
        event_type: done
        delay_ms: 50
`,
			warns: []string{"flows[0].steps[0].delay_ms: First step has delay_ms - delay applies before flow starts"},
			clean: true,
		},
		{
			name: "unknown correlation type",
			doc: `
name: s
flows:
  - name: f
    correlation:
      type: sequence
    steps:
      - topic: a
        event_type: created
      - topic: b
        event_type: done
`,
			errs: []string{"flows[0].correlation.type: Invalid correlation type 'sequence'. Valid types: field_ref, uuid"},
		},
		{
			name: "field_ref correlation requires a field",
			doc: `
name: s
flows:
  - name: f
    correlation:
      type: field_ref
    steps:
      - topic: a
        event_type: created
      - topic: b
        event_type: done
`,
			errs: []string{"flows[0].correlation.field: Correlation type 'field_ref' requires 'field' to be specified"},
		},
		{
			name: "step consumer counts",
			doc: `
name: s
flows:
  - name: f
    steps:
      - topic: a
        event_type: created
        consumers:
          groups: 0
          per_group: 0
          delay_ms: -1
      - topic: b
        event_type: done
`,
			errs: []string{
				"flows[0].steps[0].consumers.groups: Field 'groups' must be a positive integer",
				"flows[0].steps[0].consumers.per_group: Field 'per_group' must be a positive integer",
				"flows[0].steps[0].consumers.delay_ms: Field 'delay_ms' must be a non-negative integer",
			},
		},
		{
			name: "step fields are validated like topic fields",
			doc: `
name: s
flows:
  - name: f
    steps:
      - topic: a
        event_type: created
        fields:
          - name: id
            type: blob
      - topic: b
        event_type: done
`,
			errs: []string{
				"flows[0].steps[0].fields[0].type: Unknown field type 'blob'. " +
					"Valid types: array, boolean, enum, faker, float, int, object, string, timestamp, uuid",
			},
		},
	})
}

// TestValidateAccumulatesEveryError is the property the two-pass design exists for: one
// run reports every problem, not just the first.
func TestValidateAccumulatesEveryError(t *testing.T) {
	doc := `
topics:
  - partitions: 0
    replication_factor: 9
    message_schema:
      data_format: xml
      fields:
        - type: enum
incidents:
  - type: meltdown
`
	_, diags := Decode([]byte(doc))
	errs := diags.Errors()
	if len(errs) < 7 {
		t.Fatalf("want at least 7 errors from one pass, got %d:\n%s", len(errs), errs)
	}
	for _, want := range []string{
		"name", "topics[0].partitions", "topics[0].replication_factor",
		"topics[0].message_schema.data_format", "topics[0].message_schema.fields[0].name",
		"topics[0].message_schema.fields[0].values", "incidents[0].type",
	} {
		if !slices.ContainsFunc(errs, func(d Diagnostic) bool { return d.Path == want }) {
			t.Errorf("no error reported at %q\ngot:\n%s", want, errs)
		}
	}
}

// TestValidateReportsLineNumbers pins that a diagnostic carries the source line it came
// from.
func TestValidateReportsLineNumbers(t *testing.T) {
	doc := "name: s\n" + // 1
		"topics:\n" + // 2
		"  - name: orders\n" + // 3
		"    partitions: 0\n" // 4

	_, diags := Decode([]byte(doc))
	errs := diags.Errors()
	if len(errs) != 1 {
		t.Fatalf("want exactly one error, got:\n%s", errs)
	}
	if errs[0].Line != 4 {
		t.Errorf("Line = %d, want 4 (the partitions key)", errs[0].Line)
	}
}

// TestClusterAssumptionsAreOneSwitch pins that relaxing the hardcoded cluster is a
// single assignment, not an edit spread across the validator.
func TestClusterAssumptionsAreOneSwitch(t *testing.T) {
	original := Cluster
	t.Cleanup(func() { Cluster = original })
	Cluster = ClusterAssumptions{Brokers: []string{"broker-a", "broker-b", "broker-c", "broker-d", "broker-e"}}

	doc := `
name: s
topics:
  - name: orders
    replication_factor: 5
incidents:
  - type: stop_broker
    at_seconds: 5
    broker: broker-e
`
	_, diags := Decode([]byte(doc))
	if diags.HasErrors() {
		t.Fatalf("a five-broker cluster should accept rf=5 and broker-e:\n%s", diags.Errors())
	}
}
