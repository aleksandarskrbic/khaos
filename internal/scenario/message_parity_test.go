package scenario

import "testing"

// `khaos validate` output is a compatibility surface: users grep it in CI, so the
// message text is part of the contract and not free to improve. The cases here cover
// the rules the existing tables leave out: the "must be an object/dict" family, the
// per-key type errors, and the flow and group rules.
func TestValidateMessageTextContract(t *testing.T) {
	runValidationCases(t, []validationCase{
		// -------------------------------------------------------------------
		// "must be an object/dict" -- one per container, each with its own wording.
		// -------------------------------------------------------------------
		{
			name: "topic is not a mapping",
			doc:  "name: s\ntopics:\n  - just-a-string\n",
			errs: []string{"topics[0]: Topic must be an object/dict"},
		},
		{
			name: "message_schema is not a mapping",
			doc:  "name: s\ntopics:\n  - name: t\n    message_schema: nope\n",
			errs: []string{"topics[0].message_schema: message_schema must be an object/dict"},
		},
		{
			name: "producer_config is not a mapping",
			doc:  "name: s\ntopics:\n  - name: t\n    producer_config: nope\n",
			errs: []string{"topics[0].producer_config: producer_config must be an object/dict"},
		},
		{
			name: "consumer_config is not a mapping",
			doc:  "name: s\ntopics:\n  - name: t\n    consumer_config: nope\n",
			errs: []string{"topics[0].consumer_config: consumer_config must be an object/dict"},
		},
		{
			name: "schema_registry is not a mapping",
			doc:  "name: s\ntopics:\n  - name: t\nschema_registry: nope\n",
			errs: []string{"schema_registry: schema_registry must be an object/dict"},
		},
		{
			name: "incident is not a mapping",
			doc:  "name: s\ntopics:\n  - name: t\nincidents:\n  - nope\n",
			errs: []string{"incidents[0]: Incident must be an object/dict"},
		},
		{
			name: "target is not a mapping",
			doc: "name: s\ntopics:\n  - name: t\nincidents:\n  - type: rebalance_consumer\n" +
				"    at_seconds: 10\n    target: nope\n",
			errs: []string{"incidents[0].target: Target must be an object/dict"},
		},
		{
			name: "incident group is not a mapping",
			doc:  "name: s\ntopics:\n  - name: t\nincidents:\n  - group: nope\n",
			errs: []string{"incidents[0].group: Incident group must be an object/dict"},
		},
		{
			name: "flow is not a mapping",
			doc:  "name: s\nflows:\n  - nope\n",
			errs: []string{"flows[0]: Flow must be an object/dict"},
		},
		{
			name: "correlation is not a mapping",
			doc:  "name: s\nflows:\n  - name: f\n    correlation: nope\n    steps: []\n",
			errs: []string{"flows[0].correlation: correlation must be an object/dict"},
		},
		{
			name: "step is not a mapping",
			doc:  "name: s\nflows:\n  - name: f\n    steps:\n      - nope\n      - nope\n",
			errs: []string{"flows[0].steps[0]: Step must be an object/dict"},
		},
		{
			name: "step consumers is not a mapping",
			doc: "name: s\nflows:\n  - name: f\n    steps:\n      - topic: a\n        event_type: e\n" +
				"        consumers: nope\n      - topic: b\n        event_type: e\n",
			errs: []string{"flows[0].steps[0].consumers: consumers must be an object/dict"},
		},
		{
			name: "field is not a mapping",
			doc:  "name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n        - nope\n",
			errs: []string{"topics[0].message_schema.fields[0]: Field must be an object/dict"},
		},
		{
			name: "array item is not a mapping", // different wording from a named field
			doc: "name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n" +
				"        - name: a\n          type: array\n          items: 7\n",
			errs: []string{"topics[0].message_schema.fields[0].items: Field 'items' must be an object/dict"},
		},
		{
			name: "fields is not a list",
			doc:  "name: s\ntopics:\n  - name: t\n    message_schema:\n      fields: nope\n",
			errs: []string{"topics[0].message_schema.fields: fields must be a list"},
		},

		// -------------------------------------------------------------------
		// Per-key type and range errors on a field definition.
		// -------------------------------------------------------------------
		{
			name: "type is not a string",
			doc:  "name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n        - name: a\n          type: 7\n",
			errs: []string{"topics[0].message_schema.fields[0].type: Field 'type' must be a string"},
		},
		{
			name: "string length bounds",
			doc: "name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n" +
				"        - name: a\n          type: string\n          min_length: -1\n          max_length: 0\n",
			errs: []string{
				"topics[0].message_schema.fields[0].min_length: Field 'min_length' must be a non-negative integer",
				"topics[0].message_schema.fields[0].max_length: Field 'max_length' must be a positive integer",
			},
		},
		{
			name: "numeric bounds are not numbers",
			doc: "name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n" +
				"        - name: a\n          type: float\n          min: low\n          max: high\n",
			errs: []string{
				"topics[0].message_schema.fields[0].min: Field 'min' must be a number",
				"topics[0].message_schema.fields[0].max: Field 'max' must be a number",
			},
		},
		{
			name: "array item count bounds",
			doc: "name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n" +
				"        - name: a\n          type: array\n          min_items: -1\n          max_items: 0\n" +
				"          items:\n            type: uuid\n",
			errs: []string{
				"topics[0].message_schema.fields[0].min_items: Field 'min_items' must be a non-negative integer",
				"topics[0].message_schema.fields[0].max_items: Field 'max_items' must be a positive integer",
			},
		},
		{
			name: "enum values is not a list",
			doc: "name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n" +
				"        - name: a\n          type: enum\n          values: red\n",
			errs: []string{"topics[0].message_schema.fields[0].values: Field 'values' must be a list"},
		},
		{
			name: "object fields is not a list",
			doc: "name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n" +
				"        - name: a\n          type: object\n          fields: nope\n",
			errs: []string{"topics[0].message_schema.fields[0].fields: Field 'fields' must be a list"},
		},
		{
			name: "faker provider and locale types",
			doc: "name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n" +
				"        - name: a\n          type: faker\n          provider: 7\n",
			errs: []string{"topics[0].message_schema.fields[0].provider: Field 'provider' must be a string"},
		},
		{
			name: "faker locale must be a string",
			doc: "name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n" +
				"        - name: a\n          type: faker\n          provider: name\n          locale: 7\n",
			errs: []string{"topics[0].message_schema.fields[0].locale: Field 'locale' must be a string"},
		},

		// -------------------------------------------------------------------
		// message_schema rules that depend on the defaults rather than on the file.
		// -------------------------------------------------------------------
		{
			// The pairing check compares against the MODEL default, so setting only
			// min_size_bytes is enough to trip it against the default max of 500.
			name: "min_size_bytes alone is compared to the default max",
			doc:  "name: s\ntopics:\n  - name: t\n    message_schema:\n      min_size_bytes: 900\n",
			errs: []string{"topics[0].message_schema: min_size_bytes cannot be greater than max_size_bytes"},
		},
		{
			name: "avro without fields",
			doc:  "name: s\ntopics:\n  - name: t\n    message_schema:\n      data_format: avro\n",
			errs: []string{
				"topics[0].message_schema.fields: data_format 'avro' requires 'fields' to be defined " +
					"(or use schema_provider: registry to fetch from Schema Registry)",
			},
			warns: []string{"schema_registry: Using Avro without Schema Registry (schemaless mode)"},
		},

		// -------------------------------------------------------------------
		// Flow steps.
		// -------------------------------------------------------------------
		{
			name: "step missing topic and event_type",
			doc:  "name: s\nflows:\n  - name: f\n    steps:\n      - {}\n      - {}\n",
			errs: []string{
				"flows[0].steps[0].topic: Missing required field 'topic'",
				"flows[0].steps[0].event_type: Missing required field 'event_type'",
			},
		},
		{
			name: "step topic and event_type wrong type",
			doc: "name: s\nflows:\n  - name: f\n    steps:\n      - topic: 7\n        event_type: 8\n" +
				"      - topic: b\n        event_type: e\n",
			errs: []string{
				"flows[0].steps[0].topic: Field 'topic' must be a string",
				"flows[0].steps[0].event_type: Field 'event_type' must be a string",
			},
		},
		{
			// A negative delay fails the type check and then fails `delay > 0` too, so it
			// is an error only.
			name: "negative first-step delay errors without warning",
			doc: "name: s\nflows:\n  - name: f\n    steps:\n      - topic: a\n        event_type: e\n" +
				"        delay_ms: -1\n      - topic: b\n        event_type: e\n",
			errs: []string{"flows[0].steps[0].delay_ms: Field 'delay_ms' must be a non-negative integer"},
		},
		{
			// QUIRK: the range check and the first-step warning are independent checks
			// against the raw value, so a fractional first-step delay is rejected as
			// "not an integer" AND warned about as a real delay.
			name: "fractional first-step delay errors and warns",
			doc: "name: s\nflows:\n  - name: f\n    steps:\n      - topic: a\n        event_type: e\n" +
				"        delay_ms: 1.5\n      - topic: b\n        event_type: e\n",
			errs: []string{"flows[0].steps[0].delay_ms: Field 'delay_ms' must be a non-negative integer"},
			warns: []string{
				"flows[0].steps[0].delay_ms: First step has delay_ms - delay applies before flow starts",
			},
		},
		{
			name: "first-step delay warning",
			doc: "name: s\nflows:\n  - name: f\n    steps:\n      - topic: a\n        event_type: e\n" +
				"        delay_ms: 500\n      - topic: b\n        event_type: e\n",
			warns: []string{"flows[0].steps[0].delay_ms: First step has delay_ms - delay applies before flow starts"},
			clean: true,
		},
		{
			name: "step consumer counts",
			doc: "name: s\nflows:\n  - name: f\n    steps:\n      - topic: a\n        event_type: e\n" +
				"        consumers:\n          groups: 0\n          per_group: 0\n          delay_ms: -1\n" +
				"      - topic: b\n        event_type: e\n",
			errs: []string{
				"flows[0].steps[0].consumers.groups: Field 'groups' must be a positive integer",
				"flows[0].steps[0].consumers.per_group: Field 'per_group' must be a positive integer",
				"flows[0].steps[0].consumers.delay_ms: Field 'delay_ms' must be a non-negative integer",
			},
		},
		{
			name: "steps is not a list",
			doc:  "name: s\nflows:\n  - name: f\n    steps: nope\n",
			errs: []string{"flows[0].steps: Field 'steps' must be a list"},
		},
		{
			name: "correlation field must be a string",
			doc: "name: s\nflows:\n  - name: f\n    correlation:\n      type: field_ref\n      field: 7\n" +
				"    steps:\n      - topic: a\n        event_type: e\n      - topic: b\n        event_type: e\n",
			errs: []string{"flows[0].correlation.field: Field 'field' must be a string"},
		},

		// -------------------------------------------------------------------
		// Incident targets and groups.
		// -------------------------------------------------------------------
		{
			name: "target topic and group types",
			doc: "name: s\ntopics:\n  - name: t\nincidents:\n  - type: rebalance_consumer\n    at_seconds: 5\n" +
				"    target:\n      topic: 7\n      group: 8\n",
			errs: []string{
				"incidents[0].target.topic: Field 'topic' must be a string",
				"incidents[0].target.group: Field 'group' must be a string",
			},
		},
		{
			name: "target indices is not a list",
			doc: "name: s\ntopics:\n  - name: t\nincidents:\n  - type: rebalance_consumer\n    at_seconds: 5\n" +
				"    target:\n      indices: nope\n",
			errs: []string{"incidents[0].target.indices: Field 'indices' must be a list"},
		},
		{
			name: "standalone at_seconds must be an integer",
			doc:  "name: s\ntopics:\n  - name: t\nincidents:\n  - type: rebalance_consumer\n    at_seconds: soon\n",
			errs: []string{"incidents[0].at_seconds: Field 'at_seconds' must be an integer"},
		},
		{
			name: "group member without at_seconds only warns",
			doc: "name: s\ntopics:\n  - name: t\nincidents:\n  - group:\n      repeat: 2\n      interval_seconds: 30\n" +
				"      incidents:\n        - type: rebalance_consumer\n",
			warns: []string{
				"incidents[0].group.incidents[0]: Group incidents should use 'at_seconds' (relative to cycle start)",
			},
			clean: true,
		},
		{
			name: "group member overlapping the cycle",
			doc: "name: s\ntopics:\n  - name: t\nincidents:\n  - group:\n      repeat: 2\n      interval_seconds: 60\n" +
				"      incidents:\n        - type: rebalance_consumer\n          at_seconds: 90\n",
			warns: []string{
				"incidents[0].group.incidents[0].at_seconds: at_seconds (90) >= interval (60), may overlap",
			},
			clean: true,
		},
		{
			// Group members pass a relaxed check, so the key must be PRESENT but its
			// value is never range-checked -- delay_ms: -5 is accepted inside a group and
			// rejected outside one. An oversight, pinned here on purpose.
			name: "group member value checks are relaxed",
			doc: "name: s\ntopics:\n  - name: t\nincidents:\n  - group:\n      repeat: 1\n      interval_seconds: 30\n" +
				"      incidents:\n        - type: increase_consumer_delay\n          at_seconds: 5\n" +
				"          delay_ms: -5\n",
			clean: true,
		},
	})
}

// TestRateText pins the fixed-notation shortest form used to render a producer rate in
// the incident message.
func TestRateText(t *testing.T) {
	tests := []struct {
		in   float64
		want string
	}{
		{in: 0, want: "0"},
		{in: 200, want: "200"},
		{in: 500.5, want: "500.5"},
		// %g would render this as "1e+06".
		{in: 1000000, want: "1000000"},
		{in: 0.5, want: "0.5"},
	}
	for _, tt := range tests {
		if got := rateText(tt.in); got != tt.want {
			t.Errorf("rateText(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
