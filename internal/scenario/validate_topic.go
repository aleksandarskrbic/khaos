package scenario

import (
	"fmt"
	"strings"

	"go.yaml.in/yaml/v3"
)

// topics validates the topic list and emits the schemaless-mode warning.
//
// The warning names the data_format of the LAST avro/protobuf topic seen, since the
// scan reassigns schemaFormatName on every matching topic in the loop.
func (v *validator) topics(n *yaml.Node, hasSchemaRegistry bool) {
	if !isSequence(n) {
		v.errf(n, "topics", "Field 'topics' must be a list")
		return
	}
	schemaFormatName := ""
	var lastFormatNode *yaml.Node
	for i, topic := range seqItems(n) {
		path := index("topics", i)
		v.topic(topic, path)
		format := mapGet(mapGet(topic, "message_schema"), "data_format")
		if text := scalarText(format); text == "avro" || text == "protobuf" {
			schemaFormatName = text
			lastFormatNode = format
		}
	}
	if schemaFormatName != "" && !hasSchemaRegistry {
		v.warnf(lastFormatNode, "schema_registry",
			"Using %s without Schema Registry (schemaless mode)", title(schemaFormatName))
	}
}

// title upper-cases the first letter of a single-word format name ("avro" -> "Avro",
// "protobuf" -> "Protobuf").
func title(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// topic validates one entry of `topics:`.
func (v *validator) topic(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "Topic must be an object/dict")
		return
	}
	v.knownKeys(n, path,
		"name", "partitions", "replication_factor", "num_producers", "num_consumer_groups",
		"consumers_per_group", "producer_rate", "consumer_delay_ms", "message_schema",
		"producer_config", "consumer_config", "schema_provider", "subject_name")

	switch name := mapGet(n, "name"); {
	case name == nil:
		v.errf(n, join(path, "name"), "Missing required field 'name'")
	case !isString(name):
		v.errf(name, join(path, "name"), "Field 'name' must be a string")
	}

	// partitions: the >100 case is a warning, not an error.
	if p := mapGet(n, "partitions"); p != nil {
		if count, ok := intValue(p); !ok || count < 1 {
			v.errf(p, join(path, "partitions"), "Field 'partitions' must be a positive integer")
		} else if count > 100 {
			v.warnf(p, join(path, "partitions"),
				"High partition count (%d) may impact performance", count)
		}
	}

	// replication_factor: the upper bound is a cluster assumption (see Cluster).
	if rf := mapGet(n, "replication_factor"); rf != nil {
		if factor, ok := intValue(rf); !ok || factor < 1 {
			v.errf(rf, join(path, "replication_factor"),
				"Field 'replication_factor' must be a positive integer")
		} else if factor > Cluster.MaxReplicationFactor() {
			v.errf(rf, join(path, "replication_factor"),
				"Replication factor cannot exceed %d (cluster has %d brokers)",
				Cluster.MaxReplicationFactor(), len(Cluster.Brokers))
		}
	}

	// num_producers is non-negative, unlike the other counts: zero producers is a
	// legitimate consume-only topic.
	v.intAtLeast(n, path, "num_producers", 0, "Field 'num_producers' must be a non-negative integer")
	v.intAtLeast(n, path, "num_consumer_groups", 1, "Field 'num_consumer_groups' must be a positive integer")
	v.intAtLeast(n, path, "consumers_per_group", 1, "Field 'consumers_per_group' must be a positive integer")

	if rate := mapGet(n, "producer_rate"); rate != nil {
		if r, ok := floatValue(rate); !ok || r <= 0 {
			v.errf(rate, join(path, "producer_rate"), "Field 'producer_rate' must be a positive number")
		}
	}
	v.intAtLeast(n, path, "consumer_delay_ms", 0, "Field 'consumer_delay_ms' must be a non-negative integer")

	// schema_provider is read before message_schema because it changes whether fields
	// are required or forbidden.
	providerNode := mapGet(n, "schema_provider")
	provider := defaultSchemaProvider
	if providerNode != nil {
		provider = scalarText(providerNode)
	}

	if ms := mapGet(n, "message_schema"); ms != nil {
		v.messageSchema(ms, join(path, "message_schema"), provider)
	}
	if pc := mapGet(n, "producer_config"); pc != nil {
		v.producerConfig(pc, join(path, "producer_config"))
	}
	if cc := mapGet(n, "consumer_config"); cc != nil {
		v.consumerConfig(cc, join(path, "consumer_config"))
	}

	if !validSchemaProviders.has(provider) {
		v.errf(providerNode, join(path, "schema_provider"),
			"Invalid schema_provider '%s'. Valid values: %s", provider, validSchemaProviders)
	}

	// registry provider: subject_name required, inline fields forbidden.
	if provider == "registry" {
		switch subject := mapGet(n, "subject_name"); {
		case subject == nil:
			v.errf(n, join(path, "subject_name"),
				"Field 'subject_name' is required when schema_provider is 'registry'")
		case !isString(subject):
			v.errf(subject, join(path, "subject_name"), "Field 'subject_name' must be a string")
		}
		if fields := mapGet(mapGet(n, "message_schema"), "fields"); fields != nil && len(seqItems(fields)) > 0 {
			v.errf(fields, path,
				"Cannot define 'message_schema.fields' when schema_provider is 'registry' "+
					"(schema will be fetched from Schema Registry)")
		}
	}
}

// intAtLeast is the shape shared by the "must be a (non-)negative integer" checks: the
// key is optional, but when present it must be an int no smaller than lo.
func (v *validator) intAtLeast(n *yaml.Node, path, key string, lo int, message string) {
	node := mapGet(n, key)
	if node == nil {
		return
	}
	if value, ok := intValue(node); !ok || value < lo {
		v.errf(node, join(path, key), "%s", message)
	}
}

// messageSchema validates one `message_schema:` block.
func (v *validator) messageSchema(n *yaml.Node, path, provider string) {
	if !isMapping(n) {
		v.errf(n, path, "message_schema must be an object/dict")
		return
	}
	v.knownKeys(n, path, "key_distribution", "key_cardinality", "min_size_bytes",
		"max_size_bytes", "data_format", "fields")

	if kd := mapGet(n, "key_distribution"); kd != nil {
		if text := scalarText(kd); !validKeyDistributions.has(text) {
			v.errf(kd, join(path, "key_distribution"),
				"Invalid key_distribution '%s'. Valid values: %s", text, validKeyDistributions)
		}
	}
	v.intAtLeast(n, path, "key_cardinality", 1, "Field 'key_cardinality' must be a positive integer")

	if df := mapGet(n, "data_format"); df != nil {
		text := scalarText(df)
		if !validDataFormats.has(text) {
			v.errf(df, join(path, "data_format"),
				"Invalid data_format '%s'. Valid values: %s", text, validDataFormats)
		}
		// Binary formats need a schema to encode against. The registry provider
		// supplies one at runtime, so the requirement only applies inline.
		fields := mapGet(n, "fields")
		hasFields := fields != nil && len(seqItems(fields)) > 0
		if provider == "inline" && (text == "avro" || text == "protobuf") && !hasFields {
			v.errf(df, join(path, "fields"),
				"data_format '%s' requires 'fields' to be defined "+
					"(or use schema_provider: registry to fetch from Schema Registry)", text)
		}
	}

	v.intAtLeast(n, path, "min_size_bytes", 1, "Field 'min_size_bytes' must be a positive integer")
	v.intAtLeast(n, path, "max_size_bytes", 1, "Field 'max_size_bytes' must be a positive integer")
	// The pairing is checked against the defaults when a bound is omitted, so setting
	// only min_size_bytes: 900 is an error against the default max of 500.
	v.pairing(n, path, "min_size_bytes", "max_size_bytes", defaultMinSizeBytes, defaultMaxSizeBytes,
		"min_size_bytes cannot be greater than max_size_bytes")

	if fields := mapGet(n, "fields"); fields != nil {
		v.fields(fields, join(path, "fields"))
	}
}

// pairing reproduces the four "min cannot be greater than max" checks, each of which
// falls back to the model default for an omitted bound and skips entirely when either
// present value is not an integer.
func (v *validator) pairing(n *yaml.Node, path, minKey, maxKey string, minDefault, maxDefault int, message string) {
	minNode, maxNode := mapGet(n, minKey), mapGet(n, maxKey)
	minVal, maxVal := minDefault, maxDefault
	if minNode != nil {
		value, ok := intValue(minNode)
		if !ok {
			return
		}
		minVal = value
	}
	if maxNode != nil {
		value, ok := intValue(maxNode)
		if !ok {
			return
		}
		maxVal = value
	}
	if minVal > maxVal {
		v.errf(cmpNode(minNode, n), path, "%s", message)
	}
}

// cmpNode picks the most specific node available to hang a pairing error on.
func cmpNode(preferred, fallback *yaml.Node) *yaml.Node {
	if preferred != nil {
		return preferred
	}
	return fallback
}

// schemaRegistry validates a top-level `schema_registry:` block.
func (v *validator) schemaRegistry(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "schema_registry must be an object/dict")
		return
	}
	v.knownKeys(n, path, "url")
	switch url := mapGet(n, "url"); {
	case url == nil:
		v.errf(n, join(path, "url"), "Missing required field 'url'")
	case !isString(url):
		v.errf(url, join(path, "url"), "Field 'url' must be a string")
	case !strings.HasPrefix(url.Value, "http://") && !strings.HasPrefix(url.Value, "https://"):
		v.errf(url, join(path, "url"), "Field 'url' must be a valid HTTP(S) URL")
	}
}

// producerConfig validates one `producer_config:` block.
func (v *validator) producerConfig(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "producer_config must be an object/dict")
		return
	}
	v.knownKeys(n, path, "batch_size", "linger_ms", "acks", "compression_type", "duplicate_rate")

	v.intAtLeast(n, path, "batch_size", 0, "Field 'batch_size' must be a non-negative integer")
	v.intAtLeast(n, path, "linger_ms", 0, "Field 'linger_ms' must be a non-negative integer")

	// acks is compared as a string, so both `acks: 1` and `acks: "1"` are accepted.
	if acks := mapGet(n, "acks"); acks != nil {
		text := scalarText(acks)
		if !validAcks.has(text) {
			v.errf(acks, join(path, "acks"), "Invalid acks '%s'. Valid values: %s", text, validAcks)
		}
	}
	if ct := mapGet(n, "compression_type"); ct != nil {
		if text := scalarText(ct); !validCompressionTypes.has(text) {
			v.errf(ct, join(path, "compression_type"),
				"Invalid compression_type '%s'. Valid values: %s", text, validCompressionTypes)
		}
	}
	v.rate01(n, path, "duplicate_rate",
		"Field 'duplicate_rate' must be a number between 0.0 and 1.0",
		"High duplicate rate (%s) will produce many duplicate messages")
}

// consumerConfig validates one `consumer_config:` block.
func (v *validator) consumerConfig(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "consumer_config must be an object/dict")
		return
	}
	v.knownKeys(n, path, "failure_rate", "commit_failure_rate", "on_failure", "max_retries")

	v.rate01(n, path, "failure_rate",
		"Field 'failure_rate' must be a number between 0.0 and 1.0",
		"High failure rate (%s) may cause significant message loss")
	v.rate01(n, path, "commit_failure_rate",
		"Field 'commit_failure_rate' must be a number between 0.0 and 1.0",
		"High commit failure rate (%s) may cause significant reprocessing")

	if of := mapGet(n, "on_failure"); of != nil {
		text := scalarText(of)
		if !validOnFailureActions.has(text) {
			v.errf(of, join(path, "on_failure"),
				"Invalid on_failure '%s'. Valid values: %s", text, validOnFailureActions)
		} else if text == "dlq" {
			v.warnf(of, join(path, "on_failure"),
				"DLQ mode enabled - ensure DLQ topics exist or are auto-created")
		}
	}
	v.intAtLeast(n, path, "max_retries", 0, "Field 'max_retries' must be a non-negative integer")
}

// rate01 is the shape shared by the three [0,1] probability keys: out of range is an
// error, and anything above 0.5 is a warning rendered as a percentage.
func (v *validator) rate01(n *yaml.Node, path, key, errMessage, warnFormat string) {
	node := mapGet(n, key)
	if node == nil {
		return
	}
	rate, ok := floatValue(node)
	if !ok || rate < 0 || rate > 1 {
		v.errf(node, join(path, key), "%s", errMessage)
		return
	}
	if rate > 0.5 {
		v.warnf(node, join(path, key), warnFormat, percent(rate))
	}
}

// percent renders a rate as a whole-number percentage, e.g. 0.75 -> "75%".
func percent(rate float64) string { return fmt.Sprintf("%.0f%%", rate*100) }
