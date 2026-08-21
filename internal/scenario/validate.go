package scenario

import (
	"fmt"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"
)

// This file validates a scenario document: top level and topics, then fields
// recursively, then flows and steps, then incidents.
//
// It runs against the raw yaml.Node tree, before any typed decoding, so a scenario
// author gets every problem in one pass instead of one per edit-run cycle. The typed
// decode in decode.go only happens once this pass reports no errors.
//
// Messages are kept stable, including the sorted comma-joined "Valid values" lists,
// because they are the user-facing surface of the tool.

// ClusterAssumptions is what khaos assumes about the Kafka cluster a scenario will run
// against. Both facts below are hardcoded and are the two places a scenario can be
// rejected for a reason that has nothing to do with the scenario itself.
//
// Relaxing these for external clusters is wanted but not yet agreed. Collecting both
// behind one exported var means the relaxation is a single edit -- assign a different
// ClusterAssumptions -- rather than a hunt through the validator.
type ClusterAssumptions struct {
	// Brokers is the closed set of container names stop_broker/start_broker may name,
	// and its size is the maximum allowed replication factor.
	Brokers []string
}

// Cluster is the hardcoded local docker-compose cluster: 3 brokers, and a replication
// factor cap to match. See ClusterAssumptions.
var Cluster = ClusterAssumptions{Brokers: []string{"kafka-1", "kafka-2", "kafka-3"}}

// AllowsBroker reports whether name is a broker of this cluster.
func (c ClusterAssumptions) AllowsBroker(name string) bool {
	return slices.Contains(c.Brokers, name)
}

// MaxReplicationFactor is the largest replication factor the cluster can satisfy.
func (c ClusterAssumptions) MaxReplicationFactor() int { return len(c.Brokers) }

// brokerList renders the brokers sorted and comma-joined, for error messages.
func (c ClusterAssumptions) brokerList() string {
	b := slices.Clone(c.Brokers)
	slices.Sort(b)
	return strings.Join(b, ", ")
}

// valueSet is a closed set of accepted string values that can also render itself
// sorted and comma-joined for an error message.
type valueSet []string

func newValueSet(values ...string) valueSet {
	s := valueSet(slices.Clone(values))
	slices.Sort(s)
	return s
}

func (s valueSet) has(v string) bool { return slices.Contains(s, v) }
func (s valueSet) String() string    { return strings.Join(s, ", ") }

// The closed value sets accepted throughout the document.
var (
	validKeyDistributions = newValueSet("uniform", "zipfian", "single_key", "round_robin")
	validCompressionTypes = newValueSet("none", "gzip", "snappy", "lz4", "zstd")
	validDataFormats      = newValueSet("json", "avro", "protobuf")
	// "-1" is accepted alongside "all" even though it is undocumented -- dropping it
	// would reject a working scenario.
	validAcks              = newValueSet("0", "1", "all", "-1")
	validSchemaProviders   = newValueSet("inline", "registry")
	validOnFailureActions  = newValueSet("skip", "dlq", "retry")
	validCorrelationTypes  = newValueSet(CorrelationUUID, CorrelationFieldRef)
	validIncidentTypeNames = newValueSet(
		IncidentStopBroker, IncidentStartBroker, IncidentPauseConsumer,
		IncidentRebalanceConsumer, IncidentIncreaseConsumerDlay, IncidentChangeProducerRate,
	)
	// sortedFieldTypes renders ValidFieldTypes for messages.
	sortedFieldTypes = func() valueSet {
		names := make([]string, 0, len(ValidFieldTypes))
		for t := range ValidFieldTypes {
			names = append(names, t)
		}
		return newValueSet(names...)
	}()
)

// validator accumulates findings while walking one document.
type validator struct {
	diags Diagnostics
	// strict enables unknown-key reporting. Off by default: shipped scenarios carry
	// keys a strict parser would reject (scenarios/serialization/protobuf-example.yaml
	// uses `pattern` on a faker field, which no model has ever read).
	strict bool
	// visiting is the set of field nodes currently on the recursion stack. See field.
	visiting map[*yaml.Node]bool
}

func (v *validator) errf(n *yaml.Node, path, format string, args ...any) {
	v.diags = append(v.diags, Diagnostic{
		Path: path, Message: fmt.Sprintf(format, args...),
		Severity: SeverityError, Line: nodeLine(n),
	})
}

func (v *validator) warnf(n *yaml.Node, path, format string, args ...any) {
	v.diags = append(v.diags, Diagnostic{
		Path: path, Message: fmt.Sprintf(format, args...),
		Severity: SeverityWarning, Line: nodeLine(n),
	})
}

// join builds a child path, tolerating an empty parent so root-level keys come out as
// "name" rather than ".name".
func join(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

func index(path string, i int) string { return fmt.Sprintf("%s[%d]", path, i) }

// knownKeys reports unknown keys when strict mode is on. Ordinary decoding ignores
// them.
func (v *validator) knownKeys(n *yaml.Node, path string, known ...string) {
	if !v.strict {
		return
	}
	for _, e := range mappingEntries(n) {
		if !slices.Contains(known, e.key) {
			v.errf(e.keyNode, join(path, e.key), "Unknown field %q", e.key)
		}
	}
}

// ---------------------------------------------------------------------------
// Top level
// ---------------------------------------------------------------------------

func (v *validator) scenario(n *yaml.Node) {
	n = resolveNode(n)
	if !isMapping(n) {
		v.errf(n, "root", "Scenario must be a YAML object/dict")
		return
	}
	v.knownKeys(n, "", "name", "description", "topics", "flows", "incidents", "schema_registry")

	// name, plus the non-empty check: Discover keys scenarios by file path and skips
	// any file without a usable name, so an empty name is never addressable.
	switch name := mapGet(n, "name"); {
	case name == nil:
		v.errf(n, "name", "Missing required field 'name'")
	case !isString(name):
		v.errf(name, "name", "Field 'name' must be a string")
	case strings.TrimSpace(name.Value) == "":
		v.errf(name, "name", "Field 'name' must not be empty")
	}

	// At least one of topics/flows. Note both a missing key and a key whose value is
	// not a non-empty list count as absent here, which is why a bare `topics:`
	// produces this error as well as the "must be a list" one below.
	topics, flows := mapGet(n, "topics"), mapGet(n, "flows")
	hasTopics := isSequence(topics) && len(seqItems(topics)) > 0
	hasFlows := isSequence(flows) && len(seqItems(flows)) > 0
	if !hasTopics && !hasFlows {
		v.errf(n, "topics", "Scenario must have at least 'topics' or 'flows'")
	}

	hasSchemaRegistry := false
	if reg := mapGet(n, "schema_registry"); reg != nil {
		hasSchemaRegistry = true
		v.schemaRegistry(reg, "schema_registry")
	}

	if topics != nil {
		v.topics(topics, hasSchemaRegistry)
	}
	if flows != nil {
		v.flows(flows, "flows")
	}
	if incidents := mapGet(n, "incidents"); incidents != nil {
		if !isSequence(incidents) {
			v.errf(incidents, "incidents", "Field 'incidents' must be a list")
		} else {
			for i, inc := range seqItems(incidents) {
				v.incident(inc, index("incidents", i))
			}
		}
	}
}

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

// ---------------------------------------------------------------------------
// Fields
// ---------------------------------------------------------------------------

func (v *validator) fields(n *yaml.Node, path string) {
	if !isSequence(n) {
		v.errf(n, path, "fields must be a list")
		return
	}
	for i, f := range seqItems(n) {
		v.field(f, index(path, i), false)
	}
}

// field validates one field definition, recursing into nested objects and array items.
//
// asItem distinguishes a named field from an array's `items:` schema, which differ in
// two ways preserved verbatim: an array item needs no `name`, and an array item of type
// faker is NOT checked for a `provider` since the item dispatch below simply has no
// faker branch.
//
// A YAML anchor can make a field definition contain itself:
//
//	fields:
//	  - &f
//	    name: a
//	    type: array
//	    items: *f
//
// Without a guard that recurses forever, walking a hand-built node tree until the
// machine runs out of memory. Nodes on the current recursion stack are therefore
// tracked and re-entry is reported. A finite tree, however deeply nested, is
// unaffected: only an alias can revisit a node.
func (v *validator) field(n *yaml.Node, path string, asItem bool) {
	n = resolveNode(n)
	if v.visiting[n] {
		v.errf(n, path, "Field definition refers to itself through a YAML anchor")
		return
	}
	if v.visiting == nil {
		v.visiting = make(map[*yaml.Node]bool)
	}
	v.visiting[n] = true
	defer delete(v.visiting, n)

	if !isMapping(n) {
		if asItem {
			v.errf(n, path, "Array item must be an object/dict")
		} else {
			v.errf(n, path, "Field must be an object/dict")
		}
		return
	}
	v.knownKeys(n, path, "name", "type", "min", "max", "min_length", "max_length",
		"cardinality", "values", "fields", "items", "min_items", "max_items",
		"provider", "locale")

	if !asItem {
		switch name := mapGet(n, "name"); {
		case name == nil:
			v.errf(n, join(path, "name"), "Missing required field 'name'")
		case !isString(name):
			v.errf(name, join(path, "name"), "Field 'name' must be a string")
		}
	}

	typeNode := mapGet(n, "type")
	if typeNode == nil {
		v.errf(n, join(path, "type"), "Missing required field 'type'")
		return
	}
	if !isString(typeNode) {
		v.errf(typeNode, join(path, "type"), "Field 'type' must be a string")
		return
	}
	fieldType := typeNode.Value
	if !ValidFieldTypes[fieldType] {
		v.errf(typeNode, join(path, "type"),
			"Unknown field type '%s'. Valid types: %s", fieldType, sortedFieldTypes)
		return
	}

	switch fieldType {
	case FieldString:
		v.stringField(n, path)
	case FieldInt:
		v.numericRange(n, path)
		v.cardinality(n, path)
	case FieldFloat:
		// No cardinality check for floats.
		v.numericRange(n, path)
	case FieldEnum:
		v.enumField(n, path)
	case FieldObject:
		v.objectField(n, path)
	case FieldArray:
		v.arrayField(n, path)
	case FieldFaker:
		if !asItem {
			v.fakerField(n, path)
		}
	}
}

// stringField validates the string-only constraints.
func (v *validator) stringField(n *yaml.Node, path string) {
	v.intAtLeast(n, path, "min_length", 0, "Field 'min_length' must be a non-negative integer")
	v.intAtLeast(n, path, "max_length", 1, "Field 'max_length' must be a positive integer")
	// The defaults here (0 and 100) are the validator's own; the field model has no
	// length defaults at all.
	v.pairing(n, path, "min_length", "max_length", 0, 100,
		"min_length cannot be greater than max_length")
	v.cardinality(n, path)
}

// cardinality validates the shared `cardinality:` key.
func (v *validator) cardinality(n *yaml.Node, path string) {
	v.intAtLeast(n, path, "cardinality", 1, "Field 'cardinality' must be a positive integer")
}

// numericRange validates min/max: they may be int or float, and are only compared when
// both are present.
func (v *validator) numericRange(n *yaml.Node, path string) {
	minNode, maxNode := mapGet(n, "min"), mapGet(n, "max")
	minOK, maxOK := true, true
	if minNode != nil && !isNumber(minNode) {
		v.errf(minNode, join(path, "min"), "Field 'min' must be a number")
		minOK = false
	}
	if maxNode != nil && !isNumber(maxNode) {
		v.errf(maxNode, join(path, "max"), "Field 'max' must be a number")
		maxOK = false
	}
	if minNode == nil || maxNode == nil || !minOK || !maxOK {
		return
	}
	minVal, _ := floatValue(minNode)
	maxVal, _ := floatValue(maxNode)
	if minVal > maxVal {
		v.errf(minNode, path, "min cannot be greater than max")
	}
}

// enumField validates an enum field's `values:` list.
func (v *validator) enumField(n *yaml.Node, path string) {
	values := mapGet(n, "values")
	if values == nil {
		v.errf(n, join(path, "values"), "Enum field requires 'values' list")
		return
	}
	if !isSequence(values) {
		v.errf(values, join(path, "values"), "Field 'values' must be a list")
		return
	}
	items := seqItems(values)
	if len(items) == 0 {
		v.errf(values, join(path, "values"), "Enum must have at least one value")
	}
	for i, item := range items {
		if !isString(item) {
			v.errf(item, index(join(path, "values"), i), "Enum values must be strings")
		}
	}
}

// objectField validates an object field's nested `fields:` list.
func (v *validator) objectField(n *yaml.Node, path string) {
	fields := mapGet(n, "fields")
	if fields == nil {
		v.errf(n, join(path, "fields"), "Object field requires 'fields' list")
		return
	}
	if !isSequence(fields) {
		v.errf(fields, join(path, "fields"), "Field 'fields' must be a list")
		return
	}
	items := seqItems(fields)
	if len(items) == 0 {
		v.errf(fields, join(path, "fields"), "Object must have at least one field")
	}
	// Nested object fields are full fields, so they do need a name.
	for i, nested := range items {
		v.field(nested, index(join(path, "fields"), i), false)
	}
}

// arrayField validates an array field's `items:` schema and length bounds.
func (v *validator) arrayField(n *yaml.Node, path string) {
	items := mapGet(n, "items")
	if items == nil {
		v.errf(n, join(path, "items"), "Array field requires 'items' schema")
		return
	}
	if !isMapping(items) {
		v.errf(items, join(path, "items"), "Field 'items' must be an object/dict")
		return
	}
	v.intAtLeast(n, path, "min_items", 0, "Field 'min_items' must be a non-negative integer")
	v.intAtLeast(n, path, "max_items", 1, "Field 'max_items' must be a positive integer")
	v.pairing(n, path, "min_items", "max_items", defaultMinItems, defaultMaxItems,
		"min_items cannot be greater than max_items")
	v.field(items, join(path, "items"), true)
}

// fakerField validates a faker field's `provider:`/`locale:` keys.
func (v *validator) fakerField(n *yaml.Node, path string) {
	provider := mapGet(n, "provider")
	if provider == nil {
		v.errf(n, join(path, "provider"), "Faker field requires 'provider'")
		return
	}
	if !isString(provider) {
		v.errf(provider, join(path, "provider"), "Field 'provider' must be a string")
		return
	}
	if locale := mapGet(n, "locale"); locale != nil && !isString(locale) {
		v.errf(locale, join(path, "locale"), "Field 'locale' must be a string")
	}
}

// ---------------------------------------------------------------------------
// Flows
// ---------------------------------------------------------------------------

func (v *validator) flows(n *yaml.Node, path string) {
	if !isSequence(n) {
		v.errf(n, path, "Field 'flows' must be a list")
		return
	}
	for i, f := range seqItems(n) {
		v.flow(f, index(path, i))
	}
}

// flow validates one entry of `flows:`.
func (v *validator) flow(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "Flow must be an object/dict")
		return
	}
	v.knownKeys(n, path, "name", "rate", "steps", "correlation")

	switch name := mapGet(n, "name"); {
	case name == nil:
		v.errf(n, join(path, "name"), "Missing required field 'name'")
	case !isString(name):
		v.errf(name, join(path, "name"), "Field 'name' must be a string")
	}

	if rate := mapGet(n, "rate"); rate != nil {
		if r, ok := floatValue(rate); !ok || r <= 0 {
			v.errf(rate, join(path, "rate"), "Field 'rate' must be a positive number")
		}
	}
	if corr := mapGet(n, "correlation"); corr != nil {
		v.correlation(corr, join(path, "correlation"))
	}

	steps := mapGet(n, "steps")
	if steps == nil {
		v.errf(n, join(path, "steps"), "Missing required field 'steps'")
		return
	}
	if !isSequence(steps) {
		v.errf(steps, join(path, "steps"), "Field 'steps' must be a list")
		return
	}
	items := seqItems(steps)
	// A one-step flow is legal but pointless -- warning, not error.
	if len(items) < 2 {
		v.warnf(steps, join(path, "steps"),
			"Flow has fewer than 2 steps - consider using regular topics instead")
	}
	for i, step := range items {
		v.flowStep(step, index(join(path, "steps"), i), i == 0)
	}
}

// correlation validates a `correlation:` block. field_ref without a field name is the
// one hard error here.
func (v *validator) correlation(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "correlation must be an object/dict")
		return
	}
	v.knownKeys(n, path, "type", "field")

	corrType := defaultCorrelationType
	typeNode := mapGet(n, "type")
	if typeNode != nil {
		corrType = scalarText(typeNode)
	}
	if !validCorrelationTypes.has(corrType) {
		v.errf(cmpNode(typeNode, n), join(path, "type"),
			"Invalid correlation type '%s'. Valid types: %s", corrType, validCorrelationTypes)
	}
	if corrType == CorrelationFieldRef {
		switch fieldNode := mapGet(n, "field"); {
		case fieldNode == nil:
			v.errf(n, join(path, "field"),
				"Correlation type 'field_ref' requires 'field' to be specified")
		case !isString(fieldNode):
			v.errf(fieldNode, join(path, "field"), "Field 'field' must be a string")
		}
	}
}

// flowStep validates one entry of a flow's `steps:` list.
func (v *validator) flowStep(n *yaml.Node, path string, isFirst bool) {
	if !isMapping(n) {
		v.errf(n, path, "Step must be an object/dict")
		return
	}
	v.knownKeys(n, path, "topic", "event_type", "delay_ms", "fields", "consumers")

	switch topic := mapGet(n, "topic"); {
	case topic == nil:
		v.errf(n, join(path, "topic"), "Missing required field 'topic'")
	case !isString(topic):
		v.errf(topic, join(path, "topic"), "Field 'topic' must be a string")
	}
	switch event := mapGet(n, "event_type"); {
	case event == nil:
		v.errf(n, join(path, "event_type"), "Missing required field 'event_type'")
	case !isString(event):
		v.errf(event, join(path, "event_type"), "Field 'event_type' must be a string")
	}

	if delay := mapGet(n, "delay_ms"); delay != nil {
		if ms, ok := intValue(delay); !ok || ms < 0 {
			v.errf(delay, join(path, "delay_ms"), "Field 'delay_ms' must be a non-negative integer")
		}
		// The range check above and the first-step warning below are independent
		// checks against the raw value, not chained as an else-branch: a first-step
		// `delay_ms: 1.5` produces BOTH the range error and this warning. Reading the
		// value as a float here (rather than only as an int) is what surfaces the
		// warning even when the range check already failed. A non-numeric delay_ms
		// simply fails the isNum check below and skips the warning instead of
		// crashing.
		if value, isNum := floatValue(delay); isFirst && isNum && value > 0 {
			v.warnf(delay, join(path, "delay_ms"),
				"First step has delay_ms - delay applies before flow starts")
		}
	}

	if fields := mapGet(n, "fields"); fields != nil {
		v.fields(fields, join(path, "fields"))
	}
	if consumers := mapGet(n, "consumers"); consumers != nil {
		v.stepConsumers(consumers, join(path, "consumers"))
	}
}

// stepConsumers validates a step's `consumers:` block.
func (v *validator) stepConsumers(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "consumers must be an object/dict")
		return
	}
	v.knownKeys(n, path, "groups", "per_group", "delay_ms")
	v.intAtLeast(n, path, "groups", 1, "Field 'groups' must be a positive integer")
	v.intAtLeast(n, path, "per_group", 1, "Field 'per_group' must be a positive integer")
	v.intAtLeast(n, path, "delay_ms", 0, "Field 'delay_ms' must be a non-negative integer")
}

// ---------------------------------------------------------------------------
// Incidents
// ---------------------------------------------------------------------------

// incidentKeys is the union of every key any incident type reads, plus `group`.
var incidentKeys = []string{
	"type", "group", "at_seconds", "every_seconds", "initial_delay_seconds",
	"target", "broker", "duration_seconds", "delay_ms", "rate",
}

// incident validates one entry of the top-level `incidents:` list, which is either a
// standalone incident or a group wrapper.
func (v *validator) incident(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "Incident must be an object/dict")
		return
	}
	// A `group` key makes the whole entry a group and nothing else on it is read.
	if group := mapGet(n, "group"); group != nil {
		v.incidentGroup(group, join(path, "group"))
		return
	}
	v.knownKeys(n, path, incidentKeys...)

	incidentType, ok := v.incidentType(n, path)
	if !ok {
		return
	}

	// Standalone incidents need a schedule; group members do not.
	at, every := mapGet(n, "at_seconds"), mapGet(n, "every_seconds")
	if at == nil && every == nil {
		v.errf(n, path, "Incident must have either 'at_seconds' or 'every_seconds'")
	}
	if at != nil && !isInt(at) {
		v.errf(at, join(path, "at_seconds"), "Field 'at_seconds' must be an integer")
	}
	if every != nil {
		if seconds, ok := intValue(every); !ok || seconds < 1 {
			v.errf(every, join(path, "every_seconds"),
				"Field 'every_seconds' must be a positive integer")
		}
	}

	if target := mapGet(n, "target"); target != nil {
		v.target(target, join(path, "target"))
	}
	v.incidentTypeFields(n, incidentType, path, true)
}

// incidentType reads and checks the `type` key shared by both incident paths.
func (v *validator) incidentType(n *yaml.Node, path string) (string, bool) {
	typeNode := mapGet(n, "type")
	if typeNode == nil {
		v.errf(n, join(path, "type"), "Missing required field 'type'")
		return "", false
	}
	incidentType := scalarText(typeNode)
	if !validIncidentTypeNames.has(incidentType) {
		v.errf(typeNode, join(path, "type"),
			"Unknown incident type '%s'. Valid types: %s", incidentType, validIncidentTypeNames)
		return "", false
	}
	return incidentType, true
}

// incidentTypeFields checks the parameters each incident type requires.
//
// strict controls whether a present value is also range-checked: a standalone incident
// is strict, a group member is not. Inside a group the key must still be PRESENT, but
// its value is not range-checked, so a group member may carry `delay_ms: -5` while the
// identical standalone incident is rejected. That asymmetry is almost certainly an
// oversight, but it is observable behaviour and is preserved rather than silently
// tightened.
func (v *validator) incidentTypeFields(n *yaml.Node, incidentType, path string, strict bool) {
	switch incidentType {
	case IncidentStopBroker, IncidentStartBroker:
		broker := mapGet(n, "broker")
		if broker == nil {
			v.errf(n, join(path, "broker"),
				"Incident type '%s' requires 'broker' field", incidentType)
		} else if !Cluster.AllowsBroker(scalarText(broker)) {
			v.errf(broker, join(path, "broker"), "Invalid broker. Valid: %s", Cluster.brokerList())
		}

	case IncidentIncreaseConsumerDlay:
		delay := mapGet(n, "delay_ms")
		if delay == nil {
			v.errf(n, join(path, "delay_ms"),
				"Incident type 'increase_consumer_delay' requires 'delay_ms' field")
		} else if strict {
			if ms, ok := intValue(delay); !ok || ms < 0 {
				v.errf(delay, join(path, "delay_ms"),
					"Field 'delay_ms' must be a non-negative integer")
			}
		}

	case IncidentChangeProducerRate:
		rate := mapGet(n, "rate")
		if rate == nil {
			v.errf(n, join(path, "rate"),
				"Incident type 'change_producer_rate' requires 'rate' field")
		} else if strict {
			if r, ok := floatValue(rate); !ok || r < 0 {
				v.errf(rate, join(path, "rate"), "Field 'rate' must be a non-negative number")
			}
		}

	case IncidentPauseConsumer:
		duration := mapGet(n, "duration_seconds")
		if duration == nil {
			v.errf(n, join(path, "duration_seconds"),
				"Incident type 'pause_consumer' requires 'duration_seconds' field")
		} else if strict {
			if seconds, ok := intValue(duration); !ok || seconds < 1 {
				v.errf(duration, join(path, "duration_seconds"),
					"Field 'duration_seconds' must be a positive integer")
			}
		}
	}
}

// target validates one `target:` block. Used for both consumer and producer targets;
// producers have no group, but this does not distinguish, so a `group` on a producer
// target is accepted here and then ignored at decode.
func (v *validator) target(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "Target must be an object/dict")
		return
	}
	v.knownKeys(n, path, "topic", "group", "group_id", "percentage", "count", "indices")

	if topic := mapGet(n, "topic"); topic != nil && !isString(topic) {
		v.errf(topic, join(path, "topic"), "Field 'topic' must be a string")
	}
	// `group_id` is validated alongside `group` because decode accepts both -- see
	// rawTarget.GroupID in decode.go.
	for _, key := range []string{"group", "group_id"} {
		if group := mapGet(n, key); group != nil && !isString(group) {
			v.errf(group, join(path, key), "Field '%s' must be a string", key)
		}
	}

	if pct := mapGet(n, "percentage"); pct != nil {
		if value, ok := intValue(pct); !ok || value < 1 || value > 100 {
			v.errf(pct, join(path, "percentage"),
				"Field 'percentage' must be an integer between 1 and 100")
		}
	}
	if count := mapGet(n, "count"); count != nil {
		if value, ok := intValue(count); !ok || value < 1 {
			v.errf(count, join(path, "count"), "Field 'count' must be a positive integer")
		}
	}
	if indices := mapGet(n, "indices"); indices != nil {
		if !isSequence(indices) {
			v.errf(indices, join(path, "indices"), "Field 'indices' must be a list")
		} else {
			for _, item := range seqItems(indices) {
				if value, ok := intValue(item); !ok || value < 0 {
					v.errf(indices, join(path, "indices"),
						"Field 'indices' must be a list of non-negative integers")
					break
				}
			}
		}
	}

	// The three selection keys are mutually exclusive.
	var set []string
	for _, key := range []string{"percentage", "count", "indices"} {
		if mapGet(n, key) != nil {
			set = append(set, key)
		}
	}
	if len(set) > 1 {
		v.errf(n, path, "Only one of percentage, count, indices can be set, got: %s",
			strings.Join(set, ", "))
	}
}

// incidentGroup validates a `group:` block.
//
// repeat and interval_seconds are REQUIRED here even though decode defaults them to 1
// and 60 when missing; validation is stricter than decode and that is preserved.
func (v *validator) incidentGroup(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "Incident group must be an object/dict")
		return
	}
	v.knownKeys(n, path, "repeat", "interval_seconds", "incidents")

	switch repeat := mapGet(n, "repeat"); {
	case repeat == nil:
		v.errf(n, join(path, "repeat"), "Missing required field 'repeat'")
	default:
		if value, ok := intValue(repeat); !ok || value < 1 {
			v.errf(repeat, join(path, "repeat"), "Field 'repeat' must be a positive integer")
		}
	}

	intervalNode := mapGet(n, "interval_seconds")
	switch {
	case intervalNode == nil:
		v.errf(n, join(path, "interval_seconds"), "Missing required field 'interval_seconds'")
	default:
		if value, ok := intValue(intervalNode); !ok || value < 1 {
			v.errf(intervalNode, join(path, "interval_seconds"),
				"Field 'interval_seconds' must be a positive integer")
		}
	}

	incidents := mapGet(n, "incidents")
	switch {
	case incidents == nil:
		v.errf(n, join(path, "incidents"), "Missing required field 'incidents'")
	case !isSequence(incidents):
		v.errf(incidents, join(path, "incidents"), "Field 'incidents' must be a list")
	case len(seqItems(incidents)) == 0:
		v.errf(incidents, join(path, "incidents"), "Incident group must have at least one incident")
	default:
		for i, inc := range seqItems(incidents) {
			v.groupIncident(inc, index(join(path, "incidents"), i))
		}
	}

	// A member scheduled at or past the cycle length overlaps the next repetition --
	// warning only.
	if interval, ok := intValue(intervalNode); ok && isSequence(incidents) {
		for i, inc := range seqItems(incidents) {
			atNode := mapGet(inc, "at_seconds")
			if at, isInt := intValue(atNode); isInt && at >= interval {
				v.warnf(atNode, join(index(join(path, "incidents"), i), "at_seconds"),
					"at_seconds (%d) >= interval (%d), may overlap", at, interval)
			}
		}
	}
}

// groupIncident validates one member of a `group:` block's `incidents:` list. Differs
// from a standalone incident in three ways: no schedule is required (a missing
// at_seconds is a warning), every_seconds is not checked at all, and value checks are
// relaxed (strict=false).
func (v *validator) groupIncident(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "Incident must be an object/dict")
		return
	}
	v.knownKeys(n, path, incidentKeys...)

	incidentType, ok := v.incidentType(n, path)
	if !ok {
		return
	}

	at := mapGet(n, "at_seconds")
	if at == nil {
		v.warnf(n, path, "Group incidents should use 'at_seconds' (relative to cycle start)")
	} else if !isInt(at) {
		v.errf(at, join(path, "at_seconds"), "Field 'at_seconds' must be an integer")
	}

	if target := mapGet(n, "target"); target != nil {
		v.target(target, join(path, "target"))
	}
	v.incidentTypeFields(n, incidentType, path, false)
}
