package scenario

import (
	"fmt"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"
)

// This file holds the validator and its shared helpers plus the top-level document
// walk; the regions it dispatches to live beside it: validate_topic.go (topics and
// their configs), validate_field.go (field recursion), validate_flow.go (flows and
// steps) and validate_incident.go (incidents and groups).
//
// Validation runs against the raw yaml.Node tree, before any typed decoding, so a scenario
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
		IncidentRebalanceConsumer, IncidentIncreaseConsumerDelay, IncidentChangeProducerRate,
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

	// At least one of topics/flows. Both a missing key and a key whose value is not
	// a non-empty list count as absent here, which is why a bare `topics:` produces
	// this error as well as the "must be a list" one below.
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
