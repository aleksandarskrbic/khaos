package scenario

import (
	"fmt"

	"go.yaml.in/yaml/v3"
)

// Decode turns a scenario document into a Scenario.
//
// It is a two-pass operation and the order matters: pass one validates the raw
// yaml.Node tree and collects every problem it can find (validate.go), and pass two
// decodes into the typed structs, running only when pass one found no errors. This way
// an author fixing a file gets the whole list of problems at once instead of one error
// per run.
//
// The returned Scenario is nil when the diagnostics contain any error. Warnings are
// returned alongside a usable Scenario.
func Decode(data []byte) (*Scenario, Diagnostics) { return decode(data, false) }

// DecodeStrict is Decode plus unknown-key rejection.
//
// Strictness is opt-in: scenarios/serialization/protobuf-example.yaml carries a
// `pattern:` key on a faker field that no model reads, so turning this on by default
// would reject a working scenario.
func DecodeStrict(data []byte) (*Scenario, Diagnostics) { return decode(data, true) }

func decode(data []byte, strict bool) (*Scenario, Diagnostics) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		// A syntax error is reported as a diagnostic like any other, not as a
		// separate failure mode.
		return nil, Diagnostics{{
			Path:     "yaml",
			Message:  fmt.Sprintf("Invalid YAML syntax: %v", err),
			Severity: SeverityError,
		}}
	}

	root := resolveNode(&doc)
	v := &validator{strict: strict}
	v.scenario(root)
	if v.diags.HasErrors() {
		return nil, v.diags
	}

	sc, diags := decodeValidated(root)
	return sc, append(v.diags, diags...)
}

// decodeValidated performs pass two. Every failure it can still hit is a type mismatch
// the validator does not cover -- most of them behind the relaxed value checks group
// members get -- so they are reported as diagnostics rather than returned as errors.
func decodeValidated(root *yaml.Node) (*Scenario, Diagnostics) {
	var raw rawScenario
	if err := root.Decode(&raw); err != nil {
		return nil, Diagnostics{{
			Path:     "",
			Message:  fmt.Sprintf("Cannot decode scenario: %v", err),
			Severity: SeverityError,
			Line:     nodeLine(root),
		}}
	}

	sc := &Scenario{
		Name:           raw.Name,
		Description:    raw.Description,
		Topics:         raw.Topics,
		Flows:          raw.Flows,
		SchemaRegistry: raw.SchemaRegistry,
	}

	var diags Diagnostics
	for i, n := range seqItems(mapGet(root, "incidents")) {
		path := index("incidents", i)
		// One heterogeneous list: an entry carrying `group` becomes an IncidentGroup,
		// everything else an Incident.
		if group := mapGet(n, "group"); group != nil {
			g, err := decodeIncidentGroup(group, join(path, "group"))
			if err != nil {
				diags = append(diags, decodeError(group, join(path, "group"), err))
				continue
			}
			sc.IncidentGroups = append(sc.IncidentGroups, g)
			continue
		}
		inc, err := decodeIncident(n)
		if err != nil {
			diags = append(diags, decodeError(n, path, err))
			continue
		}
		sc.Incidents = append(sc.Incidents, inc)
	}
	if diags.HasErrors() {
		return nil, diags
	}
	return sc, diags
}

func decodeError(n *yaml.Node, path string, err error) Diagnostic {
	return Diagnostic{
		Path:     path,
		Message:  err.Error(),
		Severity: SeverityError,
		Line:     nodeLine(n),
	}
}

// rawScenario is the document shape minus `incidents`.
//
// Scenario.Incidents and Scenario.IncidentGroups carry no yaml tags since they are
// derived from one heterogeneous list rather than decoded directly; routing the rest of
// the fields through this type keeps them out of reach of yaml's default field
// matching, which would otherwise try to unmarshal `incidents:` into a []Incident of
// interfaces.
type rawScenario struct {
	Name           string          `yaml:"name"`
	Description    string          `yaml:"description"`
	Topics         []Topic         `yaml:"topics"`
	Flows          []Flow          `yaml:"flows"`
	SchemaRegistry *SchemaRegistry `yaml:"schema_registry"`
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------
//
// Every type below needs an UnmarshalYAML because Go's zero value and khaos's default
// value disagree: an omitted `partitions:` must become 12, not 0. The `type raw T` alias
// makes this safe -- raw has no methods, so node.Decode fills the pre-seeded struct
// instead of calling this method again, and nested defaults apply at every depth.

// UnmarshalYAML fills omitted keys with the topic defaults.
func (t *Topic) UnmarshalYAML(node *yaml.Node) error {
	type raw Topic
	d := raw(defaultTopic())
	if err := node.Decode(&d); err != nil {
		return err
	}
	*t = Topic(d)
	return nil
}

// UnmarshalYAML fills omitted keys with the message schema defaults.
func (m *MessageSchema) UnmarshalYAML(node *yaml.Node) error {
	type raw MessageSchema
	d := raw(defaultMessageSchema())
	if err := node.Decode(&d); err != nil {
		return err
	}
	*m = MessageSchema(d)
	return nil
}

// UnmarshalYAML fills omitted keys with the producer defaults.
func (p *ProducerConf) UnmarshalYAML(node *yaml.Node) error {
	type raw ProducerConf
	d := raw(defaultProducerConf())
	if err := node.Decode(&d); err != nil {
		return err
	}
	*p = ProducerConf(d)
	return nil
}

// UnmarshalYAML fills omitted keys with the consumer defaults.
func (c *ConsumerConf) UnmarshalYAML(node *yaml.Node) error {
	type raw ConsumerConf
	d := raw(defaultConsumerConf())
	if err := node.Decode(&d); err != nil {
		return err
	}
	*c = ConsumerConf(d)
	return nil
}

// UnmarshalYAML fills omitted keys with the field defaults. Recursion into nested
// fields and array items happens through the ordinary decoder, so min_items/max_items
// default at every depth.
func (f *Field) UnmarshalYAML(node *yaml.Node) error {
	type raw Field
	d := raw(defaultField())
	if err := node.Decode(&d); err != nil {
		return err
	}
	*f = Field(d)
	return nil
}

// UnmarshalYAML fills omitted keys with the flow defaults -- notably rate 10.0 and a
// uuid correlation for a flow that declares neither.
func (f *Flow) UnmarshalYAML(node *yaml.Node) error {
	type raw Flow
	d := raw(defaultFlow())
	if err := node.Decode(&d); err != nil {
		return err
	}
	*f = Flow(d)
	return nil
}

// UnmarshalYAML fills omitted keys with the step-consumer defaults.
func (s *StepConsumer) UnmarshalYAML(node *yaml.Node) error {
	type raw StepConsumer
	d := raw(defaultStepConsumer())
	if err := node.Decode(&d); err != nil {
		return err
	}
	*s = StepConsumer(d)
	return nil
}

// UnmarshalYAML defaults the correlation type to uuid.
func (c *Correlation) UnmarshalYAML(node *yaml.Node) error {
	type raw Correlation
	d := raw(Correlation{Type: defaultCorrelationType})
	if err := node.Decode(&d); err != nil {
		return err
	}
	*c = Correlation(d)
	return nil
}

// ---------------------------------------------------------------------------
// Incidents
// ---------------------------------------------------------------------------

// rawIncident is the YAML shape of one incident.
//
// The schedule keys sit at the same flat level as the incident's own parameters rather
// than in a nested block.
type rawIncident struct {
	Type string `yaml:"type"`

	AtSeconds           *int `yaml:"at_seconds"`
	EverySeconds        *int `yaml:"every_seconds"`
	InitialDelaySeconds int  `yaml:"initial_delay_seconds"`

	Broker          string  `yaml:"broker"`
	DurationSeconds int     `yaml:"duration_seconds"`
	DelayMS         int     `yaml:"delay_ms"`
	Rate            float64 `yaml:"rate"`

	Target *rawTarget `yaml:"target"`
}

func (r rawIncident) schedule() Schedule {
	return Schedule{
		AtSeconds:           r.AtSeconds,
		EverySeconds:        r.EverySeconds,
		InitialDelaySeconds: r.InitialDelaySeconds,
	}
}

// rawTarget is the nested `target:` block, shared by consumer and producer incidents.
type rawTarget struct {
	Topic string `yaml:"topic"`
	Group string `yaml:"group"`
	// GroupID accepts the `group_id:` spelling as an alias for `group`, since the
	// bundled scenario at scenarios/chaos/targeted-incidents.yaml:36 uses that key and
	// would otherwise silently widen to every consumer.
	GroupID    string `yaml:"group_id"`
	Percentage *int   `yaml:"percentage"`
	Count      *int   `yaml:"count"`
	Indices    []int  `yaml:"indices"`
}

// consumer builds a ConsumerTarget. A nil rawTarget means "no target", which selects
// every consumer.
func (t *rawTarget) consumer() ConsumerTarget {
	if t == nil {
		return ConsumerTarget{}
	}
	group := t.Group
	if group == "" {
		group = t.GroupID
	}
	return ConsumerTarget{
		Topic:      t.Topic,
		Group:      group,
		Percentage: t.Percentage,
		Count:      t.Count,
		Indices:    t.Indices,
	}
}

// producer builds a ProducerTarget. Producers have no group, so `group`/`group_id` on a
// producer target are accepted and ignored.
func (t *rawTarget) producer() ProducerTarget {
	if t == nil {
		return ProducerTarget{}
	}
	return ProducerTarget{
		Topic:      t.Topic,
		Percentage: t.Percentage,
		Count:      t.Count,
		Indices:    t.Indices,
	}
}

// decodeIncident dispatches on `type` over the six incident kinds.
//
// An unrecognised type is a hard error, though validation rejects it first, so this
// only fires for a document decoded by hand.
func decodeIncident(n *yaml.Node) (Incident, error) {
	var r rawIncident
	if err := n.Decode(&r); err != nil {
		return nil, fmt.Errorf("cannot decode incident: %w", err)
	}
	sched := r.schedule()

	switch r.Type {
	case IncidentStopBroker:
		return StopBrokerIncident{Broker: r.Broker, Schedule: sched}, nil
	case IncidentStartBroker:
		return StartBrokerIncident{Broker: r.Broker, Schedule: sched}, nil
	case IncidentPauseConsumer:
		return PauseConsumers{
			DurationSeconds: r.DurationSeconds,
			Target:          r.Target.consumer(),
			Schedule:        sched,
		}, nil
	case IncidentRebalanceConsumer:
		return RebalanceConsumer{Target: r.Target.consumer(), Schedule: sched}, nil
	case IncidentIncreaseConsumerDlay:
		return IncreaseConsumerDelay{
			DelayMS:  r.DelayMS,
			Target:   r.Target.consumer(),
			Schedule: sched,
		}, nil
	case IncidentChangeProducerRate:
		return ChangeProducerRate{
			Rate:     r.Rate,
			Target:   r.Target.producer(),
			Schedule: sched,
		}, nil
	default:
		return nil, fmt.Errorf("unknown incident type %q", r.Type)
	}
}

// decodeIncidentGroup builds one IncidentGroup from a `group:` block.
//
// repeat and interval_seconds fall back to 1 and 60 even though validation requires
// both to be present -- the defaults are only reachable through a hand-built document.
func decodeIncidentGroup(n *yaml.Node, path string) (IncidentGroup, error) {
	var raw rawGroup
	if err := n.Decode(&raw); err != nil {
		return IncidentGroup{}, fmt.Errorf("cannot decode incident group: %w", err)
	}
	g := IncidentGroup{Repeat: raw.Repeat, IntervalSeconds: raw.IntervalSeconds}
	for i, member := range seqItems(mapGet(n, "incidents")) {
		inc, err := decodeIncident(member)
		if err != nil {
			return IncidentGroup{}, fmt.Errorf("%s: %w", index(join(path, "incidents"), i), err)
		}
		g.Incidents = append(g.Incidents, inc)
	}
	return g, nil
}

// rawGroup carries only the scalar group settings; members are decoded from the node so
// that a failing member can be reported with its own index.
type rawGroup struct {
	Repeat          int `yaml:"repeat"`
	IntervalSeconds int `yaml:"interval_seconds"`
}

// UnmarshalYAML applies the parser's group defaults.
func (g *rawGroup) UnmarshalYAML(node *yaml.Node) error {
	type raw rawGroup
	d := raw{Repeat: defaultGroupRepeat, IntervalSeconds: defaultGroupInterval}
	if err := node.Decode(&d); err != nil {
		return err
	}
	*g = rawGroup(d)
	return nil
}
