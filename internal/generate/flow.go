package generate

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// Keys injected into every flow message. They are set before the step's own
// fields, so they lead the document -- and a step field named
// "correlation_id" or "event_type" overwrites the injected value in place.
const (
	correlationIDKey = "correlation_id"
	eventTypeKey     = "event_type"
)

// FlowMessage is one step's message within one flow instance.
//
// Doc is always serialised as plain JSON by the caller: flows ignore
// data_format entirely, so an Avro or Protobuf flow still emits JSON.
type FlowMessage struct {
	// Topic is the step's target topic.
	Topic string
	// Key is the correlation id. Every step of an instance shares it, so all
	// events of one instance land on one partition per topic.
	Key []byte
	// Doc is the message body, correlation_id and event_type first.
	Doc *Doc
	// DelayMS is how long to wait BEFORE producing this message. It is always 0
	// for the first message of an instance: a step's delay only applies when
	// the step index is greater than zero, so step 0's configured delay_ms is
	// dead config.
	DelayMS int
	// EventType is the step's event_type, duplicated out of Doc so the engine
	// can label metrics without reaching into the document.
	EventType string
}

// FlowGen produces correlated multi-step message sequences for one flow.
// Producing, rate limiting and stats stay with the engine.
//
// Per-step field generators are built once, in NewFlowGen, and shared by
// every instance. That sharing is load-bearing: a step field with
// `cardinality: 100` draws from one 100-value cache for the whole run, not a
// fresh cache per instance. Not safe for concurrent use.
type FlowGen struct {
	steps       []scenario.FlowStep
	gens        []*DocGen
	correlation scenario.Correlation
	r           *rand.Rand
}

// NewFlowGen builds the generator for a flow.
//
// Options apply to every step's fields, at every depth, so a flow step
// carrying an impossible cardinality honours the caller's BoundFillAttempts
// instead of hanging.
//
// The returned generator is not safe for concurrent use: it owns the
// *rand.Rand and the per-step cardinality caches, and calling Instance from a
// worker pool has produced a real race-detector-caught race in the engine's
// flow runner. Generate on one goroutine and hand the messages to workers.
func NewFlowGen(f scenario.Flow, r *rand.Rand, opts ...Option) (*FlowGen, error) {
	if len(f.Steps) == 0 {
		return nil, fmt.Errorf("flow %q has no steps", f.Name)
	}
	o := resolve(opts)
	g := &FlowGen{
		steps:       f.Steps,
		gens:        make([]*DocGen, len(f.Steps)),
		correlation: f.Correlation,
		r:           r,
	}
	for i, step := range f.Steps {
		// A step with no fields is legal and yields {correlation_id, event_type}
		// only.
		dg, err := newDocGen(step.Fields, r, o)
		if err != nil {
			return nil, fmt.Errorf("flow %q step %d (%s): %w", f.Name, i, step.Topic, err)
		}
		g.gens[i] = dg
	}
	return g, nil
}

// Instance generates one flow instance: one message per step, in step order,
// all sharing a correlation id.
//
// The correlation id comes from Correlation.Type:
//
//	"uuid"      -> a fresh uuid4
//	"field_ref" -> the value of the named field in the FIRST step's document,
//	               stringified; if the field is missing, a uuid4 instead
//
// Step 0 is generated with an empty correlation_id first and rewritten once
// the id is known; because Doc.Set keeps a key's original position,
// correlation_id stays the leading key.
//
// The error return is part of the API contract for future validation-at-runtime
// needs; generation itself cannot currently fail, since every failure mode is
// caught in NewFlowGen.
func (g *FlowGen) Instance() ([]FlowMessage, error) {
	msgs := make([]FlowMessage, len(g.steps))
	var correlationID string

	for i, step := range g.steps {
		doc := g.gens[i].Next()

		// Rebuild with the injected keys leading, before the step's fields.
		body := NewDoc(doc.Len() + 2)
		body.Set(correlationIDKey, "")
		body.Set(eventTypeKey, step.EventType)
		for _, name := range doc.Keys() {
			v, _ := doc.Get(name)
			body.Set(name, v)
		}

		if i == 0 {
			correlationID = g.correlationID(body)
		}
		body.Set(correlationIDKey, correlationID)

		delay := step.DelayMS
		if i == 0 {
			delay = 0
		}
		// An empty correlation id -- reachable when field_ref points at a
		// field that generated an empty string -- means no key at all, which
		// sends the message to a partition of the broker's choosing.
		var key []byte
		if correlationID != "" {
			key = []byte(correlationID)
		}
		msgs[i] = FlowMessage{
			Topic:     step.Topic,
			Key:       key,
			Doc:       body,
			DelayMS:   delay,
			EventType: step.EventType,
		}
	}
	return msgs, nil
}

// Steps returns the flow's steps, so the engine can set up producers and
// consumers without re-reading the scenario.
func (g *FlowGen) Steps() []scenario.FlowStep { return g.steps }

// correlationID resolves the correlation id for a flow instance from the
// first step's document.
//
// A field_ref naming a field the first step does not produce falls back to a
// fresh uuid4 rather than failing; scenarios rely on the flow still running.
func (g *FlowGen) correlationID(firstStep *Doc) string {
	if g.correlation.Type == scenario.CorrelationFieldRef && g.correlation.Field != "" {
		if v, ok := firstStep.Get(g.correlation.Field); ok {
			return compatStr(v)
		}
	}
	return uuid4(g.r)
}

// compatStr stringifies a generated value for use as a correlation id and
// Kafka key: strings pass through, booleans render as "True"/"False", numbers
// use their canonical decimal form, and anything else falls back to JSON.
// The exact rules -- capitalised booleans, floats always carrying a decimal
// point -- are kept for compatibility with correlation ids produced by
// earlier khaos releases, so mixed-version data still joins on the same keys.
//
// Only the types this package generates are handled; the common case by far
// is a uuid or string field.
func compatStr(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		// Rendered capitalised, "True"/"False", not lowercase.
		if t {
			return "True"
		}
		return "False"
	case int64:
		return strconv.FormatInt(t, 10)
	case int:
		return strconv.Itoa(t)
	case float64:
		return compatFloatStr(t)
	default:
		// Nested objects and arrays fall back to JSON, the closest useful and
		// parseable rendering. Referencing a composite field as the
		// correlation id is a scenario bug either way.
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}

// compatFloatStr renders f as the shortest decimal representation that
// round-trips, always carrying a decimal point (1.0 renders as "1.0", never
// "1").
func compatFloatStr(f float64) string {
	if math.IsInf(f, 1) {
		return "inf"
	}
	if math.IsInf(f, -1) {
		return "-inf"
	}
	if math.IsNaN(f) {
		return "nan"
	}
	s := strconv.FormatFloat(f, 'g', -1, 64)
	for _, c := range s {
		if c == '.' || c == 'e' || c == 'E' {
			return s
		}
	}
	return s + ".0"
}
