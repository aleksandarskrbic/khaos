package generate

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

func orderFlow() scenario.Flow {
	return scenario.Flow{
		Name: "orders",
		Rate: 10,
		Steps: []scenario.FlowStep{
			{
				Topic: "orders.created", EventType: "order_created", DelayMS: 250,
				Fields: []scenario.Field{
					{Name: "order_id", Type: scenario.FieldUUID},
					{Name: "amount", Type: scenario.FieldFloat, Min: floatPtr(1), Max: floatPtr(100)},
				},
			},
			{
				Topic: "payments.processed", EventType: "payment_processed", DelayMS: 500,
				Fields: []scenario.Field{{Name: "method", Type: scenario.FieldEnum, Values: []string{"card", "sepa"}}},
			},
			{Topic: "orders.shipped", EventType: "order_shipped", DelayMS: 1000},
		},
	}
}

func mustFlowGen(t *testing.T, f scenario.Flow) *FlowGen {
	t.Helper()
	g, err := NewFlowGen(f, testRand())
	if err != nil {
		t.Fatalf("NewFlowGen: %v", err)
	}
	return g
}

// One flow instance is one pass over the steps, all sharing a correlation id
// which is also the Kafka key.
func TestFlowInstanceCorrelation(t *testing.T) {
	g := mustFlowGen(t, orderFlow())

	msgs, err := g.Instance()
	if err != nil {
		t.Fatalf("Instance: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("got %d messages, want one per step", len(msgs))
	}

	first, _ := msgs[0].Doc.Get(correlationIDKey)
	id, _ := first.(string)
	if !regexp.MustCompile(`^[0-9a-f-]{36}$`).MatchString(id) {
		t.Fatalf("correlation id %q is not a uuid", id)
	}

	wantTopics := []string{"orders.created", "payments.processed", "orders.shipped"}
	wantEvents := []string{"order_created", "payment_processed", "order_shipped"}
	for i, m := range msgs {
		if m.Topic != wantTopics[i] {
			t.Errorf("message %d topic = %q, want %q", i, m.Topic, wantTopics[i])
		}
		if m.EventType != wantEvents[i] {
			t.Errorf("message %d event type = %q, want %q", i, m.EventType, wantEvents[i])
		}
		if got, _ := m.Doc.Get(eventTypeKey); got != wantEvents[i] {
			t.Errorf("message %d event_type field = %v, want %q", i, got, wantEvents[i])
		}
		if got, _ := m.Doc.Get(correlationIDKey); got != id {
			t.Errorf("message %d correlation_id = %v, want %q", i, got, id)
		}
		// The key is the correlation id, so every event of an instance lands
		// on one partition per topic.
		if string(m.Key) != id {
			t.Errorf("message %d key = %q, want the correlation id %q", i, m.Key, id)
		}
	}

	// A second instance gets its own id.
	other, err := g.Instance()
	if err != nil {
		t.Fatalf("Instance: %v", err)
	}
	if got, _ := other[0].Doc.Get(correlationIDKey); got == id {
		t.Error("two instances shared a correlation id")
	}
}

// A step's delay only applies when the step index is > 0, so step 0's
// configured delay_ms is dead config.
func TestFlowFirstStepDelayIsDropped(t *testing.T) {
	msgs, err := mustFlowGen(t, orderFlow()).Instance()
	if err != nil {
		t.Fatalf("Instance: %v", err)
	}
	want := []int{0, 500, 1000}
	for i, m := range msgs {
		if m.DelayMS != want[i] {
			t.Errorf("message %d delay = %d, want %d", i, m.DelayMS, want[i])
		}
	}
}

// field_ref takes the correlation id out of the first step's document, and
// the first step is re-serialised with it.
func TestFlowFieldRefCorrelation(t *testing.T) {
	tests := []struct {
		name        string
		correlation scenario.Correlation
		refField    string
		wantFromDoc bool
	}{
		{
			name:        "field_ref uses the named field",
			correlation: scenario.Correlation{Type: scenario.CorrelationFieldRef, Field: "order_id"},
			refField:    "order_id",
			wantFromDoc: true,
		},
		{
			name:        "field_ref to a missing field falls back to uuid4",
			correlation: scenario.Correlation{Type: scenario.CorrelationFieldRef, Field: "nope"},
			wantFromDoc: false,
		},
		{
			name:        "field_ref with no field name falls back to uuid4",
			correlation: scenario.Correlation{Type: scenario.CorrelationFieldRef},
			wantFromDoc: false,
		},
		{
			name:        "uuid ignores the field name",
			correlation: scenario.Correlation{Type: scenario.CorrelationUUID, Field: "order_id"},
			refField:    "order_id",
			wantFromDoc: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flow := orderFlow()
			flow.Correlation = tt.correlation
			msgs, err := mustFlowGen(t, flow).Instance()
			if err != nil {
				t.Fatalf("Instance: %v", err)
			}

			got, _ := msgs[0].Doc.Get(correlationIDKey)
			id, _ := got.(string)
			if id == "" {
				t.Fatal("correlation id is empty")
			}
			if tt.refField != "" {
				ref, _ := msgs[0].Doc.Get(tt.refField)
				if tt.wantFromDoc && id != ref {
					t.Errorf("correlation id = %q, want the %s value %v", id, tt.refField, ref)
				}
				if !tt.wantFromDoc && id == ref {
					t.Errorf("correlation id = %q, want a fresh uuid rather than the %s value", id, tt.refField)
				}
			}
			for i, m := range msgs {
				if v, _ := m.Doc.Get(correlationIDKey); v != id {
					t.Errorf("step %d correlation_id = %v, want %q", i, v, id)
				}
			}
		})
	}
}

// The injected keys lead the document and correlation_id keeps the position
// it was first inserted at, even though it is rewritten afterwards.
func TestFlowMessageJSONKeyOrder(t *testing.T) {
	msgs, err := mustFlowGen(t, orderFlow()).Instance()
	if err != nil {
		t.Fatalf("Instance: %v", err)
	}

	raw, err := json.Marshal(msgs[0].Doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(raw)
	if !strings.HasPrefix(got, `{"correlation_id":"`) {
		t.Errorf("payload = %s, want correlation_id first", got)
	}
	for _, want := range []string{`"correlation_id"`, `"event_type"`, `"order_id"`, `"amount"`} {
		if !strings.Contains(got, want) {
			t.Fatalf("payload %s is missing %s", got, want)
		}
	}
	if strings.Index(got, `"event_type"`) > strings.Index(got, `"order_id"`) {
		t.Errorf("payload = %s, want event_type before the step's own fields", got)
	}

	// A step with no fields carries only the injected keys.
	bare, err := json.Marshal(msgs[2].Doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if n := msgs[2].Doc.Len(); n != 2 {
		t.Errorf("fieldless step produced %d keys (%s), want 2", n, bare)
	}
}

// A step field named correlation_id or event_type overwrites the injected
// value, because step fields are written into the document afterwards.
func TestFlowStepFieldsOverwriteInjectedKeys(t *testing.T) {
	flow := scenario.Flow{
		Name: "clash",
		Steps: []scenario.FlowStep{{
			Topic: "t", EventType: "declared",
			Fields: []scenario.Field{
				{Name: "event_type", Type: scenario.FieldEnum, Values: []string{"overridden"}},
			},
		}},
	}
	msgs, err := mustFlowGen(t, flow).Instance()
	if err != nil {
		t.Fatalf("Instance: %v", err)
	}
	if got, _ := msgs[0].Doc.Get(eventTypeKey); got != "overridden" {
		t.Errorf("event_type = %v, want the step field to win", got)
	}
	if msgs[0].EventType != "declared" {
		t.Errorf("FlowMessage.EventType = %q, want the declared step event type", msgs[0].EventType)
	}
}

// Per-step generators are created once and shared by every instance, so a
// step's cardinality cache spans the whole run.
func TestFlowSharesStepGeneratorsAcrossInstances(t *testing.T) {
	flow := scenario.Flow{
		Name: "cardinality",
		Steps: []scenario.FlowStep{{
			Topic: "t", EventType: "e",
			Fields: []scenario.Field{{Name: "region", Type: scenario.FieldString, Cardinality: intPtr(2)}},
		}},
	}
	g := mustFlowGen(t, flow)

	seen := map[any]bool{}
	for range 25 {
		msgs, err := g.Instance()
		if err != nil {
			t.Fatalf("Instance: %v", err)
		}
		v, _ := msgs[0].Doc.Get("region")
		seen[v] = true
	}
	if len(seen) != 2 {
		t.Errorf("saw %d distinct regions over 25 instances, want 2 from the shared cache", len(seen))
	}
}

func TestNewFlowGenErrors(t *testing.T) {
	tests := []struct {
		name    string
		flow    scenario.Flow
		wantSub string
	}{
		{
			name:    "no steps",
			flow:    scenario.Flow{Name: "empty"},
			wantSub: "has no steps",
		},
		{
			name: "bad field in a step",
			flow: scenario.Flow{Name: "bad", Steps: []scenario.FlowStep{{
				Topic: "t", Fields: []scenario.Field{{Name: "x", Type: "nope"}},
			}}},
			wantSub: "unknown field type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewFlowGen(tt.flow, testRand())
			if err == nil {
				t.Fatal("want error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantSub)
			}
		})
	}
}

func TestFlowSeededReproducibility(t *testing.T) {
	run := func() []string {
		g := mustFlowGen(t, orderFlow())
		var out []string
		for range 5 {
			msgs, err := g.Instance()
			if err != nil {
				t.Fatalf("Instance: %v", err)
			}
			for _, m := range msgs {
				raw, err := json.Marshal(m.Doc)
				if err != nil {
					t.Fatalf("marshal: %v", err)
				}
				out = append(out, string(raw))
			}
		}
		return out
	}

	a, b := run(), run()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("message %d differs between equally seeded runs:\n %s\n %s", i, a[i], b[i])
		}
	}
}

// The referenced value is stringified before it becomes the key.
func TestCompatStr(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{name: "string", in: "abc", want: "abc"},
		{name: "true", in: true, want: "True"},
		{name: "false", in: false, want: "False"},
		{name: "int64", in: int64(-12), want: "-12"},
		{name: "float keeps a decimal point", in: 1.0, want: "1.0"},
		{name: "float", in: 3.25, want: "3.25"},
		{name: "array falls back to json", in: []any{1, 2}, want: "[1,2]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compatStr(tt.in); got != tt.want {
				t.Errorf("compatStr(%v) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// An empty correlation id means no key at all.
func TestFlowEmptyCorrelationIDProducesNilKey(t *testing.T) {
	flow := scenario.Flow{
		Name:        "empty-ref",
		Correlation: scenario.Correlation{Type: scenario.CorrelationFieldRef, Field: "blank"},
		Steps: []scenario.FlowStep{{
			Topic: "t", EventType: "e",
			Fields: []scenario.Field{{Name: "blank", Type: scenario.FieldEnum, Values: []string{""}}},
		}},
	}
	msgs, err := mustFlowGen(t, flow).Instance()
	if err != nil {
		t.Fatalf("Instance: %v", err)
	}
	if msgs[0].Key != nil {
		t.Errorf("key = %q, want nil for an empty correlation id", msgs[0].Key)
	}
}
