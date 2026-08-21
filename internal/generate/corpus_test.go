package generate

import (
	"encoding/json"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// Every shipped scenario must be generatable, not merely validatable.
//
// internal/scenario's corpus test proves the bundled files validate clean; this proves
// the other half, that the generators this package builds from those same files
// actually construct and produce JSON. The two halves are what stops a validation rule
// and a generator from drifting apart -- a field the validator accepts and the
// generator rejects is a scenario that passes `khaos validate` and then dies at run
// start.
func TestBundledScenariosAreGeneratable(t *testing.T) {
	available, err := scenario.Discover()
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(available) == 0 {
		t.Fatal("no scenarios discovered")
	}

	names := make([]string, 0, len(available))
	for name := range available {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			sc, diags, err := scenario.Load(available[name])
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if sc == nil {
				t.Fatalf("scenario did not decode: %v", diags)
			}

			// A shared source, exactly as one producer goroutine would use it.
			r := rand.New(rand.NewPCG(7, 11))

			for _, topic := range sc.Topics {
				keys, err := NewKeyGen(topic.MessageSchema, r)
				if err != nil {
					t.Fatalf("topic %q: NewKeyGen: %v", topic.Name, err)
				}
				if len(keys()) == 0 {
					t.Errorf("topic %q: empty key", topic.Name)
				}

				if len(topic.MessageSchema.Fields) == 0 {
					// Registry-backed topics have no inline fields either; the synthetic
					// payload generator is what runs for both.
					if len(NewRawJSONGen(topic.MessageSchema, r).Next()) == 0 {
						t.Errorf("topic %q: empty payload", topic.Name)
					}
					continue
				}
				docs, err := NewDocGen(topic.MessageSchema.Fields, r, BoundFillAttempts(4096))
				if err != nil {
					t.Fatalf("topic %q: NewDocGen: %v", topic.Name, err)
				}
				assertGeneratesJSON(t, "topic "+topic.Name, func() (any, error) { return docs.Next(), nil })
			}

			for _, flow := range sc.Flows {
				gen, err := NewFlowGen(flow, r, BoundFillAttempts(4096))
				if err != nil {
					t.Fatalf("flow %q: NewFlowGen: %v", flow.Name, err)
				}
				for range 3 {
					msgs, err := gen.Instance()
					if err != nil {
						t.Fatalf("flow %q: Instance: %v", flow.Name, err)
					}
					if len(msgs) != len(flow.Steps) {
						t.Fatalf("flow %q: %d messages for %d steps", flow.Name, len(msgs), len(flow.Steps))
					}
					for i, m := range msgs {
						if i == 0 && m.DelayMS != 0 {
							t.Errorf("flow %q: first step carries delay %d; step 0 must drop it",
								flow.Name, m.DelayMS)
						}
						assertGeneratesJSON(t, "flow "+flow.Name, func() (any, error) { return m.Doc, nil })
					}
				}
			}
		})
	}
}

func assertGeneratesJSON(t *testing.T, what string, next func() (any, error)) {
	t.Helper()
	for range 5 {
		v, err := next()
		if err != nil {
			t.Fatalf("%s: generate: %v", what, err)
		}
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("%s: marshal: %v", what, err)
		}
		if len(b) == 0 {
			t.Fatalf("%s: empty document", what)
		}
	}
}
