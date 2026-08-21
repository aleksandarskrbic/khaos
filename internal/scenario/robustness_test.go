package scenario

import (
	"strings"
	"testing"
	"time"
)

// Decode must always terminate with diagnostics, never with a panic and never at all.
//
// A scenario file is untrusted input -- `khaos validate` is pointed at whatever the
// user wrote -- so every shape below has to come back as findings. The recursive-anchor
// case in particular used to spin forever at 100% CPU with the diagnostic path growing
// by ".items" per level until the process was killed.
func TestHostileDocumentsTerminate(t *testing.T) {
	tests := []struct {
		name    string
		doc     string
		wantSub string // a substring that must appear in some diagnostic; "" means any
	}{
		{
			name: "array items refer to the field through an anchor",
			doc: "name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n" +
				"        - &f\n          name: a\n          type: array\n          items: *f\n",
			wantSub: "refers to itself through a YAML anchor",
		},
		{
			name: "object fields refer to the field through an anchor",
			doc: "name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n" +
				"        - &f\n          name: a\n          type: object\n          fields:\n            - *f\n",
			wantSub: "refers to itself through a YAML anchor",
		},
		{
			// An alias used as a SIBLING rather than an ancestor is legitimate reuse and
			// must not be mistaken for a cycle.
			name: "sibling alias is not a cycle",
			doc: "name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n" +
				"        - &f\n          name: a\n          type: int\n" +
				"        - name: b\n          type: array\n          items: *f\n",
			wantSub: "",
		},
		{name: "empty document", doc: "", wantSub: "Scenario must be a YAML object/dict"},
		{name: "comment only", doc: "# nothing here\n", wantSub: "Scenario must be a YAML object/dict"},
		{name: "scalar root", doc: "just a string\n", wantSub: "Scenario must be a YAML object/dict"},
		{name: "sequence root", doc: "- a\n- b\n", wantSub: "Scenario must be a YAML object/dict"},
		{name: "syntax error", doc: "name: [unclosed\n", wantSub: "Invalid YAML syntax"},
		{
			name:    "every top-level key is null",
			doc:     "name:\ntopics:\nflows:\nincidents:\nschema_registry:\n",
			wantSub: "Scenario must have at least 'topics' or 'flows'",
		},
		{
			name:    "integer too large for int64",
			doc:     "name: s\ntopics:\n  - name: t\n    partitions: 99999999999999999999999\n",
			wantSub: "Field 'partitions' must be a positive integer",
		},
		{
			// NaN bounds pass validation because every comparison against NaN is false.
			// The generator is where they are caught.
			name: "NaN numeric bounds validate clean",
			doc: "name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n" +
				"        - name: a\n          type: int\n          min: .nan\n          max: .nan\n",
			wantSub: "",
		},
		{
			name:    "non-scalar mapping key",
			doc:     "name: s\n? [1, 2]\n: value\ntopics:\n  - name: t\n",
			wantSub: "",
		},
		{
			name:    "duplicate top-level key",
			doc:     "name: a\nname: b\ntopics:\n  - name: t\n",
			wantSub: "",
		},
		{
			name:    "deeply but finitely nested objects",
			doc:     deeplyNestedObjects(500),
			wantSub: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Guarding with a timer rather than trusting the test binary's global
			// deadline: a hang here should name the case that hung.
			done := make(chan Diagnostics, 1)
			go func() {
				_, diags := Decode([]byte(tt.doc))
				done <- diags
			}()

			var diags Diagnostics
			select {
			case diags = <-done:
			case <-time.After(20 * time.Second):
				t.Fatal("Decode did not terminate")
			}

			if tt.wantSub == "" {
				return
			}
			for _, d := range diags {
				if strings.Contains(d.Message, tt.wantSub) {
					return
				}
			}
			t.Errorf("no diagnostic contains %q; got %v", tt.wantSub, diags)
		})
	}
}

// DecodeStrict walks the same tree with unknown-key reporting on, so it needs the same
// termination guarantee.
func TestHostileDocumentsTerminateUnderStrict(t *testing.T) {
	doc := "name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n" +
		"        - &f\n          name: a\n          type: array\n          items: *f\n"

	done := make(chan struct{})
	go func() {
		defer close(done)
		DecodeStrict([]byte(doc))
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("DecodeStrict did not terminate")
	}
}

func deeplyNestedObjects(depth int) string {
	var b strings.Builder
	b.WriteString("name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n")
	indent := "        "
	for range depth {
		b.WriteString(indent + "- name: o\n" + indent + "  type: object\n" + indent + "  fields:\n")
		indent += "  "
	}
	b.WriteString(indent + "- name: leaf\n" + indent + "  type: int\n")
	return b.String()
}

// FuzzDecode asserts the two invariants that hold for ANY input: Decode never panics,
// and it returns a Scenario exactly when it reports no error.
func FuzzDecode(f *testing.F) {
	f.Add("name: s\ntopics:\n  - name: t\n")
	f.Add("name: s\nflows:\n  - name: f\n    steps:\n      - topic: a\n        event_type: e\n")
	f.Add("name: s\ntopics:\n  - name: t\nincidents:\n  - type: stop_broker\n    at_seconds: 5\n    broker: kafka-1\n")
	f.Add("name: s\ntopics:\n  - name: t\n    message_schema:\n      fields:\n        - name: a\n          type: array\n          items:\n            type: int\n")
	f.Add("name: s\ntopics: &t\n  <<: *t\n")
	f.Add("")
	f.Add("- - - -")
	f.Add("{}")

	f.Fuzz(func(t *testing.T, doc string) {
		sc, diags := Decode([]byte(doc))
		if diags.HasErrors() && sc != nil {
			t.Fatalf("errors reported but a Scenario was returned: %v", diags)
		}
		if !diags.HasErrors() && sc == nil {
			t.Fatalf("no errors reported but no Scenario was returned: %v", diags)
		}
	})
}
