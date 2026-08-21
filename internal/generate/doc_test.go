package generate

import (
	"encoding/json"
	"reflect"
	"testing"
)

// Insertion order is the whole reason Doc exists: a map[string]any would
// serialise these keys alphabetically and silently reorder every message.
func TestDocPreservesInsertionOrder(t *testing.T) {
	tests := []struct {
		name string
		keys []string
		want string
	}{
		{
			name: "declaration order beats alphabetical",
			keys: []string{"zeta", "alpha", "mid"},
			want: `{"zeta":1,"alpha":1,"mid":1}`,
		},
		{
			name: "single key",
			keys: []string{"only"},
			want: `{"only":1}`,
		},
		{
			name: "empty document",
			keys: nil,
			want: `{}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDoc(len(tt.keys))
			for _, k := range tt.keys {
				d.Set(k, 1)
			}
			got, err := d.MarshalJSON()
			if err != nil {
				t.Fatalf("MarshalJSON: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("MarshalJSON = %s, want %s", got, tt.want)
			}
			if !reflect.DeepEqual(d.Keys(), tt.keys) && len(tt.keys) > 0 {
				t.Errorf("Keys = %v, want %v", d.Keys(), tt.keys)
			}
		})
	}
}

// Flow generation inserts correlation_id first with a placeholder and rewrites
// it later; it must stay the leading key.
func TestDocSetKeepsPositionOnOverwrite(t *testing.T) {
	d := NewDoc(3)
	d.Set("correlation_id", "")
	d.Set("event_type", "created")
	d.Set("amount", 12)
	d.Set("correlation_id", "abc")

	got, err := d.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	want := `{"correlation_id":"abc","event_type":"created","amount":12}`
	if string(got) != want {
		t.Errorf("MarshalJSON = %s, want %s", got, want)
	}
	if d.Len() != 3 {
		t.Errorf("Len = %d, want 3", d.Len())
	}
}

func TestDocNestedValuesKeepOrder(t *testing.T) {
	inner := NewDoc(2)
	inner.Set("street", "main")
	inner.Set("city", "berlin")

	outer := NewDoc(2)
	outer.Set("address", inner)
	outer.Set("tags", []any{"b", "a"})

	// Marshalled through encoding/json (not the method directly) to prove the
	// nested Doc keeps its order when it is a plain value inside another value.
	got, err := json.Marshal(outer)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	want := `{"address":{"street":"main","city":"berlin"},"tags":["b","a"]}`
	if string(got) != want {
		t.Errorf("json.Marshal = %s, want %s", got, want)
	}
}

func TestDocGetAndKeysCopy(t *testing.T) {
	d := NewDoc(1)
	d.Set("a", 7)

	if v, ok := d.Get("a"); !ok || v != 7 {
		t.Errorf("Get(a) = %v, %v; want 7, true", v, ok)
	}
	if _, ok := d.Get("missing"); ok {
		t.Error("Get(missing) reported present")
	}

	keys := d.Keys()
	keys[0] = "mutated"
	if d.Keys()[0] != "a" {
		t.Error("Keys returned a live slice; callers can corrupt the document")
	}
}

func TestZeroDocIsUsable(t *testing.T) {
	var d Doc
	d.Set("a", 1)
	got, err := d.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	if string(got) != `{"a":1}` {
		t.Errorf("MarshalJSON = %s, want {\"a\":1}", got)
	}
}
