package codec

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestDocKeyOrder(t *testing.T) {
	tests := []struct {
		name string
		set  [][2]any
		want []string
	}{
		{
			name: "insertion order is preserved",
			set:  [][2]any{{"zebra", 1}, {"apple", 2}, {"mango", 3}},
			want: []string{"zebra", "apple", "mango"},
		},
		{
			name: "overwriting keeps the original position",
			set:  [][2]any{{"a", 1}, {"b", 2}, {"a", 3}},
			want: []string{"a", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDoc()
			for _, kv := range tt.set {
				d.Set(kv[0].(string), kv[1])
			}
			if got := d.Keys(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Keys() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDocMarshalJSONOrder(t *testing.T) {
	// A Go map would marshal these alphabetically; Doc must not.
	inner := NewDoc()
	inner.Set("z", 1)
	inner.Set("a", 2)

	d := NewDoc()
	d.Set("zebra", "z")
	d.Set("apple", int64(1))
	d.Set("nested", inner)
	d.Set("list", []any{int64(1), "two"})
	d.Set("html", "a<b&c>d")
	d.Set("nothing", nil)

	// MarshalJSON is called directly, as jsonCodec.Encode does. Going through
	// json.Marshal instead would re-escape the marshaler's output with HTML
	// escaping switched back on, which is exactly what the codec avoids.
	got, err := d.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	want := `{"zebra":"z","apple":1,"nested":{"z":1,"a":2},"list":[1,"two"],"html":"a<b&c>d","nothing":null}`
	if string(got) != want {
		t.Errorf("Marshal =\n  %s\nwant\n  %s", got, want)
	}
}

func TestDocUnmarshalJSON(t *testing.T) {
	const in = `{"b":1,"a":2.5,"c":"x","d":true,"e":null,"f":{"n":1},"g":[1,{"m":2}],"big":9007199254740993}`

	d := NewDoc()
	if err := json.Unmarshal([]byte(in), d); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got, want := d.Keys(), []string{"b", "a", "c", "d", "e", "f", "g", "big"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Keys() = %v, want %v", got, want)
	}

	tests := []struct {
		key  string
		want any
	}{
		{"b", int64(1)}, // integral literals stay integers
		{"a", 2.5},      // fractions become floats
		{"c", "x"},
		{"d", true},
		{"e", nil},
		{"big", int64(9007199254740993)}, // exact past float64 precision
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			got, ok := d.Get(tt.key)
			if !ok {
				t.Fatalf("key %q missing", tt.key)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get(%q) = %#v, want %#v", tt.key, got, tt.want)
			}
		})
	}

	nested, _ := d.Get("f")
	if _, ok := nested.(*Doc); !ok {
		t.Errorf("nested object decoded as %T, want *Doc", nested)
	}
	list, _ := d.Get("g")
	arr, ok := list.([]any)
	if !ok || len(arr) != 2 {
		t.Fatalf("array decoded as %#v", list)
	}
	if _, ok := arr[1].(*Doc); !ok {
		t.Errorf("object inside array decoded as %T, want *Doc", arr[1])
	}
}

func TestDocRoundTripPreservesOrder(t *testing.T) {
	d := NewDoc()
	d.Set("z", int64(1))
	d.Set("y", "two")
	d.Set("x", 3.5)

	b, err := d.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	back := NewDoc()
	if err := json.Unmarshal(b, back); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(d, back) {
		t.Errorf("round trip changed the document:\n got %#v\nwant %#v", back, d)
	}
}
