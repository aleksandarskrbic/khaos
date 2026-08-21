package codec

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

func topicWith(name, format string, fields []scenario.Field) scenario.Topic {
	return scenario.Topic{
		Name:           name,
		SchemaProvider: "inline",
		MessageSchema: scenario.MessageSchema{
			DataFormat: format,
			Fields:     fields,
		},
	}
}

func TestNewSelectsCodec(t *testing.T) {
	fields := []scenario.Field{fld("id", scenario.FieldString), fld("count", scenario.FieldInt)}

	tests := []struct {
		name  string
		topic scenario.Topic
		want  any
	}{
		{
			name:  "json",
			topic: topicWith("orders", formatJSON, fields),
			want:  jsonCodec{},
		},
		{
			name:  "avro with fields",
			topic: topicWith("orders", formatAvro, fields),
			want:  &avroCodec{},
		},
		{
			name:  "protobuf with fields",
			topic: topicWith("orders", formatProtobuf, fields),
			want:  &protoCodec{},
		},
		{
			name:  "avro without fields falls back to json",
			topic: topicWith("orders", formatAvro, nil),
			want:  jsonCodec{},
		},
		{
			name:  "protobuf without fields falls back to json",
			topic: topicWith("orders", formatProtobuf, nil),
			want:  jsonCodec{},
		},
		{
			name:  "an unknown format falls back to json",
			topic: topicWith("orders", "yaml", fields),
			want:  jsonCodec{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(context.Background(), tt.topic, nil)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if reflect.TypeOf(got) != reflect.TypeOf(tt.want) {
				t.Fatalf("New returned %T, want %T", got, tt.want)
			}
		})
	}
}

// Without a registry every format must emit bare bytes: no magic byte, no id.
func TestNewWithoutRegistryEmitsBareBytes(t *testing.T) {
	fields := []scenario.Field{fld("id", scenario.FieldString), fld("count", scenario.FieldInt)}

	doc := NewDoc()
	doc.Set("id", "abc")
	doc.Set("count", int64(7))

	for _, format := range []string{formatJSON, formatAvro, formatProtobuf} {
		t.Run(format, func(t *testing.T) {
			c, err := New(context.Background(), topicWith("orders", format, fields), nil)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			b, err := c.Encode(doc)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if len(b) == 0 {
				t.Fatal("Encode produced no bytes")
			}
			if b[0] == 0x00 {
				t.Errorf("output starts with the confluent magic byte: % x", b)
			}
			if _, err := c.Decode(b); err != nil {
				t.Errorf("Decode: %v", err)
			}
		})
	}
}

func TestNewRegistryProviderWithoutRegistry(t *testing.T) {
	topic := topicWith("orders", formatAvro, nil)
	topic.SchemaProvider = "registry"
	topic.SubjectName = "orders-value"

	if _, err := New(context.Background(), topic, nil); err == nil {
		t.Error("expected an error when schema_provider is registry but no registry is configured")
	}
}

// TestCodecEncodeIsConcurrencySafe covers the sharing contract on [Codec].
//
// internal/engine builds one codec per topic and hands the same value to every
// producer goroutine for that topic, so any per-codec mutable state is a data
// race in production that no single-threaded test would ever show. Each
// goroutine gets its own Doc, because a Doc is not shared state -- the codec is.
// Meaningful only under -race.
func TestCodecEncodeIsConcurrencySafe(t *testing.T) {
	fields := []scenario.Field{
		fld("id", scenario.FieldString),
		fld("count", scenario.FieldInt),
		fld("price", scenario.FieldFloat),
	}

	_, reg := newFakeRegistry(t)

	for _, format := range []string{formatJSON, formatAvro, formatProtobuf} {
		t.Run(format, func(t *testing.T) {
			// A registry-framed codec exercises the header path too, which is
			// where a shared buffer would most plausibly creep in.
			c, err := New(context.Background(), topicWith("shared-"+format, format, fields), reg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			const goroutines, perGoroutine = 8, 200
			var wg sync.WaitGroup
			errs := make(chan error, goroutines)

			for g := 0; g < goroutines; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < perGoroutine; i++ {
						doc := NewDoc()
						doc.Set("id", fmt.Sprintf("g%d-%d", g, i))
						doc.Set("count", int64(i))
						doc.Set("price", float64(i)+0.5)

						b, err := c.Encode(doc)
						if err != nil {
							errs <- fmt.Errorf("encode: %w", err)
							return
						}
						out, err := c.Decode(b)
						if err != nil {
							errs <- fmt.Errorf("decode: %w", err)
							return
						}
						got, _ := out.Get("id")
						if want := fmt.Sprintf("g%d-%d", g, i); got != want {
							errs <- fmt.Errorf("round trip gave id %v, want %q", got, want)
							return
						}
					}
				}(g)
			}

			wg.Wait()
			close(errs)
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestJSONCodecRoundTrip(t *testing.T) {
	nested := NewDoc()
	nested.Set("b", int64(2))
	nested.Set("a", "x")

	in := NewDoc()
	in.Set("z", "first")
	in.Set("a", int64(1))
	in.Set("m", 2.5)
	in.Set("flag", false)
	in.Set("nested", nested)
	in.Set("list", []any{int64(1), "two", nested})

	c := jsonCodec{}
	b, err := c.Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	want := `{"z":"first","a":1,"m":2.5,"flag":false,"nested":{"b":2,"a":"x"},"list":[1,"two",{"b":2,"a":"x"}]}`
	if string(b) != want {
		t.Errorf("Encode =\n  %s\nwant\n  %s", b, want)
	}

	out, err := c.Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Errorf("round trip:\n got %#v\nwant %#v", out, in)
	}
}
