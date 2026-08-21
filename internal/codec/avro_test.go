package codec

import (
	"reflect"
	"testing"

	"github.com/hamba/avro/v2"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

func fld(name, typ string) scenario.Field {
	return scenario.Field{Name: name, Type: typ, MinItems: 1, MaxItems: 5}
}

// The generated schema text is what gets POSTed to Schema Registry, so these
// are exact-string assertions rather than structural ones.
func TestAvroSchemaText(t *testing.T) {
	tests := []struct {
		name   string
		fields []scenario.Field
		want   string
	}{
		{
			name:   "int becomes long and float becomes double",
			fields: []scenario.Field{fld("count", scenario.FieldInt), fld("price", scenario.FieldFloat)},
			want: `{"type":"record","name":"R","namespace":"khaos.generated","fields":[` +
				`{"name":"count","type":{"type":"long"}},` +
				`{"name":"price","type":{"type":"double"}}]}`,
		},
		{
			name:   "string boolean and faker are all strings or bools",
			fields: []scenario.Field{fld("a", scenario.FieldString), fld("b", scenario.FieldBoolean), fld("c", scenario.FieldFaker)},
			want: `{"type":"record","name":"R","namespace":"khaos.generated","fields":[` +
				`{"name":"a","type":{"type":"string"}},` +
				`{"name":"b","type":{"type":"boolean"}},` +
				`{"name":"c","type":{"type":"string"}}]}`,
		},
		{
			name:   "logical types",
			fields: []scenario.Field{fld("id", scenario.FieldUUID), fld("ts", scenario.FieldTimestamp)},
			want: `{"type":"record","name":"R","namespace":"khaos.generated","fields":[` +
				`{"name":"id","type":{"type":"string","logicalType":"uuid"}},` +
				`{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}`,
		},
		{
			name: "enum uses the avro name mangling",
			fields: []scenario.Field{func() scenario.Field {
				f := fld("order_status", scenario.FieldEnum)
				f.Values = []string{"NEW", "PAID"}
				return f
			}()},
			want: `{"type":"record","name":"R","namespace":"khaos.generated","fields":[` +
				`{"name":"order_status","type":{"type":"enum","name":"OrderStatusEnum","symbols":["NEW","PAID"]}}]}`,
		},
		{
			name:   "enum without values degrades to string",
			fields: []scenario.Field{fld("status", scenario.FieldEnum)},
			want: `{"type":"record","name":"R","namespace":"khaos.generated","fields":[` +
				`{"name":"status","type":{"type":"string"}}]}`,
		},
		{
			name: "object becomes a nested record",
			fields: []scenario.Field{func() scenario.Field {
				f := fld("meta", scenario.FieldObject)
				f.Fields = []scenario.Field{fld("x", scenario.FieldString)}
				return f
			}()},
			want: `{"type":"record","name":"R","namespace":"khaos.generated","fields":[` +
				`{"name":"meta","type":{"type":"record","name":"MetaRecord","fields":[{"name":"x","type":{"type":"string"}}]}}]}`,
		},
		{
			name:   "object without fields degrades to a string map",
			fields: []scenario.Field{fld("meta", scenario.FieldObject)},
			want: `{"type":"record","name":"R","namespace":"khaos.generated","fields":[` +
				`{"name":"meta","type":{"type":"map","values":"string"}}]}`,
		},
		{
			name: "array of objects",
			fields: []scenario.Field{func() scenario.Field {
				item := fld("item", scenario.FieldObject)
				item.Fields = []scenario.Field{fld("y", scenario.FieldInt)}
				f := fld("lines", scenario.FieldArray)
				f.Items = &item
				return f
			}()},
			want: `{"type":"record","name":"R","namespace":"khaos.generated","fields":[` +
				`{"name":"lines","type":{"type":"array","items":{"type":"record","name":"ItemRecord","fields":[{"name":"y","type":{"type":"long"}}]}}}]}`,
		},
		{
			name:   "array without items degrades to array of string",
			fields: []scenario.Field{fld("tags", scenario.FieldArray)},
			want: `{"type":"record","name":"R","namespace":"khaos.generated","fields":[` +
				`{"name":"tags","type":{"type":"array","items":{"type":"string"}}}]}`,
		},
		{
			name:   "unknown type degrades to string",
			fields: []scenario.Field{fld("mystery", "nonsense")},
			want: `{"type":"record","name":"R","namespace":"khaos.generated","fields":[` +
				`{"name":"mystery","type":{"type":"string"}}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AvroSchemaText(tt.fields, "R")
			if err != nil {
				t.Fatalf("AvroSchemaText: %v", err)
			}
			if got != tt.want {
				t.Errorf("AvroSchemaText =\n  %s\nwant\n  %s", got, tt.want)
			}
			if _, err := parseAvro(got); err != nil {
				t.Errorf("generated schema does not parse: %v", err)
			}
		})
	}
}

// avroRoundTripFields is one field of every kind the generator can emit.
func avroRoundTripFields() []scenario.Field {
	status := fld("status", scenario.FieldEnum)
	status.Values = []string{"NEW", "PAID"}

	meta := fld("meta", scenario.FieldObject)
	meta.Fields = []scenario.Field{fld("region", scenario.FieldString), fld("weight", scenario.FieldFloat)}

	tagItem := fld("item", scenario.FieldString)
	tags := fld("tags", scenario.FieldArray)
	tags.Items = &tagItem

	lineItem := fld("item", scenario.FieldObject)
	lineItem.Fields = []scenario.Field{fld("sku", scenario.FieldString), fld("qty", scenario.FieldInt)}
	lines := fld("lines", scenario.FieldArray)
	lines.Items = &lineItem

	return []scenario.Field{
		fld("id", scenario.FieldUUID),
		fld("name", scenario.FieldString),
		fld("count", scenario.FieldInt),
		fld("price", scenario.FieldFloat),
		fld("active", scenario.FieldBoolean),
		fld("ts", scenario.FieldTimestamp),
		status,
		meta,
		tags,
		lines,
	}
}

func TestAvroRoundTrip(t *testing.T) {
	text, err := AvroSchemaText(avroRoundTripFields(), "OrdersRecord")
	if err != nil {
		t.Fatalf("AvroSchemaText: %v", err)
	}
	schema, err := parseAvro(text)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	c := &avroCodec{schema: schema, record: schema.(*avro.RecordSchema)}

	meta := NewDoc()
	meta.Set("region", "eu")
	meta.Set("weight", 1.25)

	line := NewDoc()
	line.Set("sku", "abc")
	line.Set("qty", int64(3))

	in := NewDoc()
	in.Set("id", "3f2504e0-4f89-11d3-9a0c-0305e82c3301")
	in.Set("name", "widget")
	in.Set("count", int64(7))
	in.Set("price", 19.99)
	in.Set("active", true)
	in.Set("ts", int64(1700000000123))
	in.Set("status", "PAID")
	in.Set("meta", meta)
	in.Set("tags", []any{"a", "b"})
	in.Set("lines", []any{line})

	b, err := c.Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if b[0] == 0x00 {
		t.Errorf("headerless avro must not start with the confluent magic byte")
	}

	out, err := c.Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Errorf("round trip changed the document:\n got %#v\nwant %#v", out, in)
	}
	// Field order must come back in schema order, not map order.
	if got, want := out.Keys(), in.Keys(); !reflect.DeepEqual(got, want) {
		t.Errorf("decoded key order = %v, want %v", got, want)
	}
}

// TestAvroParseDoesNotShareNamesBetweenSchemas pins the isolation in parseAvro.
//
// avro.Parse publishes every named type it sees into the package-global
// avro.DefaultSchemaCache and seeds the next parse from it, so a schema that
// references a type it does not define could bind to a name another topic
// happened to register first. Building "defines" before "references" is exactly
// the order that makes the leak visible: the second schema must still fail.
func TestAvroParseDoesNotShareNamesBetweenSchemas(t *testing.T) {
	const defines = `{"type":"record","name":"Holder","namespace":"leak",` +
		`"fields":[{"name":"inner","type":{"type":"record","name":"Shared",` +
		`"fields":[{"name":"a","type":"string"}]}}]}`
	// Refers to leak.Shared by name without declaring it.
	const references = `{"type":"record","name":"Other","namespace":"leak",` +
		`"fields":[{"name":"inner","type":"Shared"}]}`

	if _, err := parseAvro(defines); err != nil {
		t.Fatalf("parseAvro(defines) = %v, want it to parse", err)
	}
	if _, err := parseAvro(references); err == nil {
		t.Fatal("parseAvro(references) succeeded: an undefined type resolved through " +
			"another schema's names, which makes the encoding depend on build order")
	}

	// Intra-document reuse of a name must still work: that is legal Avro and the
	// only reason the parser consults a cache at all.
	const reuses = `{"type":"record","name":"Twice","namespace":"leak","fields":[` +
		`{"name":"one","type":{"type":"record","name":"Pair","fields":[{"name":"a","type":"string"}]}},` +
		`{"name":"two","type":"Pair"}]}`
	if _, err := parseAvro(reuses); err != nil {
		t.Fatalf("parseAvro(reuses) = %v, want a self-contained schema to still resolve its own names", err)
	}
}

// A timestamp-millis long is written from epoch millis and must decode back to
// epoch millis, not to a time.Time.
func TestAvroTimestampNormalisation(t *testing.T) {
	text, err := AvroSchemaText([]scenario.Field{fld("ts", scenario.FieldTimestamp)}, "R")
	if err != nil {
		t.Fatalf("AvroSchemaText: %v", err)
	}
	schema, err := parseAvro(text)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	c := &avroCodec{schema: schema, record: schema.(*avro.RecordSchema)}

	in := NewDoc()
	in.Set("ts", int64(1700000000123))
	b, err := c.Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, err := c.Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	got, _ := out.Get("ts")
	if got != int64(1700000000123) {
		t.Errorf("ts = %#v, want int64(1700000000123)", got)
	}
}
