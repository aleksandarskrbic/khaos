package codec

import (
	"context"
	"reflect"
	"testing"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

func TestAvroFieldsFromSchema(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   []scenario.Field
	}{
		{
			name:   "primitives map back through AVRO_TYPE_MAP",
			schema: `{"type":"record","name":"R","fields":[{"name":"a","type":"string"},{"name":"b","type":"long"},{"name":"c","type":"int"},{"name":"d","type":"double"},{"name":"e","type":"float"},{"name":"f","type":"boolean"},{"name":"g","type":"bytes"}]}`,
			want: []scenario.Field{
				fld("a", scenario.FieldString),
				fld("b", scenario.FieldInt),
				fld("c", scenario.FieldInt),
				fld("d", scenario.FieldFloat),
				fld("e", scenario.FieldFloat),
				fld("f", scenario.FieldBoolean),
				fld("g", scenario.FieldString),
			},
		},
		{
			name:   "logical types",
			schema: `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"string","logicalType":"uuid"}},{"name":"b","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"c","type":{"type":"long","logicalType":"timestamp-micros"}},{"name":"d","type":{"type":"int","logicalType":"date"}}]}`,
			want: []scenario.Field{
				fld("a", scenario.FieldUUID),
				fld("b", scenario.FieldTimestamp),
				fld("c", scenario.FieldTimestamp),
				fld("d", scenario.FieldInt),
			},
		},
		{
			name:   "a union takes its first non-null branch",
			schema: `{"type":"record","name":"R","fields":[{"name":"a","type":["null","long"]}]}`,
			want:   []scenario.Field{fld("a", scenario.FieldInt)},
		},
		{
			name:   "an all-null union is dropped",
			schema: `{"type":"record","name":"R","fields":[{"name":"a","type":["null"]},{"name":"b","type":"string"}]}`,
			want:   []scenario.Field{fld("b", scenario.FieldString)},
		},
		{
			name:   "a map degrades to an object with no fields",
			schema: `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"map","values":"string"}}]}`,
			want:   []scenario.Field{fld("a", scenario.FieldObject)},
		},
		{
			name:   "unknown primitives degrade to string",
			schema: `{"type":"record","name":"R","fields":[{"name":"a","type":"fixed"}]}`,
			want:   []scenario.Field{fld("a", scenario.FieldString)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := avroFieldsFromSchema(tt.schema)
			if err != nil {
				t.Fatalf("avroFieldsFromSchema: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("fields = %#v\nwant %#v", got, tt.want)
			}
		})
	}
}

func TestAvroFieldsFromSchemaNested(t *testing.T) {
	const schema = `{"type":"record","name":"R","fields":[
	  {"name":"meta","type":{"type":"record","name":"MetaRecord","fields":[{"name":"x","type":"string"}]}},
	  {"name":"status","type":{"type":"enum","name":"StatusEnum","symbols":["NEW","PAID"]}},
	  {"name":"tags","type":{"type":"array","items":"string"}},
	  {"name":"lines","type":{"type":"array","items":{"type":"record","name":"LineRecord","fields":[{"name":"sku","type":"string"}]}}}
	]}`

	got, err := avroFieldsFromSchema(schema)
	if err != nil {
		t.Fatalf("avroFieldsFromSchema: %v", err)
	}

	meta := fld("meta", scenario.FieldObject)
	meta.Fields = []scenario.Field{fld("x", scenario.FieldString)}

	status := fld("status", scenario.FieldEnum)
	status.Values = []string{"NEW", "PAID"}

	tagItem := fld("item", scenario.FieldString)
	tags := fld("tags", scenario.FieldArray)
	tags.Items = &tagItem

	lineItem := fld("item", scenario.FieldObject)
	lineItem.Fields = []scenario.Field{fld("sku", scenario.FieldString)}
	lines := fld("lines", scenario.FieldArray)
	lines.Items = &lineItem

	want := []scenario.Field{meta, status, tags, lines}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("fields = %#v\nwant %#v", got, want)
	}
}

func TestAvroFieldsFromSchemaRejectsNonRecord(t *testing.T) {
	if _, err := avroFieldsFromSchema(`{"type":"string"}`); err == nil {
		t.Error("expected an error for a non-record schema")
	}
	if _, err := avroFieldsFromSchema(`not json`); err == nil {
		t.Error("expected an error for unparseable schema text")
	}
}

// Pins conversion of nested messages, nested enums and repeated messages.
func TestProtoFieldsFromSource(t *testing.T) {
	const src = `syntax = "proto3";
package khaos.generated;
message Order {
  message MetaType { string region = 1; }
  message LineItem { string sku = 1; int32 qty = 2; }
  enum StatusEnum { NEW = 0; PAID = 1; }
  string id = 1;
  int64 total = 2;
  double price = 3;
  bool paid = 4;
  bytes blob = 5;
  StatusEnum status = 6;
  MetaType meta = 7;
  repeated string tags = 8;
  repeated LineItem lines = 9;
  map<string, string> labels = 10;
}
message Ignored { string x = 1; }
`
	got, err := protoFieldsFromSource(context.Background(), src)
	if err != nil {
		t.Fatalf("protoFieldsFromSource: %v", err)
	}

	status := fld("status", scenario.FieldEnum)
	status.Values = []string{"NEW", "PAID"}

	meta := fld("meta", scenario.FieldObject)
	meta.Fields = []scenario.Field{fld("region", scenario.FieldString)}

	tagItem := fld("item", scenario.FieldString)
	tags := fld("tags", scenario.FieldArray)
	tags.Items = &tagItem

	lineItem := fld("item", scenario.FieldObject)
	lineItem.Fields = []scenario.Field{fld("sku", scenario.FieldString), fld("qty", scenario.FieldInt)}
	lines := fld("lines", scenario.FieldArray)
	lines.Items = &lineItem

	want := []scenario.Field{
		fld("id", scenario.FieldString),
		fld("total", scenario.FieldInt),
		fld("price", scenario.FieldFloat),
		fld("paid", scenario.FieldBoolean),
		fld("blob", scenario.FieldString),
		status,
		meta,
		tags,
		lines,
		fld("labels", scenario.FieldObject),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("fields = %#v\nwant %#v", got, want)
	}
}

// A schema importing a well-known type must resolve; WithStandardImports is
// what makes that work.
func TestProtoFieldsFromSourceStandardImports(t *testing.T) {
	const src = `syntax = "proto3";
package khaos.generated;
import "google/protobuf/timestamp.proto";
message Event {
  string id = 1;
  google.protobuf.Timestamp at = 2;
}
`
	got, err := protoFieldsFromSource(context.Background(), src)
	if err != nil {
		t.Fatalf("protoFieldsFromSource: %v", err)
	}
	if len(got) != 2 || got[1].Type != scenario.FieldObject {
		t.Fatalf("fields = %#v, want the timestamp as an object", got)
	}
	if len(got[1].Fields) != 2 {
		t.Errorf("timestamp expanded to %d fields, want 2 (seconds, nanos)", len(got[1].Fields))
	}
}

// Self-referential messages must terminate rather than recurse forever.
func TestProtoFieldsFromSourceSelfReference(t *testing.T) {
	const src = `syntax = "proto3";
package khaos.generated;
message Node {
  string name = 1;
  Node parent = 2;
}
`
	got, err := protoFieldsFromSource(context.Background(), src)
	if err != nil {
		t.Fatalf("protoFieldsFromSource: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("fields = %#v, want 2", got)
	}
	if got[1].Type != scenario.FieldObject || len(got[1].Fields) != 0 {
		t.Errorf("recursive field = %#v, want an object with no fields", got[1])
	}
}

// An unparseable .proto is now a real error instead of a silently truncated
// regex match.
func TestProtoFieldsFromSourceRejectsGarbage(t *testing.T) {
	if _, err := protoFieldsFromSource(context.Background(), "this is not a proto file"); err == nil {
		t.Error("expected a compile error")
	}
}
