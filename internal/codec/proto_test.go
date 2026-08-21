package codec

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

const protoHeader = "syntax = \"proto3\";\n\npackage khaos.generated;\n\n"

// The generated .proto text is what gets POSTed to Schema Registry, so these
// are exact-string assertions. The nested type names in particular
// (<Name>Type, <Name>Item, <Name>Enum) are part of the compatibility surface.
func TestProtoSourceText(t *testing.T) {
	enumField := func(name string, values ...string) scenario.Field {
		f := fld(name, scenario.FieldEnum)
		f.Values = values
		return f
	}
	objectField := func(name string, fields ...scenario.Field) scenario.Field {
		f := fld(name, scenario.FieldObject)
		f.Fields = fields
		return f
	}
	arrayField := func(name string, item *scenario.Field) scenario.Field {
		f := fld(name, scenario.FieldArray)
		f.Items = item
		return f
	}

	objectItem := objectField("item", fld("sku", scenario.FieldString))
	stringItem := fld("item", scenario.FieldString)
	enumItem := enumField("item", "A")

	tests := []struct {
		name   string
		fields []scenario.Field
		want   string
	}{
		{
			name: "scalars use the fixed type map",
			fields: []scenario.Field{
				fld("a", scenario.FieldString),
				fld("b", scenario.FieldInt),
				fld("c", scenario.FieldFloat),
				fld("d", scenario.FieldBoolean),
				fld("e", scenario.FieldUUID),
				fld("f", scenario.FieldTimestamp),
				fld("g", scenario.FieldFaker),
			},
			want: protoHeader + "message M {\n" +
				"  string a = 1;\n" +
				"  int64 b = 2;\n" +
				"  double c = 3;\n" +
				"  bool d = 4;\n" +
				"  string e = 5;\n" +
				"  int64 f = 6;\n" +
				"  string g = 7;\n" +
				"}\n",
		},
		{
			name:   "enum members are numbered from zero",
			fields: []scenario.Field{enumField("order_status", "NEW", "PAID")},
			want: protoHeader + "message M {\n" +
				"  enum OrderStatusEnum {\n" +
				"    NEW = 0;\n" +
				"    PAID = 1;\n" +
				"  }\n" +
				"  OrderStatusEnum order_status = 1;\n" +
				"}\n",
		},
		{
			name:   "enum without values gets UNKNOWN",
			fields: []scenario.Field{enumField("status")},
			want: protoHeader + "message M {\n" +
				"  enum StatusEnum {\n" +
				"    UNKNOWN = 0;\n" +
				"  }\n" +
				"  StatusEnum status = 1;\n" +
				"}\n",
		},
		{
			name:   "object becomes a nested <Name>Type message",
			fields: []scenario.Field{objectField("user_meta", fld("x", scenario.FieldString))},
			want: protoHeader + "message M {\n" +
				"  message UserMetaType {\n" +
				"    string x = 1;\n" +
				"  }\n" +
				"  UserMetaType user_meta = 1;\n" +
				"}\n",
		},
		{
			name: "objects nest recursively",
			fields: []scenario.Field{
				objectField("outer", objectField("inner", fld("x", scenario.FieldInt))),
			},
			want: protoHeader + "message M {\n" +
				"  message OuterType {\n" +
				"    message InnerType {\n" +
				"      int64 x = 1;\n" +
				"    }\n" +
				"    InnerType inner = 1;\n" +
				"  }\n" +
				"  OuterType outer = 1;\n" +
				"}\n",
		},
		{
			name:   "array of objects becomes a nested <Name>Item message",
			fields: []scenario.Field{arrayField("lines", &objectItem)},
			want: protoHeader + "message M {\n" +
				"  message LinesItem {\n" +
				"    string sku = 1;\n" +
				"  }\n" +
				"  repeated LinesItem lines = 1;\n" +
				"}\n",
		},
		{
			name:   "array of scalars",
			fields: []scenario.Field{arrayField("tags", &stringItem)},
			want:   protoHeader + "message M {\n  repeated string tags = 1;\n}\n",
		},
		{
			name:   "array without items degrades to repeated string",
			fields: []scenario.Field{arrayField("tags", nil)},
			want:   protoHeader + "message M {\n  repeated string tags = 1;\n}\n",
		},
		{
			name:   "array of enums also degrades to repeated string",
			fields: []scenario.Field{arrayField("tags", &enumItem)},
			want:   protoHeader + "message M {\n  repeated string tags = 1;\n}\n",
		},
		{
			name:   "unknown type degrades to string",
			fields: []scenario.Field{fld("mystery", "nonsense")},
			want:   protoHeader + "message M {\n  string mystery = 1;\n}\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ProtoSourceText(tt.fields, "M")
			if err != nil {
				t.Fatalf("ProtoSourceText: %v", err)
			}
			if got != tt.want {
				t.Errorf("ProtoSourceText =\n%s\nwant\n%s", got, tt.want)
			}
			if _, err := compileProto(context.Background(), map[string]string{protoRootFile: got}, protoRootFile); err != nil {
				t.Errorf("generated source does not compile: %v", err)
			}
		})
	}
}

// newTestProtoCodec compiles source text into a headerless protobuf codec.
func newTestProtoCodec(t *testing.T, text string) *protoCodec {
	t.Helper()
	md, err := compileProto(context.Background(), map[string]string{protoRootFile: text}, protoRootFile)
	if err != nil {
		t.Fatalf("compileProto: %v", err)
	}
	return &protoCodec{md: md}
}

func TestProtoRoundTrip(t *testing.T) {
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

	fields := []scenario.Field{
		fld("id", scenario.FieldString),
		fld("count", scenario.FieldInt),
		fld("price", scenario.FieldFloat),
		fld("active", scenario.FieldBoolean),
		status,
		meta,
		tags,
		lines,
	}

	text, err := ProtoSourceText(fields, "OrdersRecord")
	if err != nil {
		t.Fatalf("ProtoSourceText: %v", err)
	}
	c := newTestProtoCodec(t, text)

	metaDoc := NewDoc()
	metaDoc.Set("region", "eu")
	metaDoc.Set("weight", 1.25)

	line1 := NewDoc()
	line1.Set("sku", "aaa")
	line1.Set("qty", int64(3))
	line2 := NewDoc()
	line2.Set("sku", "bbb")
	line2.Set("qty", int64(4))

	in := NewDoc()
	in.Set("id", "abc")
	in.Set("count", int64(7))
	in.Set("price", 19.5)
	in.Set("active", true)
	in.Set("status", "PAID")
	in.Set("meta", metaDoc)
	in.Set("tags", []any{"a", "b"})
	in.Set("lines", []any{line1, line2})

	b, err := c.Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, err := c.Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	// protojson decodes 64-bit integers as decimal strings.
	wantMeta := NewDoc()
	wantMeta.Set("region", "eu")
	wantMeta.Set("weight", 1.25)
	wantLine1 := NewDoc()
	wantLine1.Set("sku", "aaa")
	wantLine1.Set("qty", "3")
	wantLine2 := NewDoc()
	wantLine2.Set("sku", "bbb")
	wantLine2.Set("qty", "4")

	want := NewDoc()
	want.Set("id", "abc")
	want.Set("count", "7")
	want.Set("price", 19.5)
	want.Set("active", true)
	want.Set("status", "PAID")
	want.Set("meta", wantMeta)
	want.Set("tags", []any{"a", "b"})
	want.Set("lines", []any{wantLine1, wantLine2})

	if !reflect.DeepEqual(out, want) {
		t.Errorf("round trip:\n got %#v\nwant %#v", out, want)
	}
	if got := out.Keys(); !reflect.DeepEqual(got, want.Keys()) {
		t.Errorf("decoded key order = %v, want %v", got, want.Keys())
	}
}

// TestProtoEncodeIsStableAndOrdered pins the canonical field layout: fields
// are set in reverse order to prove the wire order comes from the descriptor,
// not from insertion order.
func TestProtoEncodeIsStableAndOrdered(t *testing.T) {
	fields := []scenario.Field{
		fld("a", scenario.FieldInt),    // field 1
		fld("b", scenario.FieldInt),    // field 2
		fld("c", scenario.FieldInt),    // field 3
		fld("d", scenario.FieldInt),    // field 4
		fld("e", scenario.FieldString), // field 5
	}
	text, err := ProtoSourceText(fields, "OrderedRecord")
	if err != nil {
		t.Fatalf("ProtoSourceText: %v", err)
	}
	c := newTestProtoCodec(t, text)

	in := NewDoc()
	in.Set("e", "z")
	in.Set("d", int64(4))
	in.Set("c", int64(3))
	in.Set("b", int64(2))
	in.Set("a", int64(1))

	// Tag byte = field number << 3 | wire type: varints 1-4 then a length-delimited 5.
	want := []byte{0x08, 0x01, 0x10, 0x02, 0x18, 0x03, 0x20, 0x04, 0x2a, 0x01, 'z'}

	// Repeated because an unordered encoder is only wrong some of the time: with five
	// fields a single comparison would pass roughly one run in a hundred.
	for i := 0; i < 50; i++ {
		got, err := c.Encode(in)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Encode (attempt %d) = % x\nwant                 % x", i, got, want)
		}
	}
}

// Every scalar kind has to be converted to the exact Go type protoreflect
// demands; anything else panics inside Set rather than returning an error.
func TestProtoScalarKinds(t *testing.T) {
	const src = `syntax = "proto3";
package khaos.generated;
message Scalars {
  int32 i32 = 1;
  int64 i64 = 2;
  uint32 u32 = 3;
  uint64 u64 = 4;
  sint32 s32 = 5;
  sint64 s64 = 6;
  fixed32 f32 = 7;
  fixed64 f64 = 8;
  sfixed32 sf32 = 9;
  sfixed64 sf64 = 10;
  float fl = 11;
  double db = 12;
  bool bo = 13;
  string st = 14;
  bytes by = 15;
}
`
	c := newTestProtoCodec(t, src)

	in := NewDoc()
	in.Set("i32", int64(-5))
	in.Set("i64", int64(-6))
	in.Set("u32", int64(7))
	in.Set("u64", int64(8))
	in.Set("s32", int64(-9))
	in.Set("s64", int64(-10))
	in.Set("f32", int64(11))
	in.Set("f64", int64(12))
	in.Set("sf32", int64(-13))
	in.Set("sf64", int64(-14))
	in.Set("fl", 1.5)
	in.Set("db", 2.5)
	in.Set("bo", true)
	in.Set("st", "s")
	in.Set("by", "hi")

	b, err := c.Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, err := c.Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	// protojson renders 32-bit integers as numbers, 64-bit ones as strings and
	// bytes as base64.
	want := map[string]any{
		"i32": int64(-5), "i64": "-6",
		"u32": int64(7), "u64": "8",
		"s32": int64(-9), "s64": "-10",
		"f32": int64(11), "f64": "12",
		"sf32": int64(-13), "sf64": "-14",
		"fl": 1.5, "db": 2.5,
		"bo": true, "st": "s", "by": "aGk=",
	}
	for k, wantV := range want {
		gotV, ok := out.Get(k)
		if !ok {
			t.Errorf("field %q missing from decoded document", k)
			continue
		}
		if !reflect.DeepEqual(gotV, wantV) {
			t.Errorf("field %q = %#v, want %#v", k, gotV, wantV)
		}
	}
}

// Encoding must accept the several Go shapes a generated value can arrive in,
// and must not blow up on a key the schema does not know.
func TestProtoEncodeInputShapes(t *testing.T) {
	const src = `syntax = "proto3";
package khaos.generated;
message M {
  int64 n = 1;
  double d = 2;
  Nested nested = 3;
  repeated string tags = 4;
  enum StatusEnum { NEW = 0; PAID = 1; }
  StatusEnum status = 5;
  message Nested { string x = 1; }
}
`
	tests := []struct {
		name string
		set  func(*Doc)
		key  string
		want any
	}{
		{"int value for an int64 field", func(d *Doc) { d.Set("n", 3) }, "n", "3"},
		{"float64 value for an int64 field", func(d *Doc) { d.Set("n", float64(4)) }, "n", "4"},
		// An integral double renders as "5" in JSON and so decodes as an
		// integer; the assertion is that the int input was accepted at all.
		{"int value for a double field", func(d *Doc) { d.Set("d", 5) }, "d", int64(5)},
		{"plain map for a message field", func(d *Doc) { d.Set("nested", map[string]any{"x": "y"}) }, "nested", nil},
		{"typed slice for a repeated field", func(d *Doc) { d.Set("tags", []string{"a"}) }, "tags", []any{"a"}},
		{"unknown enum symbol falls back to zero", func(d *Doc) { d.Set("status", "NOPE") }, "status", nil},
		{"unknown key is ignored", func(d *Doc) { d.Set("nope", "x"); d.Set("n", 1) }, "n", "1"},
	}

	c := newTestProtoCodec(t, src)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := NewDoc()
			tt.set(in)
			b, err := c.Encode(in)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			out, err := c.Decode(b)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if tt.want == nil {
				return
			}
			got, ok := out.Get(tt.key)
			if !ok {
				t.Fatalf("field %q missing from %#v", tt.key, out)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("field %q = %#v, want %#v", tt.key, got, tt.want)
			}
		})
	}
}
