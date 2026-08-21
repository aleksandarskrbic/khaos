package codec

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

const (
	testAvroSubjectSchema = `{"type":"record","name":"Order","namespace":"acme","fields":[` +
		`{"name":"id","type":"string"},` +
		`{"name":"total","type":"long"},` +
		`{"name":"at","type":{"type":"long","logicalType":"timestamp-millis"}}]}`

	testProtoSubjectSchema = `syntax = "proto3";
package acme;
message Order {
  string id = 1;
  int64 total = 2;
}
`
)

// newFakeRegistry starts an in-memory Schema Registry and returns it together
// with a codec Registry pointed at it.
func newFakeRegistry(t *testing.T) (*srfake.Registry, *Registry) {
	t.Helper()
	fake := srfake.New()
	t.Cleanup(fake.Close)

	reg, err := NewRegistry(fake.URL())
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	return fake, reg
}

func TestNewRegistryRejectsUnreachable(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{"empty url", ""},
		// Port 1 on localhost refuses connections immediately.
		{"nothing listening", "http://127.0.0.1:1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewRegistry(tt.url); err == nil {
				t.Errorf("NewRegistry(%q) succeeded, want an unreachable error", tt.url)
			}
		})
	}
}

func TestRegistryFieldsFor(t *testing.T) {
	fake, reg := newFakeRegistry(t)
	fake.SeedSchema("orders-value", 1, 7, sr.Schema{Schema: testAvroSubjectSchema, Type: sr.TypeAvro})
	fake.SeedSchema("events-value", 1, 8, sr.Schema{Schema: testProtoSubjectSchema, Type: sr.TypeProtobuf})

	tests := []struct {
		name       string
		subject    string
		wantFormat string
		wantFields []scenario.Field
	}{
		{
			name:       "avro",
			subject:    "orders-value",
			wantFormat: formatAvro,
			wantFields: []scenario.Field{
				fld("id", scenario.FieldString),
				fld("total", scenario.FieldInt),
				fld("at", scenario.FieldTimestamp),
			},
		},
		{
			name:       "protobuf",
			subject:    "events-value",
			wantFormat: formatProtobuf,
			wantFields: []scenario.Field{
				fld("id", scenario.FieldString),
				fld("total", scenario.FieldInt),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			format, fields, err := reg.FieldsFor(context.Background(), tt.subject)
			if err != nil {
				t.Fatalf("FieldsFor: %v", err)
			}
			if format != tt.wantFormat {
				t.Errorf("format = %q, want %q", format, tt.wantFormat)
			}
			if !reflect.DeepEqual(fields, tt.wantFields) {
				t.Errorf("fields = %#v\nwant %#v", fields, tt.wantFields)
			}
		})
	}

	t.Run("unknown subject", func(t *testing.T) {
		if _, _, err := reg.FieldsFor(context.Background(), "nope-value"); err == nil {
			t.Error("expected an error for a subject that does not exist")
		}
	})

	t.Run("results are cached", func(t *testing.T) {
		// Wiping the registry must not affect an already-resolved subject.
		fake.Reset()
		format, fields, err := reg.FieldsFor(context.Background(), "orders-value")
		if err != nil {
			t.Fatalf("FieldsFor after reset: %v", err)
		}
		if format != formatAvro || len(fields) != 3 {
			t.Errorf("cache miss: format %q with %d fields", format, len(fields))
		}
	})
}

func TestRegistryRejectsUnsupportedSchemaType(t *testing.T) {
	fake, reg := newFakeRegistry(t)
	fake.SeedSchema("json-value", 1, 1, sr.Schema{Schema: `{"type":"object"}`, Type: sr.TypeJSON})

	if _, _, err := reg.FieldsFor(context.Background(), "json-value"); err == nil {
		t.Error("expected an error for a JSON-schema subject")
	}
}

// Inline schemas are registered under {topic}-value and the returned id is the
// one stamped into the wire header.
func TestNewRegistersInlineSchemas(t *testing.T) {
	fields := []scenario.Field{fld("id", scenario.FieldString), fld("count", scenario.FieldInt)}
	doc := NewDoc()
	doc.Set("id", "abc")
	doc.Set("count", int64(7))

	tests := []struct {
		name      string
		format    string
		wantIndex bool
		wantText  func() string
	}{
		{
			name:     "avro",
			format:   formatAvro,
			wantText: func() string { s, _ := AvroSchemaText(fields, "OrdersRecord"); return s },
		},
		{
			name:      "protobuf",
			format:    formatProtobuf,
			wantIndex: true,
			wantText:  func() string { s, _ := ProtoSourceText(fields, "OrdersRecord"); return s },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake, reg := newFakeRegistry(t)

			c, err := New(context.Background(), topicWith("orders", tt.format, fields), reg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			stored, ok := fake.GetSchema("orders-value", 1)
			if !ok {
				t.Fatal("nothing was registered under orders-value")
			}
			// The registered text is byte-identical to the text used to encode.
			if stored.Schema.Schema != tt.wantText() {
				t.Errorf("registered schema =\n%s\nwant\n%s", stored.Schema.Schema, tt.wantText())
			}

			b, err := c.Encode(doc)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			want := []byte{0x00, byte(stored.ID >> 24), byte(stored.ID >> 16), byte(stored.ID >> 8), byte(stored.ID)}
			if tt.wantIndex {
				want = append(want, 0x00) // message index [0]
			}
			if !bytes.HasPrefix(b, want) {
				t.Errorf("encoded prefix = % x, want % x", b[:min(len(b), len(want))], want)
			}
			if _, err := c.Decode(b); err != nil {
				t.Errorf("Decode: %v", err)
			}

			// Building the same topic again must not add a version.
			if _, err := New(context.Background(), topicWith("orders", tt.format, fields), reg); err != nil {
				t.Fatalf("New (second): %v", err)
			}
			if _, ok := fake.GetSchema("orders-value", 2); ok {
				t.Error("re-registering an identical schema created a second version")
			}
		})
	}
}

// With schema_provider: registry, khaos must reuse the fetched schema id and
// register nothing new under the subject.
func TestNewWithRegistryProviderNeverReRegisters(t *testing.T) {
	tests := []struct {
		name      string
		subject   string
		schema    sr.Schema
		id        int
		wantIndex bool
	}{
		{
			name:    "avro",
			subject: "orders-value",
			schema:  sr.Schema{Schema: testAvroSubjectSchema, Type: sr.TypeAvro},
			id:      7,
		},
		{
			name:      "protobuf",
			subject:   "events-value",
			schema:    sr.Schema{Schema: testProtoSubjectSchema, Type: sr.TypeProtobuf},
			id:        8,
			wantIndex: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake, reg := newFakeRegistry(t)
			fake.SeedSchema(tt.subject, 1, tt.id, tt.schema)

			topic := scenario.Topic{
				Name:           "orders",
				SchemaProvider: "registry",
				SubjectName:    tt.subject,
				MessageSchema:  scenario.MessageSchema{DataFormat: formatJSON},
			}
			c, err := New(context.Background(), topic, reg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			doc := NewDoc()
			doc.Set("id", "abc")
			doc.Set("total", int64(3))
			if tt.name == "avro" {
				doc.Set("at", int64(1700000000123))
			}

			b, err := c.Encode(doc)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			want := []byte{0x00, 0x00, 0x00, 0x00, byte(tt.id)}
			if tt.wantIndex {
				want = append(want, 0x00)
			}
			if !bytes.HasPrefix(b, want) {
				t.Errorf("encoded prefix = % x, want the fetched id % x", b[:min(len(b), len(want))], want)
			}

			// Nothing new may appear, under this subject or the topic's own.
			if _, ok := fake.GetSchema(tt.subject, 2); ok {
				t.Error("a second version was registered under the fetched subject")
			}
			if fake.SubjectExists("orders-value") && tt.subject != "orders-value" {
				t.Error("the topic's own subject was created")
			}
		})
	}
}
