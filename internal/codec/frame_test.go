package codec

import (
	"bytes"
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// The Confluent wire format is a hard contract with every other client on the
// cluster, so the byte layout is asserted directly.
func TestFrameByteLayout(t *testing.T) {
	tests := []struct {
		name string
		f    frame
		body []byte
		want []byte
	}{
		{
			name: "avro header is magic byte plus big-endian id",
			f:    frame{id: 42},
			body: []byte{0xde, 0xad},
			want: []byte{0x00, 0x00, 0x00, 0x00, 0x2a, 0xde, 0xad},
		},
		{
			name: "large ids fill all four bytes",
			f:    frame{id: 0x01020304},
			body: nil,
			want: []byte{0x00, 0x01, 0x02, 0x03, 0x04},
		},
		{
			name: "protobuf index [0] uses the single-zero-byte shortcut",
			f:    frame{id: 1, index: []int{0}},
			body: []byte{0x08, 0x01},
			want: []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x08, 0x01},
		},
		{
			name: "a deeper index is a varint count followed by varint indexes",
			f:    frame{id: 1, index: []int{1, 2}},
			body: nil,
			// zigzag varints: 2 -> 0x04, 1 -> 0x02, 2 -> 0x04
			want: []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x04, 0x02, 0x04},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.f.prepend(tt.body)
			if err != nil {
				t.Fatalf("prepend: %v", err)
			}
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("prepend = % x, want % x", got, tt.want)
			}

			id, rest, err := tt.f.strip(got)
			if err != nil {
				t.Fatalf("strip: %v", err)
			}
			if id != tt.f.id {
				t.Errorf("strip id = %d, want %d", id, tt.f.id)
			}
			if !bytes.Equal(rest, tt.body) && !(len(rest) == 0 && len(tt.body) == 0) {
				t.Errorf("strip body = % x, want % x", rest, tt.body)
			}
		})
	}
}

// TestConfluentWireFormatExactBytes asserts the complete record value, header and
// payload, for a schema id chosen so that all four id bytes differ.
//
// This is the one part of the codec that a Go-only test cannot otherwise catch: a wrong
// magic byte, a little-endian id or a missing protobuf message index all round-trip
// perfectly through khaos' own Decode and only fail on a JVM consumer, days later. The
// bytes below are what the Confluent wire format specifies, independently of this package.
func TestConfluentWireFormatExactBytes(t *testing.T) {
	// 0x01020304 = 16909060: every byte distinct, so a byte-order mistake cannot pass.
	const id = 0x01020304

	tests := []struct {
		name    string
		subject string
		schema  sr.Schema
		want    []byte
	}{
		{
			name:    "avro carries no message index",
			subject: "wire-avro-value",
			schema: sr.Schema{Type: sr.TypeAvro, Schema: `{"type":"record","name":"W","namespace":"acme",` +
				`"fields":[{"name":"id","type":"string"},{"name":"total","type":"long"}]}`},
			want: []byte{
				0x00,                   // magic byte
				0x01, 0x02, 0x03, 0x04, // schema id, big endian
				0x06, 'a', 'b', 'c', // avro string "abc": zigzag length 3, then the bytes
				0x0e, // avro long 7: zigzag 14
			},
		},
		{
			name:    "protobuf inserts the message-index array",
			subject: "wire-proto-value",
			schema: sr.Schema{Type: sr.TypeProtobuf, Schema: "syntax = \"proto3\";\npackage acme;\n" +
				"message W {\n  string id = 1;\n  int64 total = 2;\n}\n"},
			want: []byte{
				0x00,                   // magic byte
				0x01, 0x02, 0x03, 0x04, // schema id, big endian
				0x00,                      // message index [0], written as the single-zero shortcut
				0x0a, 0x03, 'a', 'b', 'c', // field 1, wire type 2, length 3
				0x10, 0x07, // field 2, wire type 0, varint 7
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake, reg := newFakeRegistry(t)
			fake.SeedSchema(tt.subject, 1, id, tt.schema)

			c, err := New(context.Background(), scenario.Topic{
				Name:           "wire",
				SchemaProvider: "registry",
				SubjectName:    tt.subject,
				MessageSchema:  scenario.MessageSchema{DataFormat: formatJSON},
			}, reg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			doc := NewDoc()
			doc.Set("id", "abc")
			doc.Set("total", int64(7))

			got, err := c.Encode(doc)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("Encode = % x\nwant      % x", got, tt.want)
			}
		})
	}
}

func TestFrameStripRejectsBadHeader(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
	}{
		{"missing magic byte", []byte{0x01, 0x00, 0x00, 0x00, 0x01}},
		{"too short", []byte{0x00, 0x00}},
		{"empty", nil},
	}

	f := frame{id: 1}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := f.strip(tt.in); err == nil {
				t.Errorf("strip(% x) succeeded, want an error", tt.in)
			}
		})
	}
}
