// Package codec turns generated message documents into Kafka record bytes and
// back, for the four wire shapes khaos supports: JSON, Avro (inline schema or
// Schema Registry) and Protobuf (inline schema or Schema Registry).
//
// Every string that reaches the wire -- generated Avro schema JSON, generated
// .proto source, mangled type names, the Confluent header layout -- must stay
// stable across releases, because existing consumers and existing Schema
// Registry subjects depend on them.
package codec

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Doc is an ordered string-keyed document.
//
// Go's map marshalling sorts keys, which would put JSON fields out of schema
// order; Doc keeps insertion order instead, so JSON output stays in schema
// order and Avro/Protobuf decoding can hand back fields in schema order
// rather than a random map iteration order.
//
// The zero value is not usable; call NewDoc.
type Doc struct {
	keys []string
	vals map[string]any
}

// NewDoc returns an empty Doc.
func NewDoc() *Doc {
	return &Doc{vals: make(map[string]any)}
}

// Set stores v under key. Re-setting an existing key overwrites the value and
// keeps the key's original position.
func (d *Doc) Set(key string, v any) {
	if _, ok := d.vals[key]; !ok {
		d.keys = append(d.keys, key)
	}
	d.vals[key] = v
}

// Get returns the value stored under key and whether it was present.
func (d *Doc) Get(key string) (any, bool) {
	v, ok := d.vals[key]
	return v, ok
}

// Keys returns the keys in insertion order. The returned slice is a copy and
// may be modified freely by the caller.
func (d *Doc) Keys() []string {
	out := make([]string, len(d.keys))
	copy(out, d.keys)
	return out
}

// Len returns the number of keys.
func (d *Doc) Len() int { return len(d.keys) }

// MarshalJSON writes the document as a JSON object with keys in insertion
// order.
//
// HTML escaping is disabled: encoding/json escapes '<', '>' and '&' by
// default, which would corrupt any generated string containing them.
func (d *Doc) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range d.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, err := marshalNoEscape(k)
		if err != nil {
			return nil, fmt.Errorf("marshal key %q: %w", k, err)
		}
		buf.Write(kb)
		buf.WriteByte(':')
		vb, err := marshalNoEscape(d.vals[k])
		if err != nil {
			return nil, fmt.Errorf("marshal value for key %q: %w", k, err)
		}
		buf.Write(vb)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// UnmarshalJSON parses a JSON object, preserving key order. Nested objects
// become *Doc, arrays become []any, and numbers become int64 when they are
// integral and float64 otherwise, since encoding/json's default
// float64-for-everything would turn every decoded integer into a float.
func (d *Doc) UnmarshalJSON(b []byte) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	v, err := decodeJSONValue(dec)
	if err != nil {
		return err
	}
	parsed, ok := v.(*Doc)
	if !ok {
		return fmt.Errorf("codec: expected a JSON object, got %T", v)
	}
	d.keys = parsed.keys
	d.vals = parsed.vals
	return nil
}

func marshalNoEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// decodeJSONValue reads exactly one JSON value from dec, building *Doc for
// objects so that key order survives.
func decodeJSONValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("codec: read json token: %w", err)
	}
	return decodeJSONFrom(dec, tok)
}

func decodeJSONFrom(dec *json.Decoder, tok json.Token) (any, error) {
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			doc := NewDoc()
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, fmt.Errorf("codec: read json object key: %w", err)
				}
				k, ok := kt.(string)
				if !ok {
					return nil, fmt.Errorf("codec: non-string json object key %v", kt)
				}
				v, err := decodeJSONValue(dec)
				if err != nil {
					return nil, err
				}
				doc.Set(k, v)
			}
			if _, err := dec.Token(); err != nil { // closing '}'
				return nil, fmt.Errorf("codec: read json object end: %w", err)
			}
			return doc, nil
		case '[':
			arr := []any{}
			for dec.More() {
				v, err := decodeJSONValue(dec)
				if err != nil {
					return nil, err
				}
				arr = append(arr, v)
			}
			if _, err := dec.Token(); err != nil { // closing ']'
				return nil, fmt.Errorf("codec: read json array end: %w", err)
			}
			return arr, nil
		default:
			return nil, fmt.Errorf("codec: unexpected json delimiter %q", t)
		}
	case json.Number:
		return jsonNumber(t), nil
	default:
		return tok, nil
	}
}

func jsonNumber(n json.Number) any {
	if i, err := n.Int64(); err == nil {
		return i
	}
	f, err := n.Float64()
	if err != nil {
		return n.String()
	}
	return f
}

// docToMap flattens a Doc (and any nested Docs) into plain Go maps, which is
// what the Avro encoder consumes.
func docToMap(d *Doc) map[string]any {
	m := make(map[string]any, len(d.keys))
	for _, k := range d.keys {
		m[k] = plainValue(d.vals[k])
	}
	return m
}

func plainValue(v any) any {
	switch t := v.(type) {
	case *Doc:
		return docToMap(t)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = plainValue(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = plainValue(e)
		}
		return out
	default:
		return v
	}
}
