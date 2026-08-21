package codec

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/hamba/avro/v2"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// avroNamespace is the namespace of every schema khaos generates.
const avroNamespace = "khaos.generated"

// avroPrimitive maps khaos field types to their Avro type name.
//
// Note that "int" maps to Avro "long" and "float" to Avro "double". That is not
// a mistake to be corrected: khaos has always written 64-bit values, and
// narrowing them now would break every consumer holding an older reader schema.
var avroPrimitive = map[string]string{
	scenario.FieldString:  "string",
	scenario.FieldInt:     "long",
	scenario.FieldFloat:   "double",
	scenario.FieldBoolean: "boolean",
	scenario.FieldFaker:   "string",
}

// AvroSchemaText renders the Avro schema khaos generates for a field list.
//
// It is both the schema used to encode and the exact text POSTed to Schema
// Registry, so any drift in key order, nesting shape, or generated type names
// is a compatibility break.
func AvroSchemaText(fields []scenario.Field, recordName string) (string, error) {
	schema := NewDoc()
	schema.Set("type", "record")
	schema.Set("name", recordName)
	schema.Set("namespace", avroNamespace)
	schema.Set("fields", avroFieldEntries(fields))

	b, err := schema.MarshalJSON()
	if err != nil {
		return "", fmt.Errorf("codec: render avro schema %q: %w", recordName, err)
	}
	return string(b), nil
}

// avroFieldEntries renders the `fields` array of a record.
func avroFieldEntries(fields []scenario.Field) []any {
	entries := make([]any, 0, len(fields))
	for _, f := range fields {
		entry := NewDoc()
		entry.Set("name", f.Name)
		entry.Set("type", avroTypeOf(f))
		entries = append(entries, entry)
	}
	return entries
}

// avroTypeOf maps a field schema to its Avro type, including its fallbacks: an
// enum without values degrades to a plain string, an object without fields to
// a map<string>, an array without items to an array of strings, and an
// unknown type to a string.
func avroTypeOf(f scenario.Field) *Doc {
	d := NewDoc()
	if prim, ok := avroPrimitive[f.Type]; ok {
		d.Set("type", prim)
		return d
	}

	switch f.Type {
	case scenario.FieldUUID:
		d.Set("type", "string")
		d.Set("logicalType", "uuid")
	case scenario.FieldTimestamp:
		d.Set("type", "long")
		d.Set("logicalType", "timestamp-millis")
	case scenario.FieldEnum:
		if len(f.Values) == 0 {
			d.Set("type", "string")
			return d
		}
		d.Set("type", "enum")
		d.Set("name", mangleAvro(f.Name)+"Enum")
		d.Set("symbols", stringsToAny(f.Values))
	case scenario.FieldObject:
		if len(f.Fields) == 0 {
			d.Set("type", "map")
			d.Set("values", "string")
			return d
		}
		d.Set("type", "record")
		d.Set("name", mangleAvro(f.Name)+"Record")
		d.Set("fields", avroFieldEntries(f.Fields))
	case scenario.FieldArray:
		d.Set("type", "array")
		if f.Items == nil {
			items := NewDoc()
			items.Set("type", "string")
			d.Set("items", items)
			return d
		}
		d.Set("items", avroTypeOf(*f.Items))
	default:
		d.Set("type", "string")
	}
	return d
}

func stringsToAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// avroCodec encodes documents as Avro binary against a fixed writer schema.
//
// With frame == nil, Encode produces bare schemaless bytes with no header.
type avroCodec struct {
	schema avro.Schema
	record *avro.RecordSchema
	frame  *frame
}

// Encode writes Avro binary, behind a Confluent header when framed.
func (c *avroCodec) Encode(d *Doc) ([]byte, error) {
	body, err := avro.Marshal(c.schema, coerceAvro(c.schema, docToMap(d)))
	if err != nil {
		return nil, fmt.Errorf("codec: avro encode: %w", err)
	}
	if c.frame == nil {
		return body, nil
	}
	return c.frame.prepend(body)
}

// Decode reads Avro binary back into a document whose keys follow the schema's
// field order, which is also the order the encoder wrote them in.
func (c *avroCodec) Decode(b []byte) (*Doc, error) {
	body := b
	if c.frame != nil {
		_, rest, err := c.frame.strip(b)
		if err != nil {
			return nil, err
		}
		body = rest
	}
	var m map[string]any
	if err := avro.Unmarshal(c.schema, body, &m); err != nil {
		return nil, fmt.Errorf("codec: avro decode: %w", err)
	}
	return avroRecordDoc(c.record, m), nil
}

// avroRecordDoc rebuilds a Doc from a decoded record, using the schema for
// ordering.
func avroRecordDoc(rec *avro.RecordSchema, m map[string]any) *Doc {
	d := NewDoc()
	for _, f := range rec.Fields() {
		v, ok := m[f.Name()]
		if !ok {
			continue
		}
		d.Set(f.Name(), avroDecodedValue(f.Type(), v))
	}
	return d
}

// avroDecodedValue converts one decoded Avro value into the Doc value model.
func avroDecodedValue(s avro.Schema, v any) any {
	switch t := derefAvro(s).(type) {
	case *avro.RecordSchema:
		if m, ok := v.(map[string]any); ok {
			return avroRecordDoc(t, m)
		}
	case *avro.ArraySchema:
		if arr, ok := v.([]any); ok {
			out := make([]any, len(arr))
			for i, e := range arr {
				out[i] = avroDecodedValue(t.Items(), e)
			}
			return out
		}
	case *avro.MapSchema:
		if m, ok := v.(map[string]any); ok {
			d := NewDoc()
			for _, k := range sortedKeys(m) {
				d.Set(k, avroDecodedValue(t.Values(), m[k]))
			}
			return d
		}
	}
	return avroScalar(v)
}

// avroScalar normalises the Go types hamba hands back for logical types.
//
// hamba decodes timestamp-millis into time.Time and time-millis into
// time.Duration, but khaos generates and encodes those fields as integers, so
// decoding must undo the conversion or a Doc would not survive a round trip.
// timestamp-micros collapses to milliseconds here; khaos never generates one,
// and only registry-supplied schemas can contain it.
func avroScalar(v any) any {
	switch t := v.(type) {
	case time.Time:
		return t.UnixMilli()
	case time.Duration:
		return t.Milliseconds()
	case map[string]any:
		d := NewDoc()
		for _, k := range sortedKeys(t) {
			d.Set(k, avroScalar(t[k]))
		}
		return d
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = avroScalar(e)
		}
		return out
	default:
		return v
	}
}

// derefAvro unwraps the reference schemas hamba creates for the second and
// later uses of a named type.
func derefAvro(s avro.Schema) avro.Schema {
	if ref, ok := s.(*avro.RefSchema); ok {
		return ref.Schema()
	}
	return s
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// parseAvro parses Avro schema text in isolation from every other schema in
// the process, using a fresh SchemaCache instead of hamba's package-level
// default.
//
// khaos generates type names from field names in a single shared namespace,
// so two unrelated topics can mint the same full name; a shared cache would
// let one topic's schema silently resolve an unqualified reference to
// another topic's type depending on build order. A per-codec cache turns
// that into a deterministic parse error naming the unresolved type instead.
func parseAvro(text string) (avro.Schema, error) {
	return avro.ParseWithCache(text, "", &avro.SchemaCache{})
}

// avroSchemaJSON parses schema text into the generic JSON form the registry
// converter walks.
func avroSchemaJSON(text string) (map[string]any, error) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		return nil, fmt.Errorf("codec: parse avro schema json: %w", err)
	}
	return raw, nil
}
