package codec

import (
	"time"

	"github.com/hamba/avro/v2"
)

// coerceAvro walks a value alongside its writer schema and converts Go types
// into the exact types hamba's encoder accepts.
//
// hamba rejects int64 for "int", float64 for "float" and string for "bytes"
// outright. Generated khaos schemas only ever use long, double, string and
// boolean so they are unaffected, but a registry-supplied schema routinely
// uses the narrow types, and the generator has no idea which Avro type its
// int field will land in.
//
// Anything unrecognised is passed through untouched so that hamba reports the
// real type error rather than this function masking it.
func coerceAvro(s avro.Schema, v any) any {
	switch s := s.(type) {
	case *avro.RefSchema:
		return coerceAvro(s.Schema(), v)

	case *avro.UnionSchema:
		if v == nil {
			return nil
		}
		// The generator only ever produces the non-null branch, which is also
		// the branch the registry converter derived the field type from.
		for _, t := range s.Types() {
			if t.Type() != avro.Null {
				return coerceAvro(t, v)
			}
		}
		return v

	case *avro.RecordSchema:
		m, ok := v.(map[string]any)
		if !ok {
			return v
		}
		out := make(map[string]any, len(m))
		for k, e := range m {
			out[k] = e
		}
		for _, f := range s.Fields() {
			if e, ok := out[f.Name()]; ok {
				out[f.Name()] = coerceAvro(f.Type(), e)
			}
		}
		return out

	case *avro.ArraySchema:
		items, ok := v.([]any)
		if !ok {
			return v
		}
		out := make([]any, len(items))
		for i, e := range items {
			out[i] = coerceAvro(s.Items(), e)
		}
		return out

	case *avro.MapSchema:
		m, ok := v.(map[string]any)
		if !ok {
			return v
		}
		out := make(map[string]any, len(m))
		for k, e := range m {
			out[k] = coerceAvro(s.Values(), e)
		}
		return out
	}

	switch s.Type() {
	case avro.Int:
		if i, ok := toInt64(v); ok {
			return int(i)
		}
	case avro.Long:
		// A timestamp-millis long takes either epoch millis (what khaos
		// generates) or a time.Time; leave time.Time for hamba to handle.
		if _, ok := v.(time.Time); ok {
			return v
		}
		if i, ok := toInt64(v); ok {
			return i
		}
	case avro.Float:
		if f, ok := toFloat64(v); ok {
			return float32(f)
		}
	case avro.Double:
		if f, ok := toFloat64(v); ok {
			return f
		}
	case avro.Bytes:
		if s, ok := v.(string); ok {
			return []byte(s)
		}
	}
	return v
}
