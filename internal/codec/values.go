package codec

import (
	"encoding/json"
	"sort"
)

// toInt64 converts any Go numeric (or a JSON number) to int64. Generated values
// arrive as int, int64 or float64 depending on which generator produced them
// and whether they have been through a JSON round trip, so every codec accepts
// all of them rather than demanding one.
func toInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int8:
		return int64(t), true
	case int16:
		return int64(t), true
	case int32:
		return int64(t), true
	case int64:
		return t, true
	case uint:
		return int64(t), true
	case uint8:
		return int64(t), true
	case uint16:
		return int64(t), true
	case uint32:
		return int64(t), true
	case uint64:
		return int64(t), true
	case float32:
		return int64(t), true
	case float64:
		return int64(t), true
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	case json.Number:
		i, err := t.Int64()
		if err != nil {
			f, ferr := t.Float64()
			if ferr != nil {
				return 0, false
			}
			return int64(f), true
		}
		return i, true
	default:
		return 0, false
	}
}

// toUint64 converts any Go numeric to uint64.
func toUint64(v any) (uint64, bool) {
	if u, ok := v.(uint64); ok {
		return u, true
	}
	i, ok := toInt64(v)
	if !ok {
		return 0, false
	}
	return uint64(i), true
}

// toFloat64 converts any Go numeric (or a JSON number) to float64.
func toFloat64(v any) (float64, bool) {
	switch t := v.(type) {
	case float32:
		return float64(t), true
	case float64:
		return t, true
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		i, ok := toInt64(v)
		if !ok {
			return 0, false
		}
		return float64(i), true
	}
}

// toSlice normalises the list shapes a document can hold.
func toSlice(v any) ([]any, bool) {
	switch t := v.(type) {
	case []any:
		return t, true
	case []string:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = e
		}
		return out, true
	default:
		return nil, false
	}
}

// toFields normalises the object shapes a document can hold into ordered
// key/value pairs.
func toFields(v any) ([]string, map[string]any, bool) {
	switch t := v.(type) {
	case *Doc:
		return t.keys, t.vals, true
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys) // map order is random; encoding must not be
		return keys, t, true
	default:
		return nil, nil, false
	}
}
