// Package generate builds the message payloads khaos produces: single field
// values, whole documents from a `fields:` schema, synthetic size-padded JSON
// when no schema is given, Kafka keys, and correlated multi-step flow messages.
//
// Every constructor takes an explicit *rand.Rand and no generator touches the
// global source, so two generators built from equally seeded sources emit
// identical sequences.
//
// Concurrency: a generator owns its *rand.Rand and its own mutable state
// (sequence counters, cardinality cursors, round-robin index), so none of them
// are safe for concurrent use -- build one generator per producer goroutine.
package generate

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Doc is an ordered document: a JSON object that remembers the order its keys
// were first inserted in.
//
// Order is contractual, not cosmetic: Go's encoding/json sorts map keys
// alphabetically, so a map[string]any would silently reorder every message,
// breaking Avro/Protobuf field mapping that relies on declaration order.
//
// Re-Setting an existing key overwrites the value but keeps the key in its
// original position. Flow generation depends on this: correlation_id is
// inserted first with a placeholder and overwritten later, and it must stay
// the first key.
//
// The zero Doc is ready to use.
type Doc struct {
	keys []string
	vals map[string]any
}

// NewDoc returns an empty Doc with room preallocated for n keys.
//
// A Doc is a plain mutable value: safe to read from several goroutines once it is
// built, never safe to Set concurrently.
func NewDoc(n int) *Doc {
	return &Doc{
		keys: make([]string, 0, n),
		vals: make(map[string]any, n),
	}
}

// Set stores v under key. A key that is already present keeps its position.
func (d *Doc) Set(key string, v any) {
	if d.vals == nil {
		d.vals = make(map[string]any, 8)
	}
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

// Keys returns the keys in insertion order. The result is a copy; mutating it
// does not affect the document.
func (d *Doc) Keys() []string {
	out := make([]string, len(d.keys))
	copy(out, d.keys)
	return out
}

// Len returns the number of keys in the document.
func (d *Doc) Len() int { return len(d.keys) }

// MarshalJSON emits the document in insertion order, compact (no space after
// ':' or ','). See RawJSONGen for where that byte count matters.
func (d *Doc) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range d.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, err := json.Marshal(k)
		if err != nil {
			return nil, fmt.Errorf("marshal key %q: %w", k, err)
		}
		buf.Write(kb)
		buf.WriteByte(':')
		vb, err := json.Marshal(d.vals[k])
		if err != nil {
			return nil, fmt.Errorf("marshal value for key %q: %w", k, err)
		}
		buf.Write(vb)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}
