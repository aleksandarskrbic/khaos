package generate

import (
	"fmt"
	"math/rand/v2"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// DocGen builds one document per call from a list of field definitions.
//
// Generation stops at the ordered document; serialisation is the caller's
// job, so JSON, Avro and Protobuf all consume the same *Doc.
//
// Field generators are created once, at construction, so per-field state --
// most importantly the cardinality cache -- is shared across every document
// this generator produces. Not safe for concurrent use.
type DocGen struct {
	names []string
	gens  []func() any
}

// NewDocGen creates a generator for the given fields, in declaration order.
//
// An empty field list is legal and yields empty documents.
//
// The returned generator is not safe for concurrent use: it owns the
// *rand.Rand and every field's cardinality cache. Build one per producer
// goroutine.
func NewDocGen(fields []scenario.Field, r *rand.Rand, opts ...Option) (*DocGen, error) {
	return newDocGen(fields, r, resolve(opts))
}

// newDocGen is the internal constructor, taking already-resolved options so that nested
// object fields inherit them instead of silently reverting to the defaults.
func newDocGen(fields []scenario.Field, r *rand.Rand, o options) (*DocGen, error) {
	g := &DocGen{
		names: make([]string, len(fields)),
		gens:  make([]func() any, len(fields)),
	}
	for i, f := range fields {
		gen, err := newFieldGen(f, r, o)
		if err != nil {
			return nil, fmt.Errorf("field %d (%q): %w", i, f.Name, err)
		}
		g.names[i] = f.Name
		g.gens[i] = gen
	}
	return g, nil
}

// Next generates one document. Keys appear in field-declaration order.
//
// Duplicate field names collapse to one key holding the last generated value,
// but every generator still runs, so a duplicated name still advances its own
// cardinality cursor.
func (g *DocGen) Next() *Doc {
	d := NewDoc(len(g.names))
	for i, name := range g.names {
		d.Set(name, g.gens[i]())
	}
	return d
}

// Fields returns the field names this generator emits, in declaration order.
func (g *DocGen) Fields() []string {
	out := make([]string, len(g.names))
	copy(out, g.names)
	return out
}
