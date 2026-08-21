package generate

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// paddingFudge is subtracted from the padding length so a padded payload
// lands about 10 bytes under its target size rather than exactly on it: the
// `,"data":""` syntax the padded key adds costs 10 bytes of its own, and this
// constant over-compensates for that by design; see RawJSONGen.Next.
const paddingFudge = 20

// RawJSONGen produces the synthetic JSON payload used when a topic's
// message_schema declares no fields: {id, timestamp, sequence} padded with an
// "x" string to approximately the requested size. Not safe for concurrent
// use: the sequence counter and the *rand.Rand are unguarded.
type RawJSONGen struct {
	minSize          int
	maxSize          int
	includeTimestamp bool
	includeSequence  bool
	r                *rand.Rand
	sequence         int64
}

// NewRawJSONGen builds the payload generator for a schema with no fields.
//
// include_timestamp and include_sequence are hardwired on: no YAML key ever
// sets them, so scenario.MessageSchema doesn't carry them and every
// scenario-driven run has both enabled.
//
// The returned generator is not safe for concurrent use: the sequence counter
// and the *rand.Rand are unguarded. Build one per producer goroutine.
//
// Degenerate bounds are clamped rather than rejected, because the constructor
// has no error return: max < min collapses to a fixed size of min. Scenario
// validation rejects those configurations before they reach here.
func NewRawJSONGen(ms scenario.MessageSchema, r *rand.Rand) *RawJSONGen {
	minSize, maxSize := ms.MinSizeBytes, ms.MaxSizeBytes
	if maxSize < minSize {
		maxSize = minSize
	}
	return &RawJSONGen{
		minSize:          minSize,
		maxSize:          maxSize,
		includeTimestamp: true,
		includeSequence:  true,
		r:                r,
	}
}

// Next returns the next payload.
//
// The size arithmetic:
//
//	current = len(json(base))              // base = {id[, timestamp][, sequence]}
//	target  = randint(min_size, max_size)
//	if current < target:
//	    padding = target - current - 20
//	    if padding > 0: doc["data"] = "x" * padding
//
// Adding the "data" key costs 10 bytes of JSON syntax (`,"data":""`), so the
// result lands ~10 bytes UNDER target rather than on it. That undershoot is
// the contract, not a bug to fix. Documents smaller than target by less than
// 20 bytes get no padding at all and stay short.
//
// `current` is measured with the same compact encoder that emits the
// payload, so the formula stays self-consistent regardless of how much JSON
// syntax overhead the encoder adds.
func (g *RawJSONGen) Next() []byte {
	g.sequence++
	seq := g.sequence

	// Drawn before the document is built so the buffer can be sized for THIS
	// message rather than for the largest one the schema permits. Exactly one
	// value is still taken from the source, and only when the bounds differ, so
	// seeded output is unchanged.
	target := g.minSize
	if g.maxSize > g.minSize {
		target = g.minSize + int(uint64Inclusive(g.r, uint64(g.maxSize)-uint64(g.minSize)))
	}

	// The document is a fixed shape of ints and generated strings, so it is
	// encoded directly: no escaping is possible and no error can occur, which
	// keeps the hot path allocation-light and free of unreachable error paths.
	var b strings.Builder
	b.Grow(growHint(target))
	fmt.Fprintf(&b, `{"id":"msg-%d"`, seq)
	if g.includeTimestamp {
		fmt.Fprintf(&b, `,"timestamp":%d`, time.Now().UnixMilli())
	}
	if g.includeSequence {
		fmt.Fprintf(&b, `,"sequence":%d`, seq)
	}
	// Closing brace is not written yet; "data" may still be appended.
	current := b.Len() + 1

	if current < target {
		if padding := target - current - paddingFudge; padding > 0 {
			b.WriteString(`,"data":"`)
			for range padding {
				b.WriteByte('x')
			}
			b.WriteByte('"')
		}
	}
	b.WriteByte('}')

	return []byte(b.String())
}

// Sequence returns the number of payloads generated so far, for tests and
// metrics.
func (g *RawJSONGen) Sequence() int64 { return g.sequence }

// maxGrowHint caps the buffer reservation for one message.
//
// Sizing the builder is only an optimisation: strings.Builder grows on
// demand, so a hint that is too small costs one reallocation. A hint that is
// too LARGE costs a whole allocation per message -- `max_size_bytes:
// 10000000` would reserve 10MB for every message even when it comes out 200
// bytes long -- and Grow panics outright on a negative count, which
// `max_size_bytes: 9223372036854775807` used to produce once the +16
// wrapped.
const maxGrowHint = 1 << 16

// growHint sizes the buffer for a message that should land near target bytes.
func growHint(target int) int {
	switch {
	case target < 0:
		return 0
	case target > maxGrowHint:
		return maxGrowHint
	default:
		return target + 16
	}
}
