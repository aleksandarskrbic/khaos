package generate

import (
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// Defaults applied when a field omits a bound.
//
// An explicit `min_length: 0` or `max_length: 0` falls back to these defaults
// (zero means "unset" for length bounds), while `min: 0` and `max: 0` on
// numeric bounds are honoured as given.
const (
	defaultMinLength = 5
	defaultMaxLength = 20
	defaultIntMin    = 0
	defaultIntMax    = 1000
	defaultFloatMin  = 0.0
	defaultFloatMax  = 1000.0
)

// maxCardinalityAttempts bounds the rejection loop that fills the
// distinct-value cache for one cardinality slot, so a value space smaller
// than the requested cardinality -- `type: int, min: 0, max: 5, cardinality:
// 100` -- fails with a diagnosable error instead of spinning forever; see
// NewFieldGen.
const maxCardinalityAttempts = 1000

// lowercaseAlphabet is the character set generated string fields draw from.
const lowercaseAlphabet = "abcdefghijklmnopqrstuvwxyz"

// maxCachePrealloc caps how much of a cardinality cache is reserved up front.
//
// `cardinality: 9223372036854775807` validates fine (the validator only
// requires a positive integer), and make([]any, 0, that) panics with
// "makeslice: cap out of range" before a single value is drawn. The
// reservation is a pure optimisation, so it is capped rather than trusted.
const maxCachePrealloc = 1024

// uint64Inclusive draws uniformly from [0, span].
//
// The width arithmetic is done in uint64 so a span can never overflow: a
// scenario carrying `type: int, min: 0, max: 9223372036854775807` stays
// drawable instead of panicking. rand/v2 routes IntN and Uint64N through the
// same internal uint64n, so this consumes exactly the same randomness either
// way.
func uint64Inclusive(r *rand.Rand, span uint64) uint64 {
	if span == math.MaxUint64 {
		return r.Uint64()
	}
	return r.Uint64N(span + 1)
}

// maxInt64AsFloat is 2^63: the smallest float64 that is too large for an int64.
const maxInt64AsFloat = 9223372036854775808.0

// float64ToInt64 truncates toward zero, reporting whether the value is
// representable as an int64 at all.
//
// Go's float-to-int conversion is undefined outside the destination range --
// it saturates on arm64 and wraps on amd64 -- so `max: 1e30` would silently
// become a different number on different machines and then panic inside the
// range draw. Values that don't fit, including NaN and the infinities, are
// reported as an error instead; note that `min: .nan` passes schema
// validation, because every comparison against NaN is false.
func float64ToInt64(v float64) (int64, bool) {
	if math.IsNaN(v) || v >= maxInt64AsFloat || v < -maxInt64AsFloat {
		return 0, false
	}
	return int64(v), true
}

// NewFieldGen returns a generator for one field of a message schema.
//
// All construction errors are reported here, including ones that would
// otherwise only surface at generate time (min > max, an impossible
// cardinality), because the returned closure has no way to report anything.
//
// The returned function is not safe for concurrent use.
func NewFieldGen(f scenario.Field, r *rand.Rand, opts ...Option) (func() any, error) {
	return newFieldGen(f, r, resolve(opts))
}

// Option configures generator construction.
//
// These are options rather than package-level variables on purpose: a mutable
// global read during construction is a data race waiting to happen, and the
// race detector caught exactly that failure mode once.
type Option func(*options)

type options struct {
	// maxFillAttempts bounds the cardinality rejection loop. Zero means unbounded.
	maxFillAttempts int
}

func resolve(opts []Option) options {
	var o options
	for _, apply := range opts {
		apply(&o)
	}
	return o
}

// BoundFillAttempts makes an impossible cardinality an error instead of a hang.
//
// A field can ask for more distinct values than its value space can supply --
// `type: int, min: 0, max: 5, cardinality: 100`. By default the fill loop
// rejects duplicates forever and the process hangs with no error and no
// output; pass this option to give up after n consecutive failed draws and
// return a diagnosable error instead.
func BoundFillAttempts(n int) Option {
	return func(o *options) { o.maxFillAttempts = n }
}

func newFieldGen(f scenario.Field, r *rand.Rand, o options) (func() any, error) {
	switch f.Type {
	case scenario.FieldString:
		return newStringGen(f, r, o)
	case scenario.FieldInt:
		return newIntGen(f, r, o)
	case scenario.FieldFloat:
		return newFloatGen(f, r, o)
	case scenario.FieldBoolean:
		return func() any { return r.IntN(2) == 1 }, nil
	case scenario.FieldUUID:
		return func() any { return uuid4(r) }, nil
	case scenario.FieldTimestamp:
		// Emits epoch milliseconds as an int, despite the README documenting
		// ISO-8601 for this type -- the schema contract is whatever this
		// function returns.
		return func() any { return time.Now().UnixMilli() }, nil
	case scenario.FieldEnum:
		return newEnumGen(f, r, o)
	case scenario.FieldObject:
		return newObjectGen(f, r, o)
	case scenario.FieldArray:
		return newArrayGen(f, r, o)
	case scenario.FieldFaker:
		return newFakerGen(f, r, o)
	default:
		return nil, fmt.Errorf("field %q: unknown field type: %s", f.Name, f.Type)
	}
}

// newStringGen builds the generator for a `type: string` field.
func newStringGen(f scenario.Field, r *rand.Rand, o options) (func() any, error) {
	minLen := orDefaultInt(f.MinLength, defaultMinLength)
	maxLen := orDefaultInt(f.MaxLength, defaultMaxLength)
	if minLen < 0 {
		return nil, fmt.Errorf("field %q: min_length must be >= 0, got %d", f.Name, minLen)
	}
	if maxLen < minLen {
		return nil, fmt.Errorf("field %q: max_length (%d) must be >= min_length (%d)", f.Name, maxLen, minLen)
	}

	span := uint64(maxLen) - uint64(minLen)
	draw := func() any {
		length := minLen + int(uint64Inclusive(r, span))
		var b strings.Builder
		b.Grow(length)
		for range length {
			b.WriteByte(lowercaseAlphabet[r.IntN(len(lowercaseAlphabet))])
		}
		return b.String()
	}
	return withCardinality(f, draw, o)
}

// newIntGen builds the generator for a `type: int` field.
func newIntGen(f scenario.Field, r *rand.Rand, o options) (func() any, error) {
	minVal := int64(defaultIntMin)
	if f.Min != nil {
		v, ok := float64ToInt64(*f.Min)
		if !ok {
			return nil, fmt.Errorf("field %q: min (%g) is not representable as a 64-bit integer", f.Name, *f.Min)
		}
		minVal = v
	}
	maxVal := int64(defaultIntMax)
	if f.Max != nil {
		v, ok := float64ToInt64(*f.Max)
		if !ok {
			return nil, fmt.Errorf("field %q: max (%g) is not representable as a 64-bit integer", f.Name, *f.Max)
		}
		maxVal = v
	}
	if maxVal < minVal {
		return nil, fmt.Errorf("field %q: max (%d) must be >= min (%d)", f.Name, maxVal, minVal)
	}
	// No impossible-cardinality pre-check here: that only surfaces from the
	// fill loop. See BoundFillAttempts.

	// The span is a uint64 count, so even min=MinInt64/max=MaxInt64 is drawable; the
	// offset is added in uint64 too, because minVal+int64(offset) would overflow.
	span := uint64(maxVal) - uint64(minVal)
	draw := func() any {
		return int64(uint64(minVal) + uint64Inclusive(r, span))
	}
	return withCardinality(f, draw, o)
}

// newFloatGen builds the generator for a `type: float` field. Cardinality is
// accepted in the schema but ignored for float fields.
func newFloatGen(f scenario.Field, r *rand.Rand, o options) (func() any, error) {
	minVal := defaultFloatMin
	if f.Min != nil {
		minVal = *f.Min
	}
	maxVal := defaultFloatMax
	if f.Max != nil {
		maxVal = *f.Max
	}
	if maxVal < minVal {
		return nil, fmt.Errorf("field %q: max (%g) must be >= min (%g)", f.Name, maxVal, minVal)
	}
	return func() any {
		return round2(minVal + (maxVal-minVal)*r.Float64())
	}, nil
}

// newEnumGen builds the generator for a `type: enum` field. Duplicate entries
// are the documented way to weight an enum, so the list is used as-is.
func newEnumGen(f scenario.Field, r *rand.Rand, o options) (func() any, error) {
	if len(f.Values) == 0 {
		return nil, fmt.Errorf("enum field %q requires 'values' list", f.Name)
	}
	values := make([]string, len(f.Values))
	copy(values, f.Values)
	return func() any { return values[r.IntN(len(values))] }, nil
}

// newObjectGen builds the generator for a `type: object` field. Nested fields
// keep declaration order, so the nested object serialises in YAML order too.
func newObjectGen(f scenario.Field, r *rand.Rand, o options) (func() any, error) {
	if len(f.Fields) == 0 {
		return nil, fmt.Errorf("object field %q requires 'fields' list", f.Name)
	}
	inner, err := newDocGen(f.Fields, r, o)
	if err != nil {
		return nil, fmt.Errorf("object field %q: %w", f.Name, err)
	}
	return func() any { return inner.Next() }, nil
}

// newArrayGen builds the generator for a `type: array` field.
func newArrayGen(f scenario.Field, r *rand.Rand, o options) (func() any, error) {
	if f.Items == nil {
		return nil, fmt.Errorf("array field %q requires 'items' schema", f.Name)
	}
	item, err := newFieldGen(*f.Items, r, o)
	if err != nil {
		return nil, fmt.Errorf("array field %q: %w", f.Name, err)
	}
	minItems, maxItems := f.MinItems, f.MaxItems
	if minItems < 0 {
		return nil, fmt.Errorf("array field %q: min_items must be >= 0, got %d", f.Name, minItems)
	}
	if maxItems < minItems {
		return nil, fmt.Errorf("array field %q: max_items (%d) must be >= min_items (%d)", f.Name, maxItems, minItems)
	}
	span := uint64(maxItems) - uint64(minItems)
	return func() any {
		count := minItems + int(uint64Inclusive(r, span))
		out := make([]any, count)
		for i := range out {
			out[i] = item()
		}
		return out
	}, nil
}

// withCardinality wraps draw with a distinct-value cache.
//
// Contract: the first N values are N DISTINCT random draws, and every value
// after that is cache[index % cardinality] -- strict round-robin in
// first-generated order.
//
// The cache is filled eagerly, at construction, rather than lazily on the
// first N calls, because a bounded fill loop needs to be able to report
// failure, and the closure NewFieldGen returns cannot return an error.
func withCardinality(f scenario.Field, draw func() any, o options) (func() any, error) {
	card := cardinalityOf(f)
	if card < 0 {
		// Report a negative cardinality at construction rather than failing
		// unpredictably on first use.
		return nil, fmt.Errorf("field %q: cardinality must be >= 0, got %d", f.Name, card)
	}
	if card == 0 {
		// Absent and 0 both mean "unbounded".
		return draw, nil
	}

	prealloc := min(card, maxCachePrealloc)
	cache := make([]any, 0, prealloc)
	seen := make(map[any]struct{}, prealloc)
	for len(cache) < card {
		attempts := 0
		for {
			v := draw()
			if _, dup := seen[v]; !dup {
				seen[v] = struct{}{}
				cache = append(cache, v)
				break
			}
			attempts++
			if o.maxFillAttempts > 0 && attempts >= o.maxFillAttempts {
				return nil, fmt.Errorf(
					"field %q: cannot reach cardinality %d: no new distinct value after %d attempts (had %d); the value space is too small",
					f.Name, card, o.maxFillAttempts, len(cache))
			}
		}
	}

	idx := 0
	return func() any {
		v := cache[idx%card]
		idx++
		return v
	}, nil
}

func cardinalityOf(f scenario.Field) int {
	if f.Cardinality == nil {
		return 0
	}
	return *f.Cardinality
}

// orDefaultInt returns def when p is nil or points at zero; nil and 0 both
// mean "unset".
func orDefaultInt(p *int, def int) int {
	if p == nil || *p == 0 {
		return def
	}
	return *p
}

// round2 rounds to 2 decimals using round-half-to-even ("banker's rounding")
// on the exact decimal expansion of the double, so round2(0.125) == 0.12, not
// the 0.13 math.Round would give. Multiplying by 100 and using
// math.RoundToEven doesn't fix this either -- the multiply itself rounds,
// turning 2.675 into exactly 267.5 -- so this formats to two decimals and
// parses back instead, letting strconv apply the same tie-to-even rule to the
// original decimal expansion.
func round2(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return v
	}
	r, err := strconv.ParseFloat(strconv.FormatFloat(v, 'f', 2, 64), 64)
	if err != nil {
		// Unreachable: the input is a fixed-notation float this package just
		// produced. Returning the unrounded value is better than losing it.
		return v
	}
	return r
}

// uuid4 formats a random version-4 UUID (RFC 4122, variant 10x) drawn from r,
// so seeded runs are reproducible.
func uuid4(r *rand.Rand) string {
	var b [16]byte
	hi, lo := r.Uint64(), r.Uint64()
	for i := range 8 {
		b[i] = byte(hi >> (8 * i))
		b[8+i] = byte(lo >> (8 * i))
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10

	const hexDigits = "0123456789abcdef"
	out := make([]byte, 36)
	pos := 0
	for i, x := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out[pos] = '-'
			pos++
		}
		out[pos] = hexDigits[x>>4]
		out[pos+1] = hexDigits[x&0x0f]
		pos += 2
	}
	return string(out)
}
