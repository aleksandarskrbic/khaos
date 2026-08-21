package generate

import (
	"math"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// twoP62 is the largest power of two that a float64 can carry exactly, leaving
// room for a span wider than int64 when mirrored around zero.
const twoP62 = 1 << 62

// Bounds that pass `khaos validate` must not panic in the range draw.
func TestExtremeBoundsDoNotPanic(t *testing.T) {
	t.Run("int span wider than int64", func(t *testing.T) {
		gen := mustFieldGen(t, scenario.Field{
			Name: "i", Type: scenario.FieldInt,
			Min: floatPtr(-twoP62), Max: floatPtr(twoP62),
		}, testRand())

		var negative, positive int
		for range 500 {
			v, ok := gen().(int64)
			if !ok {
				t.Fatalf("int field produced %T, want int64", gen())
			}
			if v < -twoP62 || v > twoP62 {
				t.Fatalf("value %d outside [%d, %d]", v, int64(-twoP62), int64(twoP62))
			}
			if v < 0 {
				negative++
			} else {
				positive++
			}
		}
		if negative == 0 || positive == 0 {
			t.Errorf("got %d negative and %d positive draws; want both signs", negative, positive)
		}
	})

	t.Run("array item span wider than int", func(t *testing.T) {
		// min_items: 0 is honoured, so min_items: 0, max_items: MaxInt is expressible.
		_, err := NewFieldGen(scenario.Field{
			Name: "a", Type: scenario.FieldArray, MinItems: 0, MaxItems: math.MaxInt,
			Items: &scenario.Field{Name: "b", Type: scenario.FieldBoolean},
		}, testRand())
		if err != nil {
			t.Fatalf("NewFieldGen: %v", err)
		}
		// Deliberately not called: the count it rolls is astronomical, and
		// construction not panicking is the contract.
	})
}

// uint64Inclusive must cover the whole span, including the degenerate full-width one
// that no float64-typed schema bound can reach.
func TestUint64Inclusive(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))

	if got := uint64Inclusive(r, 0); got != 0 {
		t.Errorf("span 0 produced %d, want 0", got)
	}

	seen := map[uint64]bool{}
	for range 200 {
		v := uint64Inclusive(r, 2)
		if v > 2 {
			t.Fatalf("span 2 produced %d", v)
		}
		seen[v] = true
	}
	if len(seen) != 3 {
		t.Errorf("span 2 produced %d distinct values, want 3", len(seen))
	}

	// The full-width branch must not overflow span+1 to zero and panic in Uint64N.
	for range 100 {
		_ = uint64Inclusive(r, math.MaxUint64)
	}
}

// A seeded run's output must be unaffected by whether ranges are drawn via
// IntN or Uint64N.
func TestSeededOutputUnchangedByWidthArithmetic(t *testing.T) {
	fields := []scenario.Field{
		{Name: "s", Type: scenario.FieldString, MinLength: intPtr(3), MaxLength: intPtr(9)},
		{Name: "i", Type: scenario.FieldInt, Min: floatPtr(-50), Max: floatPtr(50)},
		{Name: "a", Type: scenario.FieldArray, MinItems: 1, MaxItems: 4,
			Items: &scenario.Field{Name: "n", Type: scenario.FieldInt, Min: floatPtr(0), Max: floatPtr(9)}},
	}

	draw := func() []string {
		gen, err := NewDocGen(fields, testRand())
		if err != nil {
			t.Fatalf("NewDocGen: %v", err)
		}
		var out []string
		for range 20 {
			b, err := gen.Next().MarshalJSON()
			if err != nil {
				t.Fatalf("MarshalJSON: %v", err)
			}
			out = append(out, string(b))
		}
		return out
	}

	first, second := draw(), draw()
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("document %d differs between equally seeded runs:\n %s\n %s", i, first[i], second[i])
		}
	}
	// A different seed must produce different documents, or the "determinism" above
	// would be vacuous.
	other, err := NewDocGen(fields, rand.New(rand.NewPCG(99, 99)))
	if err != nil {
		t.Fatalf("NewDocGen: %v", err)
	}
	b, err := other.Next().MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	if string(b) == first[0] {
		t.Errorf("a different seed produced the same first document: %s", b)
	}
}

// A bound too large for int64 is a construction error, not a platform-dependent
// silently wrong number.
func TestUnrepresentableNumericBoundsError(t *testing.T) {
	tests := []struct {
		name    string
		field   scenario.Field
		wantSub string
	}{
		{
			name:    "max beyond int64",
			field:   scenario.Field{Name: "i", Type: scenario.FieldInt, Min: floatPtr(0), Max: floatPtr(1e30)},
			wantSub: `field "i": max (1e+30) is not representable as a 64-bit integer`,
		},
		{
			name:    "min beyond int64",
			field:   scenario.Field{Name: "i", Type: scenario.FieldInt, Min: floatPtr(-1e30), Max: floatPtr(0)},
			wantSub: `field "i": min (-1e+30) is not representable as a 64-bit integer`,
		},
		{
			// `min: .nan` reaches here because every comparison against NaN is false,
			// so the validator's min <= max check waves it through.
			name:    "NaN bound",
			field:   scenario.Field{Name: "i", Type: scenario.FieldInt, Min: floatPtr(math.NaN())},
			wantSub: "not representable",
		},
		{
			name:    "infinite bound",
			field:   scenario.Field{Name: "i", Type: scenario.FieldInt, Max: floatPtr(math.Inf(1))},
			wantSub: "not representable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewFieldGen(tt.field, testRand())
			if err == nil {
				t.Fatal("want an error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantSub)
			}
		})
	}
}

// float64ToInt64 truncates toward zero, not toward negative infinity.
func TestFloat64ToInt64(t *testing.T) {
	tests := []struct {
		in     float64
		want   int64
		wantOK bool
	}{
		{in: 0, want: 0, wantOK: true},
		{in: 7.9, want: 7, wantOK: true},
		{in: -7.9, want: -7, wantOK: true}, // toward zero, not floor
		{in: math.MinInt64, want: math.MinInt64, wantOK: true},
		{in: 9223372036854775807, wantOK: false}, // rounds up to 2^63 as a float64
		{in: 1e30, wantOK: false},
		{in: -1e30, wantOK: false},
		{in: math.NaN(), wantOK: false},
		{in: math.Inf(1), wantOK: false},
		{in: math.Inf(-1), wantOK: false},
	}

	for _, tt := range tests {
		got, ok := float64ToInt64(tt.in)
		if ok != tt.wantOK {
			t.Errorf("float64ToInt64(%v) ok = %v, want %v", tt.in, ok, tt.wantOK)
			continue
		}
		if ok && got != tt.want {
			t.Errorf("float64ToInt64(%v) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

// A cardinality larger than any machine can hold must fail in the fill loop,
// not panic while preallocating the cache.
func TestAbsurdCardinalityDoesNotPanicInMake(t *testing.T) {
	_, err := NewFieldGen(scenario.Field{
		Name: "s", Type: scenario.FieldString,
		MinLength: intPtr(1), MaxLength: intPtr(1),
		Cardinality: intPtr(math.MaxInt),
	}, testRand(), BoundFillAttempts(32))
	if err == nil {
		t.Fatal("want an error for an unreachable cardinality, got nil")
	}
	if !strings.Contains(err.Error(), "cannot reach cardinality") {
		t.Errorf("error = %q, want a cardinality error", err)
	}
}

// Extreme size bounds must not panic in strings.Builder.Grow.
func TestRawJSONGenExtremeSizeBounds(t *testing.T) {
	tests := []struct {
		name   string
		schema scenario.MessageSchema
	}{
		{"zero-width bounds", scenario.MessageSchema{}},
		{"negative bounds are clamped, not fatal", scenario.MessageSchema{MinSizeBytes: -5, MaxSizeBytes: -5}},
		{"inverted bounds collapse to min", scenario.MessageSchema{MinSizeBytes: 300, MaxSizeBytes: 10}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewRawJSONGen(tt.schema, testRand())
			if len(g.Next()) == 0 {
				t.Error("empty payload")
			}
		})
	}
}

func TestGrowHint(t *testing.T) {
	tests := []struct {
		in, want int
	}{
		{in: -1, want: 0},
		{in: 0, want: 16},
		{in: 200, want: 216},
		{in: math.MaxInt, want: maxGrowHint},
		{in: maxGrowHint + 1, want: maxGrowHint},
	}
	for _, tt := range tests {
		if got := growHint(tt.in); got != tt.want {
			t.Errorf("growHint(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}
}
