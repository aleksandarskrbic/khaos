package generate

import (
	"encoding/json"
	"math/rand/v2"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

func testRand() *rand.Rand { return rand.New(rand.NewPCG(0xC0FFEE, 0xBADF00D)) }

func intPtr(v int) *int           { return &v }
func floatPtr(v float64) *float64 { return &v }

func mustFieldGen(t *testing.T, f scenario.Field, r *rand.Rand) func() any {
	t.Helper()
	g, err := NewFieldGen(f, r)
	if err != nil {
		t.Fatalf("NewFieldGen(%+v): %v", f, err)
	}
	return g
}

// String field values respect min/max length and use the lowercase alphabet.
func TestStringField(t *testing.T) {
	tests := []struct {
		name    string
		field   scenario.Field
		wantMin int
		wantMax int
	}{
		{
			name:    "length within range",
			field:   scenario.Field{Name: "s", Type: scenario.FieldString, MinLength: intPtr(5), MaxLength: intPtr(10)},
			wantMin: 5,
			wantMax: 10,
		},
		{
			name:    "exact length",
			field:   scenario.Field{Name: "s", Type: scenario.FieldString, MinLength: intPtr(8), MaxLength: intPtr(8)},
			wantMin: 8,
			wantMax: 8,
		},
		{
			// An explicit 0 falls back to the default; zero means "unset".
			name:    "zero bounds fall back to defaults",
			field:   scenario.Field{Name: "s", Type: scenario.FieldString, MinLength: intPtr(0), MaxLength: intPtr(0)},
			wantMin: defaultMinLength,
			wantMax: defaultMaxLength,
		},
		{
			name:    "unset bounds fall back to defaults",
			field:   scenario.Field{Name: "s", Type: scenario.FieldString},
			wantMin: defaultMinLength,
			wantMax: defaultMaxLength,
		},
	}

	lowercase := regexp.MustCompile(`^[a-z]*$`)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gen := mustFieldGen(t, tt.field, testRand())
			for range 200 {
				v, ok := gen().(string)
				if !ok {
					t.Fatalf("value is not a string")
				}
				if len(v) < tt.wantMin || len(v) > tt.wantMax {
					t.Fatalf("len(%q) = %d, want within [%d, %d]", v, len(v), tt.wantMin, tt.wantMax)
				}
				if !lowercase.MatchString(v) {
					t.Fatalf("value %q is not lowercase ascii", v)
				}
			}
		})
	}
}

// A cardinality-bounded field produces N distinct values, then strict
// round-robin in first-generated order.
func TestCardinality(t *testing.T) {
	tests := []struct {
		name  string
		field scenario.Field
	}{
		{
			name:  "string",
			field: scenario.Field{Name: "s", Type: scenario.FieldString, Cardinality: intPtr(5)},
		},
		{
			name:  "int",
			field: scenario.Field{Name: "i", Type: scenario.FieldInt, Min: floatPtr(0), Max: floatPtr(1000), Cardinality: intPtr(10)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			card := *tt.field.Cardinality
			gen := mustFieldGen(t, tt.field, testRand())

			first := make([]any, card)
			distinct := make(map[any]struct{}, card)
			for i := range first {
				first[i] = gen()
				distinct[first[i]] = struct{}{}
			}
			if len(distinct) != card {
				t.Fatalf("first %d values held %d distinct, want %d", card, len(distinct), card)
			}

			// Every subsequent value is cache[index % cardinality], so three more
			// cycles must repeat the first cycle exactly.
			for cycle := range 3 {
				for i := range first {
					if got := gen(); got != first[i] {
						t.Fatalf("cycle %d position %d = %v, want %v", cycle+1, i, got, first[i])
					}
				}
			}

			// And the total distinct set never grows.
			for range 200 {
				if _, ok := distinct[gen()]; !ok {
					t.Fatal("generated a value outside the cardinality cache")
				}
			}
		})
	}
}

// An impossible cardinality (value space smaller than requested) is reported
// as an error rather than left to hang.
func TestCardinalityImpossibleValueSpace(t *testing.T) {
	tests := []struct {
		name    string
		field   scenario.Field
		wantSub string
	}{
		{
			name:  "int range too small",
			field: scenario.Field{Name: "i", Type: scenario.FieldInt, Min: floatPtr(0), Max: floatPtr(5), Cardinality: intPtr(100)},
			// There is no impossible-cardinality pre-check: both cases surface
			// from the fill loop.
			wantSub: "cannot reach cardinality",
		},
		{
			name:    "single character strings",
			field:   scenario.Field{Name: "s", Type: scenario.FieldString, MinLength: intPtr(1), MaxLength: intPtr(1), Cardinality: intPtr(30)},
			wantSub: "cannot reach cardinality",
		},
	}

	// The default fill loop is unbounded, so an impossible cardinality spins
	// forever; that default is deliberately NOT exercised here. An earlier
	// version of this test "proved the hang" by launching a goroutine and
	// asserting it never returned, which left a goroutine spinning inside
	// withCardinality for the rest of the test binary. Asserting a hang means
	// leaking the hang, so only the bounded path is tested below.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewFieldGen(tt.field, testRand(), BoundFillAttempts(64))
			if err == nil {
				t.Fatal("want an error for an impossible cardinality, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantSub)
			}
		})
	}
}

// Options must reach nested object fields and array items, not just the top level.
func TestBoundFillAttemptsReachesNestedFields(t *testing.T) {
	impossible := scenario.Field{
		Name: "n", Type: scenario.FieldInt,
		Min: floatPtr(0), Max: floatPtr(5), Cardinality: intPtr(100),
	}

	nested := scenario.Field{
		Name: "obj", Type: scenario.FieldObject,
		Fields: []scenario.Field{impossible},
	}
	if _, err := NewFieldGen(nested, testRand(), BoundFillAttempts(64)); err == nil {
		t.Error("nested object field ignored the bound and would have hung")
	}

	arr := scenario.Field{
		Name: "arr", Type: scenario.FieldArray,
		Items: &impossible, MinItems: 1, MaxItems: 2,
	}
	if _, err := NewFieldGen(arr, testRand(), BoundFillAttempts(64)); err == nil {
		t.Error("array item schema ignored the bound and would have hung")
	}

	if _, err := NewDocGen([]scenario.Field{nested}, testRand(), BoundFillAttempts(64)); err == nil {
		t.Error("NewDocGen did not propagate the bound")
	}
}

// A negative cardinality is rejected outright in both implementations.
func TestNegativeCardinalityErrors(t *testing.T) {
	_, err := NewFieldGen(
		scenario.Field{Name: "s", Type: scenario.FieldString, Cardinality: intPtr(-1)},
		testRand(),
	)
	if err == nil || !strings.Contains(err.Error(), "cardinality must be >= 0") {
		t.Fatalf("err = %v, want a cardinality error", err)
	}
}

// Int field values respect min/max, and an explicit zero bound is honoured.
func TestIntField(t *testing.T) {
	tests := []struct {
		name             string
		field            scenario.Field
		wantMin, wantMax int64
	}{
		{
			name:    "value within range",
			field:   scenario.Field{Name: "i", Type: scenario.FieldInt, Min: floatPtr(10), Max: floatPtr(20)},
			wantMin: 10, wantMax: 20,
		},
		{
			name:    "exact value",
			field:   scenario.Field{Name: "i", Type: scenario.FieldInt, Min: floatPtr(42), Max: floatPtr(42)},
			wantMin: 42, wantMax: 42,
		},
		{
			name:    "negative range",
			field:   scenario.Field{Name: "i", Type: scenario.FieldInt, Min: floatPtr(-100), Max: floatPtr(-50)},
			wantMin: -100, wantMax: -50,
		},
		{
			name:    "defaults",
			field:   scenario.Field{Name: "i", Type: scenario.FieldInt},
			wantMin: defaultIntMin, wantMax: defaultIntMax,
		},
		{
			// `min: 0` is honoured, unlike min_length, so this must not
			// silently become the default.
			name:    "explicit zero minimum is honoured",
			field:   scenario.Field{Name: "i", Type: scenario.FieldInt, Min: floatPtr(0), Max: floatPtr(0)},
			wantMin: 0, wantMax: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gen := mustFieldGen(t, tt.field, testRand())
			for range 200 {
				v, ok := gen().(int64)
				if !ok {
					t.Fatalf("value is not an int64")
				}
				if v < tt.wantMin || v > tt.wantMax {
					t.Fatalf("value %d outside [%d, %d]", v, tt.wantMin, tt.wantMax)
				}
			}
		})
	}
}

// Float field values respect min/max and are rounded to 2 decimals.
func TestFloatField(t *testing.T) {
	f := scenario.Field{Name: "f", Type: scenario.FieldFloat, Min: floatPtr(1.5), Max: floatPtr(5.5)}
	gen := mustFieldGen(t, f, testRand())
	for range 500 {
		v, ok := gen().(float64)
		if !ok {
			t.Fatalf("value is not a float64")
		}
		if v < 1.5 || v > 5.5 {
			t.Fatalf("value %v outside [1.5, 5.5]", v)
		}
		if v != round2(v) {
			t.Fatalf("value %v is not rounded to 2 decimals", v)
		}
	}
}

// round2 uses round-half-to-even; math.Round would disagree on exact .5 cases.
func TestRound2UsesBankersRounding(t *testing.T) {
	tests := []struct {
		in   float64
		want float64
	}{
		{0.125, 0.12},   // math.Round would give 0.13
		{0.375, 0.38},   // ties go to the even digit, which is up here
		{-0.125, -0.12}, // and symmetric for negatives
		{2.675, 2.67},   // the classic float-representation case
		{0.135, 0.14},
		{1.005, 1.0},
		{3.14159, 3.14},
	}

	for _, tt := range tests {
		if got := round2(tt.in); got != tt.want {
			t.Errorf("round2(%v) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

// Boolean fields produce both true and false over enough draws.
func TestBooleanField(t *testing.T) {
	gen := mustFieldGen(t, scenario.Field{Name: "b", Type: scenario.FieldBoolean}, testRand())
	seen := map[bool]bool{}
	for range 100 {
		v, ok := gen().(bool)
		if !ok {
			t.Fatal("value is not a bool")
		}
		seen[v] = true
	}
	if !seen[true] || !seen[false] {
		t.Errorf("100 draws produced only %v", seen)
	}
}

// UUID fields produce unique, well-shaped version-4 UUIDs.
func TestUUIDField(t *testing.T) {
	gen := mustFieldGen(t, scenario.Field{Name: "u", Type: scenario.FieldUUID}, testRand())
	shape := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	seen := make(map[string]bool, 1000)
	for range 1000 {
		v, ok := gen().(string)
		if !ok {
			t.Fatal("value is not a string")
		}
		if !shape.MatchString(v) {
			t.Fatalf("%q is not a version-4 UUID", v)
		}
		if seen[v] {
			t.Fatalf("duplicate uuid %q", v)
		}
		seen[v] = true
	}
}

// Timestamp fields produce epoch milliseconds as an integer, despite the
// README documenting ISO-8601 for this type.
func TestTimestampField(t *testing.T) {
	gen := mustFieldGen(t, scenario.Field{Name: "ts", Type: scenario.FieldTimestamp}, testRand())

	before := time.Now().UnixMilli()
	v, ok := gen().(int64)
	after := time.Now().UnixMilli()
	if !ok {
		t.Fatalf("value is not an int64 (must not be an ISO-8601 string)")
	}
	if v < before || v > after {
		t.Errorf("timestamp %d outside [%d, %d]", v, before, after)
	}
	if v < 1577836800000 || v > 4102444800000 {
		t.Errorf("timestamp %d is not plausible epoch millis", v)
	}
}

// Enum fields draw uniformly from the values list, and duplicate entries
// weight the draw.
func TestEnumField(t *testing.T) {
	t.Run("all values appear", func(t *testing.T) {
		gen := mustFieldGen(t, scenario.Field{Name: "e", Type: scenario.FieldEnum, Values: []string{"a", "b", "c"}}, testRand())
		seen := map[any]bool{}
		for range 200 {
			seen[gen()] = true
		}
		if len(seen) != 3 || !seen["a"] || !seen["b"] || !seen["c"] {
			t.Errorf("seen = %v, want exactly a, b, c", seen)
		}
	})

	t.Run("duplicates weight the draw", func(t *testing.T) {
		gen := mustFieldGen(t, scenario.Field{
			Name: "e", Type: scenario.FieldEnum,
			Values: []string{"success", "success", "success", "failed"},
		}, testRand())
		counts := map[any]int{}
		for range 1000 {
			counts[gen()]++
		}
		if counts["success"] <= counts["failed"]*2 {
			t.Errorf("counts = %v, want success > 2x failed", counts)
		}
	})
}

// Object fields nest a *Doc whose keys keep declaration order.
func TestObjectField(t *testing.T) {
	f := scenario.Field{
		Name: "o", Type: scenario.FieldObject,
		Fields: []scenario.Field{
			{Name: "zeta", Type: scenario.FieldString},
			{Name: "age", Type: scenario.FieldInt},
			{Name: "active", Type: scenario.FieldBoolean},
		},
	}
	gen := mustFieldGen(t, f, testRand())
	v, ok := gen().(*Doc)
	if !ok {
		t.Fatalf("object field produced %T, want *Doc", gen())
	}
	wantKeys := []string{"zeta", "age", "active"}
	for i, k := range v.Keys() {
		if k != wantKeys[i] {
			t.Fatalf("keys = %v, want declaration order %v", v.Keys(), wantKeys)
		}
	}
	if s, _ := v.Get("zeta"); !isString(s) {
		t.Errorf("zeta = %T, want string", s)
	}
	if n, _ := v.Get("age"); !isInt64(n) {
		t.Errorf("age = %T, want int64", n)
	}
	if b, _ := v.Get("active"); !isBool(b) {
		t.Errorf("active = %T, want bool", b)
	}
}

// Array fields produce a slice whose length respects min/max items.
func TestArrayField(t *testing.T) {
	tests := []struct {
		name             string
		minItems         int
		maxItems         int
		wantMin, wantMax int
	}{
		{name: "range", minItems: 3, maxItems: 7, wantMin: 3, wantMax: 7},
		{name: "exact", minItems: 4, maxItems: 4, wantMin: 4, wantMax: 4},
		{name: "always empty", minItems: 0, maxItems: 0, wantMin: 0, wantMax: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := scenario.Field{
				Name: "a", Type: scenario.FieldArray,
				Items:    &scenario.Field{Name: "item", Type: scenario.FieldInt},
				MinItems: tt.minItems, MaxItems: tt.maxItems,
			}
			gen := mustFieldGen(t, f, testRand())
			for range 100 {
				v, ok := gen().([]any)
				if !ok {
					t.Fatalf("array field produced %T, want []any", gen())
				}
				if len(v) < tt.wantMin || len(v) > tt.wantMax {
					t.Fatalf("len = %d, want within [%d, %d]", len(v), tt.wantMin, tt.wantMax)
				}
				for _, item := range v {
					if !isInt64(item) {
						t.Fatalf("item %T, want int64", item)
					}
				}
			}
		})
	}
}

// Every construction error case reports a diagnosable message.
func TestNewFieldGenErrors(t *testing.T) {
	tests := []struct {
		name    string
		field   scenario.Field
		wantSub string
	}{
		{
			name:    "enum requires values",
			field:   scenario.Field{Name: "e", Type: scenario.FieldEnum},
			wantSub: "requires 'values'",
		},
		{
			name:    "object requires fields",
			field:   scenario.Field{Name: "o", Type: scenario.FieldObject},
			wantSub: "requires 'fields'",
		},
		{
			name:    "array requires items",
			field:   scenario.Field{Name: "a", Type: scenario.FieldArray},
			wantSub: "requires 'items'",
		},
		{
			name:    "faker requires provider",
			field:   scenario.Field{Name: "f", Type: scenario.FieldFaker},
			wantSub: "requires 'provider'",
		},
		{
			name:    "unknown type",
			field:   scenario.Field{Name: "x", Type: "unknown_type"},
			wantSub: "unknown field type",
		},
		{
			name:    "inverted int range",
			field:   scenario.Field{Name: "i", Type: scenario.FieldInt, Min: floatPtr(10), Max: floatPtr(1)},
			wantSub: "must be >= min",
		},
		{
			name:    "inverted string lengths",
			field:   scenario.Field{Name: "s", Type: scenario.FieldString, MinLength: intPtr(10), MaxLength: intPtr(2)},
			wantSub: "must be >= min_length",
		},
		{
			name: "nested object error is wrapped",
			field: scenario.Field{
				Name: "o", Type: scenario.FieldObject,
				Fields: []scenario.Field{{Name: "bad", Type: "nope"}},
			},
			wantSub: "unknown field type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewFieldGen(tt.field, testRand())
			if err == nil {
				t.Fatal("want error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantSub)
			}
		})
	}
}

// Equally seeded generators must emit identical documents.
func TestSeededReproducibility(t *testing.T) {
	fields := []scenario.Field{
		{Name: "id", Type: scenario.FieldUUID},
		{Name: "name", Type: scenario.FieldString, MinLength: intPtr(3), MaxLength: intPtr(12)},
		{Name: "qty", Type: scenario.FieldInt, Min: floatPtr(1), Max: floatPtr(99), Cardinality: intPtr(7)},
		{Name: "price", Type: scenario.FieldFloat, Min: floatPtr(0), Max: floatPtr(500)},
		{Name: "active", Type: scenario.FieldBoolean},
		{Name: "status", Type: scenario.FieldEnum, Values: []string{"NEW", "DONE", "FAILED"}},
		{Name: "customer", Type: scenario.FieldFaker, Provider: "name"},
		{Name: "tags", Type: scenario.FieldArray, Items: &scenario.Field{Name: "t", Type: scenario.FieldString}, MinItems: 1, MaxItems: 4},
		{Name: "addr", Type: scenario.FieldObject, Fields: []scenario.Field{{Name: "city", Type: scenario.FieldFaker, Provider: "city"}}},
	}

	run := func() []string {
		g, err := NewDocGen(fields, rand.New(rand.NewPCG(42, 42)))
		if err != nil {
			t.Fatalf("NewDocGen: %v", err)
		}
		out := make([]string, 20)
		for i := range out {
			b, err := json.Marshal(g.Next())
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			out[i] = string(b)
		}
		return out
	}

	a, b := run(), run()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("document %d differs between equally seeded runs:\n %s\n %s", i, a[i], b[i])
		}
	}

	c, err := NewDocGen(fields, rand.New(rand.NewPCG(7, 7)))
	if err != nil {
		t.Fatalf("NewDocGen: %v", err)
	}
	other, err := json.Marshal(c.Next())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(other) == a[0] {
		t.Error("a different seed produced an identical document")
	}
}

// DocGen produces documents in field-declaration order with correctly typed values.
func TestDocGen(t *testing.T) {
	fields := []scenario.Field{
		{Name: "id", Type: scenario.FieldInt, Min: floatPtr(1), Max: floatPtr(100)},
		{Name: "name", Type: scenario.FieldString, MinLength: intPtr(1), MaxLength: intPtr(50)},
		{Name: "score", Type: scenario.FieldFloat, Min: floatPtr(0), Max: floatPtr(1)},
		{Name: "active", Type: scenario.FieldBoolean},
		{Name: "status", Type: scenario.FieldEnum, Values: []string{"pending", "done"}},
	}
	g, err := NewDocGen(fields, testRand())
	if err != nil {
		t.Fatalf("NewDocGen: %v", err)
	}

	doc := g.Next()
	wantOrder := []string{"id", "name", "score", "active", "status"}
	for i, k := range doc.Keys() {
		if k != wantOrder[i] {
			t.Fatalf("keys = %v, want %v", doc.Keys(), wantOrder)
		}
	}
	if v, _ := doc.Get("id"); !isInt64(v) {
		t.Errorf("id = %T, want int64", v)
	}
	if v, _ := doc.Get("score"); !isFloat64(v) {
		t.Errorf("score = %T, want float64", v)
	}
	if v, _ := doc.Get("status"); v != "pending" && v != "done" {
		t.Errorf("status = %v, want pending or done", v)
	}

	// Constraints hold across many documents, not just the first.
	for range 100 {
		v, _ := g.Next().Get("id")
		if n := v.(int64); n < 1 || n > 100 {
			t.Fatalf("id %d outside [1, 100]", n)
		}
	}
}

// An empty field list yields an empty document.
func TestDocGenEmptyFields(t *testing.T) {
	g, err := NewDocGen(nil, testRand())
	if err != nil {
		t.Fatalf("NewDocGen: %v", err)
	}
	got, err := g.Next().MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	if string(got) != "{}" {
		t.Errorf("empty field list produced %s, want {}", got)
	}
}

// The cardinality cache lives in the generator, so it is shared by every
// document the DocGen produces.
func TestDocGenSharesCardinalityCacheAcrossDocuments(t *testing.T) {
	g, err := NewDocGen([]scenario.Field{
		{Name: "region", Type: scenario.FieldString, Cardinality: intPtr(3)},
	}, testRand())
	if err != nil {
		t.Fatalf("NewDocGen: %v", err)
	}

	seen := map[any]bool{}
	for range 60 {
		v, _ := g.Next().Get("region")
		seen[v] = true
	}
	if len(seen) != 3 {
		t.Errorf("saw %d distinct regions across 60 documents, want 3", len(seen))
	}
}

func isString(v any) bool  { _, ok := v.(string); return ok }
func isInt64(v any) bool   { _, ok := v.(int64); return ok }
func isBool(v any) bool    { _, ok := v.(bool); return ok }
func isFloat64(v any) bool { _, ok := v.(float64); return ok }
