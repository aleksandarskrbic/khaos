package generate

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"regexp"
	"strings"
	"testing"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// The zipfian key distribution must match its analytic weights, not merely
// "have a hot head": weights[i] = 1/(i+1)^skew normalised to sum 1, with skew
// pinned at 1.5.
//
// The seed is fixed, so this is a deterministic assertion rather than a
// flaky one: the tolerance exists to absorb sampling noise at this sample
// size, not to make a wrong distribution pass.
func TestZipfianKeysMatchAnalyticWeights(t *testing.T) {
	const (
		card    = 12
		samples = 200_000
		// ~4.5 sigma at this sample size for the rarest key, which sits near p=0.012.
		absTolerance = 0.0025
		relTolerance = 0.05
	)

	want := make([]float64, card)
	total := 0.0
	for i := range card {
		want[i] = 1.0 / math.Pow(float64(i+1), zipfSkew)
		total += want[i]
	}
	for i := range want {
		want[i] /= total
	}

	gen := mustKeyGen(t, scenario.MessageSchema{KeyDistribution: KeyZipfian, KeyCardinality: card})
	counts := make([]int, card)
	for range samples {
		key := string(gen())
		var rank int
		if _, err := fmt.Sscanf(key, "key-%d", &rank); err != nil || rank < 0 || rank >= card {
			t.Fatalf("unexpected key %q", key)
		}
		counts[rank]++
	}

	for i := range card {
		got := float64(counts[i]) / samples
		tolerance := math.Max(absTolerance, want[i]*relTolerance)
		if math.Abs(got-want[i]) > tolerance {
			t.Errorf("rank %d: observed p=%.5f, want %.5f (+/- %.5f)", i, got, want[i], tolerance)
		}
	}

	// The head must dominate in the exact order the weights say, not just on average.
	for i := 1; i < card; i++ {
		if counts[i] >= counts[i-1] {
			t.Errorf("rank %d (%d samples) is not rarer than rank %d (%d samples)",
				i, counts[i], i-1, counts[i-1])
		}
	}
}

// Every key of the distribution must be reachable; a cumulative table that ends a
// hair below 1.0 would starve the last rank, and one that starts too high would
// starve the first.
func TestZipfianCoversEveryRank(t *testing.T) {
	const card = 8
	gen := mustKeyGen(t, scenario.MessageSchema{KeyDistribution: KeyZipfian, KeyCardinality: card})

	seen := map[string]bool{}
	for range 100_000 {
		seen[string(gen())] = true
	}
	for i := range card {
		if key := fmt.Sprintf("key-%d", i); !seen[key] {
			t.Errorf("%s was never generated", key)
		}
	}
}

// A single-key cardinality of 1 must still work for every distribution: the
// cumulative table is one entry long and the round-robin cursor never advances.
func TestKeyGenCardinalityOne(t *testing.T) {
	for _, dist := range []string{KeyUniform, KeyZipfian, KeyRoundRobin} {
		t.Run(dist, func(t *testing.T) {
			gen := mustKeyGen(t, scenario.MessageSchema{KeyDistribution: dist, KeyCardinality: 1})
			for range 50 {
				if got := string(gen()); got != "key-0" {
					t.Fatalf("got %q, want key-0", got)
				}
			}
		})
	}
}

// JSON key order is declaration order at every depth, including inside array
// items.
//
// Go's encoding/json sorts map keys, so the whole point of Doc is that a
// nested object generated inside an array does NOT come out alphabetised.
// Every field name below is deliberately anti-alphabetical.
func TestGeneratedJSONKeyOrderAtEveryDepth(t *testing.T) {
	fields := []scenario.Field{
		{Name: "zulu", Type: scenario.FieldEnum, Values: []string{"z"}},
		{
			Name: "mike", Type: scenario.FieldObject,
			Fields: []scenario.Field{
				{Name: "yankee", Type: scenario.FieldEnum, Values: []string{"y"}},
				{Name: "alpha", Type: scenario.FieldEnum, Values: []string{"a"}},
				{
					Name: "november", Type: scenario.FieldObject,
					Fields: []scenario.Field{
						{Name: "xray", Type: scenario.FieldEnum, Values: []string{"x"}},
						{Name: "bravo", Type: scenario.FieldEnum, Values: []string{"b"}},
					},
				},
			},
		},
		{
			Name: "items", Type: scenario.FieldArray, MinItems: 2, MaxItems: 2,
			Items: &scenario.Field{
				Name: "item", Type: scenario.FieldObject,
				Fields: []scenario.Field{
					{Name: "whisky", Type: scenario.FieldEnum, Values: []string{"w"}},
					{Name: "charlie", Type: scenario.FieldEnum, Values: []string{"c"}},
				},
			},
		},
		{Name: "alpha", Type: scenario.FieldEnum, Values: []string{"A"}},
	}

	gen, err := NewDocGen(fields, testRand())
	if err != nil {
		t.Fatalf("NewDocGen: %v", err)
	}
	got, err := json.Marshal(gen.Next())
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	want := `{"zulu":"z","mike":{"yankee":"y","alpha":"a","november":{"xray":"x","bravo":"b"}},` +
		`"items":[{"whisky":"w","charlie":"c"},{"whisky":"w","charlie":"c"}],"alpha":"A"}`
	if string(got) != want {
		t.Errorf("json.Marshal =\n %s\nwant\n %s", got, want)
	}
}

// Construction options must reach flow step fields, at every depth.
//
// NewFlowGen used to build its per-step DocGens with the package defaults, so a flow
// step carrying an impossible cardinality hung where the identical field inside a
// topic's message_schema reported an error. This is the same gap that was just fixed
// for nested objects and array items.
func TestFlowGenPropagatesOptions(t *testing.T) {
	impossible := scenario.Field{
		Name: "n", Type: scenario.FieldInt,
		Min: floatPtr(0), Max: floatPtr(5), Cardinality: intPtr(100),
	}

	tests := []struct {
		name  string
		field scenario.Field
	}{
		{"top-level step field", impossible},
		{"nested object field", scenario.Field{Name: "o", Type: scenario.FieldObject, Fields: []scenario.Field{impossible}}},
		{"array item", scenario.Field{Name: "a", Type: scenario.FieldArray, MinItems: 1, MaxItems: 2, Items: &impossible}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flow := scenario.Flow{
				Name: "f", Rate: 1,
				Steps: []scenario.FlowStep{{Topic: "t", EventType: "e", Fields: []scenario.Field{tt.field}}},
			}
			_, err := NewFlowGen(flow, testRand(), BoundFillAttempts(64))
			if err == nil {
				t.Fatal("the bound did not reach the step field; this would have hung")
			}
			if !strings.Contains(err.Error(), "cannot reach cardinality") {
				t.Errorf("error = %q, want a cardinality error", err)
			}
		})
	}
}

// Flows are still unbounded by default, so the fill loop can hang on an
// impossible cardinality. Only a REACHABLE cardinality is exercised here,
// because asserting the hang would mean leaking the hang.
func TestFlowGenDefaultsToUnboundedFill(t *testing.T) {
	flow := scenario.Flow{
		Name: "f", Rate: 1,
		Steps: []scenario.FlowStep{{
			Topic: "t", EventType: "e",
			Fields: []scenario.Field{{
				Name: "n", Type: scenario.FieldInt,
				Min: floatPtr(0), Max: floatPtr(1000), Cardinality: intPtr(4),
			}},
		}},
	}
	g, err := NewFlowGen(flow, testRand())
	if err != nil {
		t.Fatalf("NewFlowGen: %v", err)
	}

	// The round-robin tail is contractual: after the cache is full, values repeat in
	// first-generated order.
	var seq []int64
	for range 12 {
		msgs, err := g.Instance()
		if err != nil {
			t.Fatalf("Instance: %v", err)
		}
		v, _ := msgs[0].Doc.Get("n")
		seq = append(seq, v.(int64))
	}
	for i := 4; i < len(seq); i++ {
		if seq[i] != seq[i%4] {
			t.Errorf("value %d = %d, want the round-robin repeat %d", i, seq[i], seq[i%4])
		}
	}
}

// Seeding must actually seed: two sources with different seeds must produce different
// output for documents, keys AND flows.
//
// The existing reproducibility tests only prove that equally seeded generators agree,
// which a generator that ignored its source and returned a constant would also satisfy.
func TestDifferentSeedsProduceDifferentOutput(t *testing.T) {
	seedA := func() *rand.Rand { return rand.New(rand.NewPCG(1, 1)) }
	seedB := func() *rand.Rand { return rand.New(rand.NewPCG(2, 2)) }

	t.Run("documents", func(t *testing.T) {
		fields := []scenario.Field{
			{Name: "id", Type: scenario.FieldUUID},
			{Name: "n", Type: scenario.FieldInt, Min: floatPtr(0), Max: floatPtr(1_000_000)},
			{Name: "s", Type: scenario.FieldString, MinLength: intPtr(8), MaxLength: intPtr(16)},
		}
		assertSeedsDiffer(t, func(r *rand.Rand) string {
			gen, err := NewDocGen(fields, r)
			if err != nil {
				t.Fatalf("NewDocGen: %v", err)
			}
			b, err := json.Marshal(gen.Next())
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			return string(b)
		}, seedA(), seedB())
	})

	t.Run("keys", func(t *testing.T) {
		for _, dist := range []string{KeyUniform, KeyZipfian} {
			t.Run(dist, func(t *testing.T) {
				ms := scenario.MessageSchema{KeyDistribution: dist, KeyCardinality: 500}
				assertSeedsDiffer(t, func(r *rand.Rand) string {
					gen, err := NewKeyGen(ms, r)
					if err != nil {
						t.Fatalf("NewKeyGen: %v", err)
					}
					var out strings.Builder
					for range 20 {
						out.Write(gen())
					}
					return out.String()
				}, seedA(), seedB())
			})
		}
	})

	t.Run("flows", func(t *testing.T) {
		assertSeedsDiffer(t, func(r *rand.Rand) string {
			gen, err := NewFlowGen(orderFlow(), r)
			if err != nil {
				t.Fatalf("NewFlowGen: %v", err)
			}
			msgs, err := gen.Instance()
			if err != nil {
				t.Fatalf("Instance: %v", err)
			}
			var out strings.Builder
			for _, m := range msgs {
				out.Write(m.Key)
				b, err := json.Marshal(m.Doc)
				if err != nil {
					t.Fatalf("marshal: %v", err)
				}
				out.Write(b)
			}
			return out.String()
		}, seedA(), seedB())
	})
}

func assertSeedsDiffer(t *testing.T, run func(*rand.Rand) string, a, b *rand.Rand) {
	t.Helper()
	if x, y := run(a), run(b); x == y {
		t.Errorf("two different seeds produced identical output: %s", x)
	}
}

// Timestamps are epoch milliseconds as an integer, never ISO-8601, at every
// depth, despite the README documenting a string for this type.
func TestTimestampIsEpochMillisEverywhere(t *testing.T) {
	fields := []scenario.Field{
		{Name: "top", Type: scenario.FieldTimestamp},
		{Name: "obj", Type: scenario.FieldObject, Fields: []scenario.Field{{Name: "inner", Type: scenario.FieldTimestamp}}},
		{Name: "arr", Type: scenario.FieldArray, MinItems: 1, MaxItems: 1,
			Items: &scenario.Field{Name: "item", Type: scenario.FieldTimestamp}},
	}
	gen, err := NewDocGen(fields, testRand())
	if err != nil {
		t.Fatalf("NewDocGen: %v", err)
	}
	b, err := json.Marshal(gen.Next())
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	// Thirteen digits is epoch millis for any date this millennium; an ISO-8601
	// string would carry quotes and dashes.
	millis := regexp.MustCompile(`^\{"top":\d{13},"obj":\{"inner":\d{13}\},"arr":\[\d{13}\]\}$`)
	if !millis.Match(b) {
		t.Errorf("payload = %s, want bare 13-digit epoch millis at every depth", b)
	}
}

// The payload padding formula lands ~10 bytes UNDER target, because the "data"
// key costs 10 bytes of JSON syntax that the -20 fudge over-compensates for. A
// document within 20 bytes of target gets no padding at all and stays short.
func TestPayloadPaddingUndershoot(t *testing.T) {
	tests := []struct {
		name       string
		size       int
		wantMinPad bool
	}{
		{name: "well above the base document", size: 400, wantMinPad: true},
		{name: "target only just above the base", size: 60},
		{name: "target below the base document", size: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewRawJSONGen(scenario.MessageSchema{MinSizeBytes: tt.size, MaxSizeBytes: tt.size}, testRand())
			got := len(g.Next())
			if !tt.wantMinPad {
				if got >= tt.size && tt.size > 20 {
					t.Errorf("payload is %d bytes for target %d; a near-target document must not be padded", got, tt.size)
				}
				return
			}
			// target - 20 padding + 10 bytes of `,"data":""` syntax.
			if want := tt.size - 10; got != want {
				t.Errorf("payload is %d bytes for target %d, want exactly %d (target minus the 10-byte undershoot)",
					got, tt.size, want)
			}
		})
	}
}
