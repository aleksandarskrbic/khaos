package generate

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

func mustKeyGen(t *testing.T, ms scenario.MessageSchema) func() []byte {
	t.Helper()
	g, err := NewKeyGen(ms, testRand())
	if err != nil {
		t.Fatalf("NewKeyGen(%+v): %v", ms, err)
	}
	return g
}

// Keys are namespaced key-{i} and uniform spreads evenly.
func TestUniformKeys(t *testing.T) {
	const card = 5
	gen := mustKeyGen(t, scenario.MessageSchema{KeyDistribution: KeyUniform, KeyCardinality: card})

	counts := map[string]int{}
	const samples = 10000
	for range samples {
		counts[string(gen())]++
	}
	if len(counts) != card {
		t.Fatalf("saw %d distinct keys, want %d", len(counts), card)
	}
	expected := float64(samples) / card
	for i := range card {
		key := fmt.Sprintf("key-%d", i)
		count, ok := counts[key]
		if !ok {
			t.Fatalf("key %q never generated", key)
		}
		if diff := float64(count) - expected; diff > expected*0.2 || diff < -expected*0.2 {
			t.Errorf("%s appeared %d times, want roughly %.0f", key, count, expected)
		}
	}
}

// Skew is hardcoded to 1.5, so only the shape of that one distribution can be
// asserted.
func TestZipfianKeys(t *testing.T) {
	const card = 10
	gen := mustKeyGen(t, scenario.MessageSchema{KeyDistribution: KeyZipfian, KeyCardinality: card})

	counts := map[string]int{}
	for range 10000 {
		counts[string(gen())]++
	}
	for key := range counts {
		if !strings.HasPrefix(key, "key-") {
			t.Fatalf("unexpected key %q", key)
		}
	}
	if counts["key-0"] <= counts["key-9"]*5 {
		t.Errorf("key-0 appeared %d times and key-9 %d; want key-0 at least 5x hotter",
			counts["key-0"], counts["key-9"])
	}
	// Monotonically decreasing in expectation: check the halves rather than
	// adjacent pairs, which are noisy.
	head, tail := 0, 0
	for i := range card {
		c := counts[fmt.Sprintf("key-%d", i)]
		if i < card/2 {
			head += c
		} else {
			tail += c
		}
	}
	if head <= tail {
		t.Errorf("first half got %d samples and second half %d; want a hot head", head, tail)
	}
}

// zipfCumulative must reproduce weight_i = 1/(i+1)^skew normalised.
func TestZipfCumulativeWeights(t *testing.T) {
	cum := zipfCumulative(4, zipfSkew)

	// 1, 1/2^1.5, 1/3^1.5, 1/4^1.5 -> normalised, accumulated.
	wantStep := []float64{1, 0.35355339059327373, 0.19245008972987526, 0.125}
	total := 0.0
	for _, w := range wantStep {
		total += w
	}
	acc := 0.0
	for i, w := range wantStep {
		acc += w / total
		got := cum[i]
		if i == len(wantStep)-1 {
			acc = 1.0 // the tail is pinned to exactly 1
		}
		if diff := got - acc; diff > 1e-12 || diff < -1e-12 {
			t.Errorf("cum[%d] = %v, want %v", i, got, acc)
		}
	}
}

// single_key always returns "hot-key" and ignores key_cardinality entirely --
// that is how the hot-partition scenarios pin every message to one partition.
func TestSingleKeyIgnoresCardinality(t *testing.T) {
	for _, card := range []int{0, 1, 500} {
		gen := mustKeyGen(t, scenario.MessageSchema{KeyDistribution: KeySingleKey, KeyCardinality: card})
		for range 100 {
			if got := string(gen()); got != "hot-key" {
				t.Fatalf("key_cardinality %d: got %q, want hot-key", card, got)
			}
		}
	}
}

// Round-robin cycles through every key in order, exactly evenly.
func TestRoundRobinKeys(t *testing.T) {
	const card = 3
	gen := mustKeyGen(t, scenario.MessageSchema{KeyDistribution: KeyRoundRobin, KeyCardinality: card})

	want := []string{"key-0", "key-1", "key-2", "key-0", "key-1", "key-2", "key-0"}
	for i, w := range want {
		if got := string(gen()); got != w {
			t.Fatalf("call %d = %q, want %q", i, got, w)
		}
	}

	counts := map[string]int{}
	for range card * 100 {
		counts[string(gen())]++
	}
	for key, count := range counts {
		if count != 100 {
			t.Errorf("%s appeared %d times, want exactly 100", key, count)
		}
	}
}

func TestNewKeyGenErrors(t *testing.T) {
	tests := []struct {
		name    string
		schema  scenario.MessageSchema
		wantSub string
	}{
		{
			name:    "unknown distribution",
			schema:  scenario.MessageSchema{KeyDistribution: "zipfain", KeyCardinality: 10},
			wantSub: "unknown key distribution",
		},
		{
			name:    "zero cardinality",
			schema:  scenario.MessageSchema{KeyDistribution: KeyUniform, KeyCardinality: 0},
			wantSub: "key_cardinality must be at least 1",
		},
		{
			name:    "negative cardinality",
			schema:  scenario.MessageSchema{KeyDistribution: KeyRoundRobin, KeyCardinality: -3},
			wantSub: "key_cardinality must be at least 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewKeyGen(tt.schema, testRand())
			if err == nil {
				t.Fatal("want error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantSub)
			}
		})
	}
}

// An empty key_distribution means uniform.
func TestEmptyDistributionDefaultsToUniform(t *testing.T) {
	gen := mustKeyGen(t, scenario.MessageSchema{KeyCardinality: 4})
	seen := map[string]bool{}
	for range 200 {
		seen[string(gen())] = true
	}
	if len(seen) != 4 {
		t.Errorf("saw %d distinct keys, want 4 spread uniformly", len(seen))
	}
}

func TestKeyGenSeededReproducibility(t *testing.T) {
	for _, dist := range []string{KeyUniform, KeyZipfian, KeyRoundRobin, KeySingleKey} {
		t.Run(dist, func(t *testing.T) {
			ms := scenario.MessageSchema{KeyDistribution: dist, KeyCardinality: 25}
			a := mustKeyGen(t, ms)
			b := mustKeyGen(t, ms)
			for i := range 100 {
				if x, y := string(a()), string(b()); x != y {
					t.Fatalf("call %d: %q vs %q between equally seeded generators", i, x, y)
				}
			}
		})
	}
}
