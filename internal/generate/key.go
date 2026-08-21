package generate

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sort"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// Key distribution names accepted in `key_distribution:`.
const (
	KeyUniform    = "uniform"
	KeyZipfian    = "zipfian"
	KeySingleKey  = "single_key"
	KeyRoundRobin = "round_robin"
)

// zipfSkew is the Zipfian exponent. It is a fixed constant, not exposed
// through the scenario schema, so 1.5 is the only value any scenario can get.
const zipfSkew = 1.5

// hotKey is the single key every message carries under the single_key
// distribution.
const hotKey = "hot-key"

// NewKeyGen returns the Kafka key generator for a message schema.
//
// Keys are namespaced "key-{i}" for i in [0, key_cardinality) and precomputed
// once. The returned function is not safe for concurrent use (round_robin
// keeps a cursor).
func NewKeyGen(ms scenario.MessageSchema, r *rand.Rand) (func() []byte, error) {
	dist := ms.KeyDistribution
	if dist == "" {
		// An empty key_distribution means uniform.
		dist = KeyUniform
	}

	if dist == KeySingleKey {
		// single_key ignores key_cardinality entirely: every message goes to
		// one key and therefore one partition. That is the whole point of the
		// distribution -- it is how hot-partition scenarios are built -- so
		// cardinality is not even validated here.
		key := []byte(hotKey)
		return func() []byte { return key }, nil
	}

	card := ms.KeyCardinality
	if card < 1 {
		return nil, fmt.Errorf("key_cardinality must be at least 1, got %d", card)
	}
	keys := make([][]byte, card)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
	}

	switch dist {
	case KeyUniform:
		return func() []byte { return keys[r.IntN(card)] }, nil

	case KeyZipfian:
		cum := zipfCumulative(card, zipfSkew)
		return func() []byte {
			// Same sampling as random.choices(): draw in [0, 1) and bisect the
			// cumulative weights.
			return keys[sort.SearchFloat64s(cum, r.Float64())]
		}, nil

	case KeyRoundRobin:
		idx := 0
		return func() []byte {
			key := keys[idx]
			idx = (idx + 1) % card
			return key
		}, nil

	default:
		// An unknown name is rejected rather than silently falling back to
		// uniform, so a typo like `key_distribution: zipfain` fails loudly
		// instead of quietly producing an even spread.
		return nil, fmt.Errorf(
			"unknown key distribution %q (want %s, %s, %s or %s)",
			ms.KeyDistribution, KeyUniform, KeyZipfian, KeySingleKey, KeyRoundRobin)
	}
}

// zipfCumulative returns the normalised cumulative weights of the Zipfian
// distribution used for keys: weight_i = 1/(i+1)^skew, normalised to sum 1.
//
// math/rand/v2's rand.NewZipf(r, s, v, imax) draws k with P(k) proportional to
// (v+k)^-s, which for v=1 is exactly this table -- but it is not used here:
// NewZipf returns nil for s <= 1 (a silent nil-pointer trap if the skew ever
// becomes configurable), and it samples by rejection, consuming an
// unpredictable number of values per draw. The explicit table keeps exactly
// one draw per key.
func zipfCumulative(n int, skew float64) []float64 {
	cum := make([]float64, n)
	total := 0.0
	for i := range n {
		total += 1.0 / math.Pow(float64(i+1), skew)
		cum[i] = total
	}
	for i := range cum {
		cum[i] /= total
	}
	// Guard against a float64 sum landing a hair below 1.0, which would let a
	// draw very close to 1 fall off the end of the table.
	cum[n-1] = 1.0
	return cum
}
