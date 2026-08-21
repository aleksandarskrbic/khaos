package generate

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// The synthetic payload has the expected shape: id, sequence and timestamp.
func TestRawJSONGenShape(t *testing.T) {
	g := NewRawJSONGen(scenario.MessageSchema{MinSizeBytes: 100, MaxSizeBytes: 200}, testRand())

	for i := 1; i <= 10; i++ {
		raw := g.Next()

		var data map[string]any
		if err := json.Unmarshal(raw, &data); err != nil {
			t.Fatalf("payload is not valid JSON: %v (%s)", err, raw)
		}
		id, _ := data["id"].(string)
		if !strings.HasPrefix(id, "msg-") {
			t.Errorf("id = %q, want a msg- prefix", id)
		}
		if got := data["sequence"].(float64); int(got) != i {
			t.Errorf("sequence = %v, want %d", got, i)
		}
		if _, ok := data["timestamp"]; !ok {
			t.Error("timestamp missing; both include flags are hardwired on")
		}
	}
	if g.Sequence() != 10 {
		t.Errorf("Sequence() = %d, want 10", g.Sequence())
	}
}

// Key order must match insertion order, not the alphabetical order a Go map
// would produce (data, id, sequence, timestamp).
func TestRawJSONGenKeyOrder(t *testing.T) {
	g := NewRawJSONGen(scenario.MessageSchema{MinSizeBytes: 500, MaxSizeBytes: 500}, testRand())
	got := string(g.Next())

	want := []string{`"id"`, `"timestamp"`, `"sequence"`, `"data"`}
	prev := -1
	for _, key := range want {
		at := strings.Index(got, key)
		if at < 0 {
			t.Fatalf("key %s missing from %s", key, got[:60])
		}
		if at <= prev {
			t.Fatalf("key %s is out of order in %s", key, got[:60])
		}
		prev = at
	}
}

// padding = target - current - 20, and adding `,"data":"…"` costs 10 bytes of
// JSON syntax, so a padded payload lands exactly 10 bytes UNDER target. That
// undershoot is the contract.
func TestRawJSONGenPaddingUndershootsTargetByTen(t *testing.T) {
	tests := []struct {
		name        string
		size        int
		wantPadding bool
		wantLen     int
	}{
		{name: "padded to a fixed target", size: 500, wantPadding: true, wantLen: 490},
		{name: "large target", size: 4096, wantPadding: true, wantLen: 4086},
		{name: "target within 20 bytes of the base document gets no padding", size: 60, wantPadding: false},
		{name: "target below the base document gets no padding", size: 20, wantPadding: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewRawJSONGen(scenario.MessageSchema{MinSizeBytes: tt.size, MaxSizeBytes: tt.size}, testRand())
			raw := g.Next()

			var data map[string]any
			if err := json.Unmarshal(raw, &data); err != nil {
				t.Fatalf("payload is not valid JSON: %v", err)
			}
			_, padded := data["data"]
			if padded != tt.wantPadding {
				t.Fatalf("data key present = %v, want %v (payload %s)", padded, tt.wantPadding, raw)
			}
			if !tt.wantPadding {
				// No truncation either: a target smaller than the base document
				// simply yields the base document, since padding only ever adds.
				return
			}
			if len(raw) != tt.wantLen {
				t.Errorf("len = %d, want %d (target %d minus the 10-byte undershoot)", len(raw), tt.wantLen, tt.size)
			}
			// The loose "within 50 bytes" version of the undershoot contract.
			if len(raw) < tt.size-50 {
				t.Errorf("len = %d, more than 50 bytes below the %d byte target", len(raw), tt.size)
			}
			// padding == target - current - 20, with `current` measured
			// independently: a generator whose target is 1 byte never pads, so
			// its first payload is exactly the base document.
			base := NewRawJSONGen(scenario.MessageSchema{MinSizeBytes: 1, MaxSizeBytes: 1}, testRand())
			current := len(base.Next())
			if want := tt.size - current - 20; strings.Count(string(raw), "x") != want {
				t.Errorf("padding = %d bytes, want target - current - 20 = %d",
					strings.Count(string(raw), "x"), want)
			}
		})
	}
}

// Sizes are drawn per message from [min_size_bytes, max_size_bytes].
func TestRawJSONGenSizeVariesWithinBounds(t *testing.T) {
	g := NewRawJSONGen(scenario.MessageSchema{MinSizeBytes: 200, MaxSizeBytes: 500}, testRand())

	sizes := map[int]bool{}
	for range 200 {
		n := len(g.Next())
		sizes[n] = true
		if n > 500 {
			t.Fatalf("payload of %d bytes exceeds max_size_bytes", n)
		}
		if n < 200-50 {
			t.Fatalf("payload of %d bytes is far below min_size_bytes", n)
		}
	}
	if len(sizes) < 10 {
		t.Errorf("only %d distinct sizes in 200 messages; the target is supposed to be random", len(sizes))
	}
}

// max < min cannot be rejected (the constructor has no error return) so it
// collapses to a fixed size instead of panicking in rand.IntN.
func TestRawJSONGenClampsInvertedBounds(t *testing.T) {
	g := NewRawJSONGen(scenario.MessageSchema{MinSizeBytes: 300, MaxSizeBytes: 10}, testRand())
	for range 20 {
		if n := len(g.Next()); n != 290 {
			t.Fatalf("len = %d, want 290 (min treated as a fixed target)", n)
		}
	}
}

func TestRawJSONGenSeededReproducibility(t *testing.T) {
	// Sizes, not payloads: the timestamp field makes the bytes time-dependent.
	run := func() []int {
		g := NewRawJSONGen(scenario.MessageSchema{MinSizeBytes: 200, MaxSizeBytes: 900}, testRand())
		out := make([]int, 50)
		for i := range out {
			out[i] = len(g.Next())
		}
		return out
	}
	a, b := run(), run()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("payload %d: %d vs %d bytes between equally seeded runs", i, a[i], b[i])
		}
	}
}
