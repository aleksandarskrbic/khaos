package engine

import (
	"testing"

	"golang.org/x/time/rate"
)

// SetRate retunes the running producer's limiter live -- the change_producer_rate
// incident's whole point, and the intent of the three shipped scenarios that use it.
func TestSetRateRetunesTheRunningLimiter(t *testing.T) {
	p := &Producer{limiter: newLimiter(10)}

	p.SetRate(50)
	if got := p.Rate(); got != 50 {
		t.Fatalf("Rate() = %v, want 50 after SetRate(50)", got)
	}

	// Unlimited is expressed as +Inf, not 0 -- rate.Limiter has no zero-means-unlimited
	// convention of its own.
	p.SetRate(0)
	if got := p.Rate(); got != float64(rate.Inf) {
		t.Fatalf("Rate() = %v, want %v (unlimited) for a zero target rate", got, float64(rate.Inf))
	}
}
