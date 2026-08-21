package tui

import "time"

// rateWindow is how far back a rate calculation reaches. Long enough that one slow tick
// does not swing the number, short enough that a real change shows within a few frames.
const rateWindow = 5 * time.Second

// minRateSpan is the shortest interval a rate is derived over. Below this the counters
// have barely moved and the division amplifies noise into a number that flickers, which
// is worse than showing nothing.
const minRateSpan = 900 * time.Millisecond

func rate(delta int64, span time.Duration) float64 {
	if span <= 0 {
		return 0
	}
	return max(0, float64(delta)/span.Seconds())
}

// latest is the sample recorded for the snapshot currently being rendered.
func (m model) latest() (sample, bool) {
	if m.sampleN == 0 {
		return sample{}, false
	}
	return m.samples[(m.sampleN-1)%sampleCap], true
}

// baseline is the oldest stored sample still inside the rate window, the anchor every
// rate is measured against.
func (m model) baseline(now time.Time) (sample, bool) {
	n := m.sampleN
	start := 0
	if n > sampleCap {
		start = n - sampleCap
	}
	for i := start; i < n; i++ {
		s := m.samples[i%sampleCap]
		if s.at.Before(now) && now.Sub(s.at) <= rateWindow {
			return s, true
		}
	}
	return sample{}, false
}

// totalRates derives whole-run message and byte rates from the sample ring. ok is false
// until the ring spans enough time for the numbers to be steady.
func (m model) totalRates() (prod, cons, bytes float64, ok bool) {
	cur, okc := m.latest()
	if !okc {
		return 0, 0, 0, false
	}
	base, okb := m.baseline(cur.at)
	if !okb {
		return 0, 0, 0, false
	}
	span := cur.at.Sub(base.at)
	if span < minRateSpan {
		return 0, 0, 0, false
	}
	return rate(cur.produced-base.produced, span),
		rate(cur.consumed-base.consumed, span),
		rate(cur.bytes-base.bytes, span),
		true
}

// topicRate is the produced-message rate for one topic over the same window.
func (m model) topicRate(name string) (float64, bool) {
	cur, okc := m.latest()
	if !okc {
		return 0, false
	}
	base, okb := m.baseline(cur.at)
	if !okb {
		return 0, false
	}
	span := cur.at.Sub(base.at)
	if span < minRateSpan {
		return 0, false
	}
	c, okc2 := cur.topics[name]
	b, okb2 := base.topics[name]
	if !okc2 || !okb2 {
		return 0, false
	}
	return rate(c.produced-b.produced, span), true
}

// sampleNear is the newest sample at or before t.
func (m model) sampleNear(t time.Time) (sample, bool) {
	n := m.sampleN
	start := 0
	if n > sampleCap {
		start = n - sampleCap
	}
	var out sample
	found := false
	for i := start; i < n; i++ {
		s := m.samples[i%sampleCap]
		if s.at.After(t) {
			break
		}
		out = s
		found = true
	}
	return out, found
}

// sparkVals derives one produced-rate value per second, oldest first, for the sparkline.
// Nil until at least three seconds have data -- a one-cell sparkline is noise, and a
// sparkline that appears and disappears is worse than none.
func (m model) sparkVals(buckets int) []float64 {
	cur, ok := m.latest()
	if !ok {
		return nil
	}
	vals := make([]float64, 0, buckets)
	for k := buckets - 1; k >= 0; k-- {
		hi := cur.at.Add(-time.Duration(k) * time.Second)
		lo := hi.Add(-time.Second)
		s1, ok1 := m.sampleNear(hi)
		s0, ok0 := m.sampleNear(lo)
		if !ok0 || !ok1 || !s0.at.Before(s1.at) {
			// No data this far back. Skip leading holes entirely; once the line has
			// begun, a hole renders as zero rather than silently joining its neighbours.
			if len(vals) > 0 {
				vals = append(vals, 0)
			}
			continue
		}
		vals = append(vals, rate(s1.produced-s0.produced, s1.at.Sub(s0.at)))
	}
	if len(vals) < 3 {
		return nil
	}
	return vals
}
