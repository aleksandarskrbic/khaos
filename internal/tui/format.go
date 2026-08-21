package tui

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/x/ansi"
)

// comma renders n with thousands separators so wide counters stay readable at a glance.
func comma(n int64) string {
	s := strconv.FormatInt(n, 10)
	start := 0
	if strings.HasPrefix(s, "-") {
		start = 1
	}
	var b strings.Builder
	b.Grow(len(s) + (len(s)-start)/3)
	for i, c := range []byte(s) {
		if i > start && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteByte(c)
	}
	return b.String()
}

// humanRate renders a per-second message rate compactly: 3.2/s, 45/s, 12.4k/s, 1.2M/s.
func humanRate(v float64) string {
	switch {
	case v >= 1e6:
		return fmt.Sprintf("%.1fM/s", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.1fk/s", v/1e3)
	case v >= 10:
		return fmt.Sprintf("%.0f/s", v)
	default:
		return fmt.Sprintf("%.1f/s", v)
	}
}

// humanBytes renders a byte count with an SI suffix: 512 B, 24.3 KB, 1.2 GB.
func humanBytes(n int64) string {
	f := float64(n)
	switch {
	case f >= 1e12:
		return fmt.Sprintf("%.1f TB", f/1e12)
	case f >= 1e9:
		return fmt.Sprintf("%.1f GB", f/1e9)
	case f >= 1e6:
		return fmt.Sprintf("%.1f MB", f/1e6)
	case f >= 1e3:
		return fmt.Sprintf("%.1f KB", f/1e3)
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// humanByteRate is humanBytes for a rate: 4.9 MB/s.
func humanByteRate(v float64) string {
	switch {
	case v >= 1e9:
		return fmt.Sprintf("%.1f GB/s", v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("%.1f MB/s", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.1f KB/s", v/1e3)
	default:
		return fmt.Sprintf("%.0f B/s", v)
	}
}

// sparkChars are the eight block heights a sparkline cell can take.
var sparkChars = []rune("▁▂▃▄▅▆▇█")

// sparkline draws vals -- oldest first -- as one block character each, scaled to the
// window's own range. Zero is always the lowest block and an all-zero window still renders,
// so the sparkline holds its width once shown instead of blinking in and out.
func sparkline(vals []float64) string {
	if len(vals) == 0 {
		return ""
	}
	lo, hi := vals[0], vals[0]
	for _, v := range vals {
		lo = min(lo, v)
		hi = max(hi, v)
	}
	var b strings.Builder
	for _, v := range vals {
		i := 0
		switch {
		case v <= 0:
			// zero stays visually zero regardless of the window's range
		case hi-lo < hi*0.05:
			i = 4
		default:
			i = 1 + int((v-lo)/(hi-lo)*6.99)
			i = min(i, len(sparkChars)-1)
		}
		b.WriteRune(sparkChars[i])
	}
	return b.String()
}

// truncate shortens s to at most n terminal cells, marking the cut with an ellipsis.
// Cell-aware rather than rune-aware: flow, scenario and consumer-group names come from user
// YAML and are not restricted to ASCII, and a CJK character occupies two cells.
func truncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	return ansi.Truncate(s, n, "…")
}

func fmtDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	if h > 0 {
		return fmt.Sprintf("%dh%02dm%02ds", h, m, s)
	}
	return fmt.Sprintf("%dm%02ds", m, s)
}
