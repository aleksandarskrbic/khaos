package tui

import (
	"fmt"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/aleksandarskrbic/khaos/internal/engine"
)

// viewWith renders one snapshot through a model built with opts, optionally sized first.
func viewWith(t *testing.T, snap engine.Snapshot, w, h int, opts ...Option) string {
	t.Helper()
	m := New(newStubSource(snap), func() {}, opts...)
	var mm tea.Model = m
	if w > 0 {
		mm, _ = mm.Update(tea.WindowSizeMsg{Width: w, Height: h})
	}
	mm, _ = mm.Update(tickMsg(time.Now()))
	return plain(mm.View())
}

// The FAILED/DLQ columns follow scenario config for the whole run: present from the first
// frame when failure simulation is configured, absent when it is not, whatever the counters do.
func TestFailureColumnsFollowTheConfigGate(t *testing.T) {
	clean := engine.Snapshot{Topics: []engine.TopicStat{{Topic: "orders", Produced: 10, Consumed: 10}}}
	failing := engine.Snapshot{Topics: []engine.TopicStat{{Topic: "orders", Failed: 3, DLQ: 1}}}

	if out := viewWith(t, clean, 0, 0, ShowFailureColumns(true)); !strings.Contains(out, "FAILED") {
		t.Errorf("configured failure simulation must show the columns before the first failure:\n%s", out)
	}
	if out := viewWith(t, failing, 0, 0, ShowFailureColumns(false)); strings.Contains(out, "FAILED") {
		t.Errorf("columns gated off by config must stay off:\n%s", out)
	}
}

// LAG(BROKER) follows --lag-poll the same way: on from the first frame -- every cell
// reading "unknown" until the first measurement lands -- rather than appearing seconds
// into the run and shoving the table sideways.
func TestBrokerLagColumnFollowsTheConfigGate(t *testing.T) {
	unmeasured := engine.Snapshot{Topics: []engine.TopicStat{{
		Topic:  "orders",
		Groups: []engine.GroupStat{{GroupID: "order-processors", Consumed: 5}},
	}}}

	out := viewWith(t, unmeasured, 0, 0, ShowBrokerLag(true))
	if !strings.Contains(out, "LAG(BROKER)") {
		t.Fatalf("lag polling on must show the broker column before the first poll answers:\n%s", out)
	}
	if !strings.Contains(out, "unknown") {
		t.Errorf("an unmeasured broker cell must read unknown:\n%s", out)
	}

	measured := engine.Snapshot{Topics: []engine.TopicStat{{
		Topic:     "orders",
		BrokerLag: map[string]int64{"g": 5},
		Groups:    []engine.GroupStat{{GroupID: "g"}},
	}}}
	if out := viewWith(t, measured, 0, 0, ShowBrokerLag(false)); strings.Contains(out, "BROKER") {
		t.Errorf("polling gated off must hide the column even when a measurement exists:\n%s", out)
	}
}

// Rates are derived by the model, not the engine: two snapshots a second apart yield
// msg/s in the topic table and the totals bar.
func TestRatesDeriveFromConsecutiveSnapshots(t *testing.T) {
	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	mk := func(sec int, produced int64) engine.Snapshot {
		return engine.Snapshot{
			At:      at.Add(time.Duration(sec) * time.Second),
			Elapsed: time.Duration(sec) * time.Second,
			Topics: []engine.TopicStat{{
				Topic: "orders", Produced: produced, Consumed: produced,
			}},
			TotalProduced: produced,
			TotalConsumed: produced,
		}
	}
	src := newStubSource(mk(0, 0), mk(1, 1500), mk(2, 3000))
	var m tea.Model = New(src, func() {})
	for range 3 {
		m, _ = m.Update(tickMsg(time.Now()))
	}
	out := plain(m.View())
	if !strings.Contains(out, "MSG/S") {
		t.Fatalf("no rate column after three progressing snapshots:\n%s", out)
	}
	if !strings.Contains(out, "1.5k/s") {
		t.Errorf("want a 1.5k/s rate derived from the counter deltas:\n%s", out)
	}
}

// A single snapshot has no interval to derive a rate over, so no rate column appears and
// nothing pretends to know a number it cannot.
func TestNoRatesFromASingleSnapshot(t *testing.T) {
	snap := engine.Snapshot{
		At:     time.Now(),
		Topics: []engine.TopicStat{{Topic: "orders", Produced: 100, Consumed: 100}},
	}
	if out := viewWith(t, snap, 0, 0); strings.Contains(out, "MSG/S") {
		t.Errorf("rate column rendered with nothing to derive a rate from:\n%s", out)
	}
}

// More topics than the terminal has rows: the table keeps the header on screen, cuts the
// list, and says how much it cut.
func TestTallSnapshotIsBoundedByTerminalHeight(t *testing.T) {
	snap := engine.Snapshot{}
	for i := range 300 {
		snap.Topics = append(snap.Topics, engine.TopicStat{Topic: fmt.Sprintf("topic-%03d", i)})
	}

	m := New(newStubSource(snap), func() {})
	mm, _ := m.Update(tea.WindowSizeMsg{Width: 100, Height: 24})
	mm, _ = mm.Update(tickMsg(time.Now()))
	out := plain(mm.View())

	if got := strings.Count(out, "\n") + 1; got > 24 {
		t.Errorf("rendered %d lines into a 24-row terminal", got)
	}
	if !strings.Contains(out, "more row") {
		t.Errorf("a cut topic list must say how many rows it is hiding:\n%s", out)
	}
	if !strings.Contains(out, "TOPIC") || !strings.Contains(out, "TOTAL") || !strings.Contains(out, "q quit") {
		t.Errorf("the chrome must survive the cut:\n%s", out)
	}
}

// Whatever the snapshot holds, no rendered line may exceed the terminal width -- an
// overflowing line wraps and breaks every line below it.
func TestNoLineExceedsTheTerminalWidth(t *testing.T) {
	snap := sampleSnapshot()
	snap.Topics[0].BrokerLag = map[string]int64{"order-processors": 480}
	snap.Topics = append(snap.Topics, engine.TopicStat{
		Topic:  strings.Repeat("long-topic-name-", 10),
		Groups: []engine.GroupStat{{GroupID: strings.Repeat("long-group-name-", 10), Consumers: 12, Paused: 3}},
	})

	for _, w := range []int{40, 80, 100, 120, 150} {
		m := New(newStubSource(snap), func() {})
		mm, _ := m.Update(tea.WindowSizeMsg{Width: w, Height: 40})
		mm, _ = mm.Update(tickMsg(time.Now()))
		for i, line := range strings.Split(plain(mm.View()), "\n") {
			if got := lipgloss.Width(line); got > w {
				t.Fatalf("width %d: line %d is %d cells wide:\n%s", w, i, got, line)
			}
		}
	}
}

// Engine-measured errors and duplicates surface as one aggregated line, and only when
// something is non-zero.
func TestIssuesLineSurfacesHiddenCounters(t *testing.T) {
	snap := engine.Snapshot{Topics: []engine.TopicStat{{
		Topic:      "orders",
		ProduceErr: 3,
		ConsumeErr: 2,
		CommitErr:  1,
		Duplicates: 7,
	}}}
	out := viewWith(t, snap, 0, 0)
	for _, want := range []string{"3 produce errors", "2 consume errors", "1 commit error", "7 duplicates"} {
		if !strings.Contains(out, want) {
			t.Errorf("issues line is missing %q:\n%s", want, out)
		}
	}

	clean := viewWith(t, engine.Snapshot{Topics: []engine.TopicStat{{Topic: "orders"}}}, 0, 0)
	for _, absent := range []string{"produce error", "consume error", "commit error", "duplicate"} {
		if strings.Contains(clean, absent) {
			t.Errorf("a clean run must not mention %q:\n%s", absent, clean)
		}
	}
}

// The paused warning outlives the group id when the name column runs out of room: a
// squeezed label drops the tail of the id, never the fact that consumers are paused.
func TestPausedMarkerSurvivesANarrowTerminal(t *testing.T) {
	snap := engine.Snapshot{Topics: []engine.TopicStat{{
		Topic: "orders",
		BrokerLag: map[string]int64{
			"a-very-long-consumer-group-name-indeed": 5,
		},
		Failed: 1,
		Groups: []engine.GroupStat{{
			GroupID: "a-very-long-consumer-group-name-indeed", Consumers: 4, Consumed: 5, Paused: 2,
		}},
	}}}
	m := New(newStubSource(snap), func() {})
	mm, _ := m.Update(tea.WindowSizeMsg{Width: 80, Height: 40})
	mm, _ = mm.Update(tickMsg(time.Now()))
	if out := plain(mm.View()); !strings.Contains(out, "(2 paused)") {
		t.Errorf("the paused marker went missing at 80 columns:\n%s", out)
	}
}

// The lag poller formats errors.Join errors, whose causes are newline-separated
// (internal/engine/lag.go), so multi-line event messages are a production reality. Each
// event must still cost exactly one line or the height budget collapses.
func TestMultiLineEventMessageStaysOneLine(t *testing.T) {
	snap := engine.Snapshot{
		Topics: []engine.TopicStat{{Topic: "orders"}},
		Events: []engine.Event{{
			At:      time.Now(),
			Message: "lag unavailable:\norders[0]: NOT_COORDINATOR\norders[1]: NOT_COORDINATOR",
		}},
	}
	m := New(newStubSource(snap), func() {})
	mm, _ := m.Update(tea.WindowSizeMsg{Width: 140, Height: 24})
	mm, _ = mm.Update(tickMsg(time.Now()))
	out := plain(mm.View())
	if got := strings.Count(out, "\n") + 1; got > 24 {
		t.Errorf("multi-line event pushed the frame to %d lines in a 24-row terminal:\n%s", got, out)
	}
	if !strings.Contains(out, "NOT_COORDINATOR") {
		t.Errorf("the event content must survive the newline collapse:\n%s", out)
	}
}

// The flow table is as unbounded as the topic list -- scenarios aggregate -- so it obeys
// the height budget too, and says what it cut.
func TestManyFlowsAreBoundedByTerminalHeight(t *testing.T) {
	snap := engine.Snapshot{Topics: []engine.TopicStat{{Topic: "orders"}}}
	for i := range 40 {
		snap.Flows = append(snap.Flows, engine.FlowStat{Name: fmt.Sprintf("flow-%02d", i)})
	}
	m := New(newStubSource(snap), func() {})
	mm, _ := m.Update(tea.WindowSizeMsg{Width: 100, Height: 24})
	mm, _ = mm.Update(tickMsg(time.Now()))
	out := plain(mm.View())
	if got := strings.Count(out, "\n") + 1; got > 24 {
		t.Errorf("40 flows rendered %d lines into a 24-row terminal:\n%s", got, out)
	}
	if !strings.Contains(out, "more flow") {
		t.Errorf("a cut flow list must say how many flows it is hiding:\n%s", out)
	}
}

// A cut that lands inside a topic's group list still counts what it hid: the note is in
// rows, so hidden group rows are never silently absent.
func TestHiddenGroupRowsAreCounted(t *testing.T) {
	snap := engine.Snapshot{Topics: []engine.TopicStat{{Topic: "orders"}}}
	for i := range 40 {
		snap.Topics[0].Groups = append(snap.Topics[0].Groups,
			engine.GroupStat{GroupID: fmt.Sprintf("group-%02d", i), Consumed: 1})
	}
	m := New(newStubSource(snap), func() {})
	mm, _ := m.Update(tea.WindowSizeMsg{Width: 100, Height: 24})
	mm, _ = mm.Update(tickMsg(time.Now()))
	out := plain(mm.View())
	if got := strings.Count(out, "\n") + 1; got > 24 {
		t.Errorf("rendered %d lines into a 24-row terminal:\n%s", got, out)
	}
	if !strings.Contains(out, "more row") {
		t.Errorf("hidden group rows must be counted in the note:\n%s", out)
	}
}

// The header keeps the run-state signals when width runs out: the scenario name (already
// known from the command line) yields before the degraded badge or the shutdown notice.
func TestHeaderKeepsStateMarkersOverTheScenarioName(t *testing.T) {
	snap := engine.Snapshot{
		At:       time.Now(),
		Scenario: "orders-pipeline-with-a-very-long-name",
		Elapsed:  time.Minute,
		Deadline: 10 * time.Minute,
		Healthy:  false,
		Stopping: true,
	}
	for _, w := range []int{40, 50, 60, 80} {
		m := New(newStubSource(snap), func() {})
		mm, _ := m.Update(tea.WindowSizeMsg{Width: w, Height: 40})
		mm, _ = mm.Update(tickMsg(time.Now()))
		header := strings.SplitN(plain(mm.View()), "\n", 2)[0]
		if got := lipgloss.Width(header); got > w {
			t.Errorf("width %d: header is %d cells wide: %q", w, got, header)
		}
		if !strings.Contains(header, "degraded") {
			t.Errorf("width %d: the degraded badge must outlive the scenario name: %q", w, header)
		}
		if !strings.Contains(header, "shutting down") {
			t.Errorf("width %d: the shutdown notice must outlive the scenario name: %q", w, header)
		}
	}
}

// Derived columns must not pop in or out as counters grow digits mid-run: their
// visibility depends on the terminal width and the config-gated column set only.
func TestDerivedColumnsAreStableAsCountersGrow(t *testing.T) {
	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	mk := func(sec int, produced int64) engine.Snapshot {
		return engine.Snapshot{
			At:      at.Add(time.Duration(sec) * time.Second),
			Elapsed: time.Duration(sec) * time.Second,
			Topics: []engine.TopicStat{{
				Topic: "orders", Produced: produced, Consumed: produced, Bytes: produced * 100,
			}},
			TotalProduced: produced,
			TotalConsumed: produced,
		}
	}
	// Small counters, then absurdly large ones: same width, same columns.
	src := newStubSource(mk(0, 10), mk(1, 1000), mk(2, 900_000_000_000))
	var m tea.Model = New(src, func() {})
	m, _ = m.Update(tea.WindowSizeMsg{Width: 118, Height: 40})
	m, _ = m.Update(tickMsg(time.Now()))
	m, _ = m.Update(tickMsg(time.Now()))
	small := strings.Contains(plain(m.View()), "MSG/S")
	m, _ = m.Update(tickMsg(time.Now()))
	big := strings.Contains(plain(m.View()), "MSG/S")
	if small != big {
		t.Errorf("MSG/S visibility changed with counter growth: small=%v big=%v", small, big)
	}
}

func TestComma(t *testing.T) {
	tests := []struct {
		in   int64
		want string
	}{
		{0, "0"},
		{7, "7"},
		{999, "999"},
		{1000, "1,000"},
		{12845, "12,845"},
		{1234567, "1,234,567"},
		{-5, "-5"},
		{-1234567, "-1,234,567"},
	}
	for _, tt := range tests {
		if got := comma(tt.in); got != tt.want {
			t.Errorf("comma(%d) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestHumanUnits(t *testing.T) {
	if got := humanRate(12400); got != "12.4k/s" {
		t.Errorf("humanRate(12400) = %q", got)
	}
	if got := humanRate(3.21); got != "3.2/s" {
		t.Errorf("humanRate(3.21) = %q", got)
	}
	if got := humanBytes(24_300_000); got != "24.3 MB" {
		t.Errorf("humanBytes = %q", got)
	}
	if got := humanBytes(512); got != "512 B" {
		t.Errorf("humanBytes = %q", got)
	}
	if got := humanByteRate(4_900_000); got != "4.9 MB/s" {
		t.Errorf("humanByteRate = %q", got)
	}
}

func TestSparkline(t *testing.T) {
	if got := sparkline(nil); got != "" {
		t.Errorf("sparkline(nil) = %q, want empty", got)
	}
	// A steady rate is a flat mid-height line, not a solid brick.
	if got := sparkline([]float64{100, 100, 100}); got != "▅▅▅" {
		t.Errorf("steady sparkline = %q, want mid-height", got)
	}
	// Zero is always the bottom block, and the range maps to the full height.
	got := sparkline([]float64{0, 50, 100})
	if []rune(got)[0] != '▁' {
		t.Errorf("zero cell = %q, want lowest block", string([]rune(got)[0]))
	}
	if []rune(got)[2] != '█' {
		t.Errorf("max cell = %q, want full block", string([]rune(got)[2]))
	}
	if n := len([]rune(got)); n != 3 {
		t.Errorf("sparkline is %d cells, want one per value", n)
	}
}

func TestProgressBar(t *testing.T) {
	if got := progressBar(time.Minute, 0, 20); got != "" {
		t.Errorf("no deadline must render no bar, got %q", got)
	}
	half := plainStr(progressBar(time.Minute, 2*time.Minute, 20))
	if strings.Count(half, "█") != 10 || strings.Count(half, "░") != 10 {
		t.Errorf("half-elapsed bar = %q, want 10 filled of 20", half)
	}
	over := plainStr(progressBar(3*time.Minute, 2*time.Minute, 10))
	if strings.Count(over, "█") != 10 {
		t.Errorf("past the deadline the bar clamps full, got %q", over)
	}
}

func plainStr(s string) string { return ansiRe.ReplaceAllString(s, "") }
