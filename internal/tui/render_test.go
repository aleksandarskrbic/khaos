package tui

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/aleksandarskrbic/khaos/internal/engine"
	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

var ansiRe = regexp.MustCompile("\x1b\\[[0-9;]*m")

// plain strips the styling so assertions can talk about content.
func plain(v tea.View) string { return ansiRe.ReplaceAllString(v.Content, "") }

// viewOf renders one snapshot through the real message path, exactly as the program would.
func viewOf(t *testing.T, snap engine.Snapshot) tea.View {
	t.Helper()
	m, _ := New(newStubSource(snap), func() {}).Update(tickMsg(time.Now()))
	return m.View()
}

// TestViewSurvivesAnySnapshot is the blast-radius test. cmd/khaos runs the TUI behind a
// recover (cmd/khaos/run.go), but a panicking dashboard still loses the operator their
// only view of a live incident, so nothing plausible may panic and nothing may emit
// invalid UTF-8 into the terminal.
func TestViewSurvivesAnySnapshot(t *testing.T) {
	manyTopics := make([]engine.TopicStat, 300)
	for i := range manyTopics {
		manyTopics[i] = engine.TopicStat{
			Topic:    fmt.Sprintf("topic-%03d", i),
			Produced: int64(i) * 1000,
			Consumed: int64(i) * 999,
			Lag:      int64(i),
			Groups:   []engine.GroupStat{{GroupID: fmt.Sprintf("group-%03d", i), Consumed: int64(i)}},
		}
	}

	manyEvents := make([]engine.Event, 500)
	for i := range manyEvents {
		manyEvents[i] = engine.Event{Message: fmt.Sprintf("event %d", i), Level: scenario.EventLevel(i % 4)}
	}

	tests := []struct {
		name string
		snap engine.Snapshot
	}{
		{"zero value", engine.Snapshot{}},
		{"no topics but a scenario", engine.Snapshot{Scenario: "empty", Elapsed: time.Second}},
		{"topic with no groups", engine.Snapshot{Topics: []engine.TopicStat{{Topic: "lonely"}}}},
		{"topic with an empty name", engine.Snapshot{Topics: []engine.TopicStat{{Topic: ""}}}},
		{
			name: "very long topic and group names",
			snap: engine.Snapshot{
				Topics: []engine.TopicStat{{
					Topic:  strings.Repeat("very-long-topic-name-", 40),
					Groups: []engine.GroupStat{{GroupID: strings.Repeat("g", 500), Paused: 3}},
				}},
			},
		},
		{
			name: "multi-byte names cut mid-rune",
			snap: engine.Snapshot{
				Scenario: strings.Repeat("日本語", 30),
				Topics: []engine.TopicStat{{
					Topic:  strings.Repeat("ünïcödé-", 20),
					Groups: []engine.GroupStat{{GroupID: strings.Repeat("héllo", 20)}},
				}},
				Flows: []engine.FlowStat{{Name: strings.Repeat("флоу", 30)}},
				Events: []engine.Event{
					{Message: strings.Repeat("🔥", 50), Level: scenario.EventAlert},
				},
			},
		},
		{"hundreds of topics", engine.Snapshot{Topics: manyTopics}},
		{"hundreds of events", engine.Snapshot{Events: manyEvents}},
		{
			name: "negative lag and huge counters",
			snap: engine.Snapshot{
				Topics: []engine.TopicStat{
					{Topic: "over-consumed", Produced: 1, Consumed: 1_000_000, Lag: -999_999},
				},
				TotalProduced: 1,
				TotalConsumed: 1 << 62,
			},
		},
		{
			name: "flow-only run, no topics",
			snap: engine.Snapshot{
				Flows: []engine.FlowStat{{Name: "checkout", Started: 1, InFlight: 1, Saturated: 9}},
			},
		},
		{"stopping with no deadline", engine.Snapshot{Stopping: true, Elapsed: 3 * time.Hour}},
		{"realistic", sampleSnapshot()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := viewOf(t, tt.snap)
			if v.Content == "" {
				t.Fatal("rendered nothing at all")
			}
			if !utf8.ValidString(v.Content) {
				t.Fatal("rendered invalid UTF-8; a name was sliced mid-rune")
			}
			if !strings.HasSuffix(plain(v), "q quit\n") && !strings.Contains(plain(v), "q quit") {
				t.Errorf("the quit hint went missing:\n%s", plain(v))
			}
		})
	}
}

// The Failed and DLQ columns appear only when failure simulation is in play. The engine
// snapshot has no "enabled" flag, so non-zero counters stand in for it -- see showFailures.
func TestFailureColumnsAppearOnlyWhenFailuresHappened(t *testing.T) {
	tests := []struct {
		name  string
		topic engine.TopicStat
		want  bool
	}{
		{"clean run", engine.TopicStat{Topic: "orders", Produced: 10, Consumed: 10}, false},
		{"explicit zeros", engine.TopicStat{Topic: "orders", Failed: 0, DLQ: 0, Retries: 0}, false},
		{"a simulated failure", engine.TopicStat{Topic: "orders", Failed: 1}, true},
		{"a dead-letter message", engine.TopicStat{Topic: "orders", DLQ: 1}, true},
		{"a retry, before anything failed for good", engine.TopicStat{Topic: "orders", Retries: 1}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := plain(viewOf(t, engine.Snapshot{Topics: []engine.TopicStat{tt.topic}}))
			gotFailed := strings.Contains(out, "FAILED")
			gotDLQ := strings.Contains(out, "DLQ")
			if gotFailed != tt.want || gotDLQ != tt.want {
				t.Fatalf("FAILED=%v DLQ=%v, want both %v:\n%s", gotFailed, gotDLQ, tt.want, out)
			}
		})
	}
}

// One topic with failures turns the columns on for the whole table. The failing topic's
// counts land under the FAILED and DLQ headers -- right-aligned cells share their header's
// right edge -- and the clean topic's cells stay blank rather than showing a zero.
func TestOneFailingTopicAddsTheColumnsForEveryTopic(t *testing.T) {
	snap := engine.Snapshot{Topics: []engine.TopicStat{
		{Topic: "clean", Produced: 55, Consumed: 55},
		{Topic: "failing", Produced: 55, Consumed: 44, Failed: 7, DLQ: 9},
	}}

	out := plain(viewOf(t, snap))
	var headerLine, cleanRow, failingRow string
	for _, line := range strings.Split(out, "\n") {
		switch {
		case strings.Contains(line, "FAILED"):
			headerLine = line
		case strings.Contains(line, "clean"):
			cleanRow = line
		case strings.Contains(line, "failing"):
			failingRow = line
		}
	}
	if headerLine == "" {
		t.Fatalf("columns missing entirely:\n%s", out)
	}
	if cleanRow == "" || failingRow == "" {
		t.Fatalf("missing topic rows:\n%s", out)
	}

	failedEdge := strings.Index(headerLine, "FAILED") + len("FAILED")
	dlqEdge := strings.Index(headerLine, "DLQ") + len("DLQ")
	if got := strings.Index(failingRow, "7") + 1; got != failedEdge {
		t.Errorf("failed count right edge at %d, header FAILED ends at %d:\n%s", got, failedEdge, out)
	}
	if got := strings.Index(failingRow, "9") + 1; got != dlqEdge {
		t.Errorf("dlq count right edge at %d, header DLQ ends at %d:\n%s", got, dlqEdge, out)
	}
	// The clean topic's cells under those headers are blank, not zero.
	for _, edge := range []int{failedEdge, dlqEdge} {
		if c := cleanRow[edge-1]; c != ' ' {
			t.Errorf("clean row shows %q under a failure header, want blank:\n%s", string(c), out)
		}
	}
}

// Lag colours red above 100 and green otherwise.
func TestLagColouring(t *testing.T) {
	tests := []struct {
		name string
		lag  int64
		want string
	}{
		{"zero", 0, okStyle.Render("0")},
		{"negative", -5, okStyle.Render("-5")},
		{"at the threshold", 100, okStyle.Render("100")},
		{"one past the threshold", 101, alertStyle.Render("101")},
		{"far past, with separators", 1_000_000, alertStyle.Render("1,000,000")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := lagCell(tt.lag); got != tt.want {
				t.Fatalf("lagCell(%d) = %q, want %q", tt.lag, got, tt.want)
			}
		})
	}

	// And the two sides really are visually distinct, not just equal strings with the
	// colour profile stripped.
	if okStyle.Render("100") == alertStyle.Render("100") {
		t.Fatal("ok and alert styles render identically; the threshold is invisible")
	}
}

// The TOTAL line derives its lag from the totals, so it flips colour at the same point.
func TestTotalsLagUsesTheSameThreshold(t *testing.T) {
	within := engine.Snapshot{TotalProduced: 100, TotalConsumed: 0}
	beyond := engine.Snapshot{TotalProduced: 101, TotalConsumed: 0}

	if !strings.Contains(viewOf(t, within).Content, okStyle.Render("100")) {
		t.Error("total lag of 100 is not green")
	}
	if !strings.Contains(viewOf(t, beyond).Content, alertStyle.Render("101")) {
		t.Error("total lag of 101 is not red")
	}
}

// A paused consumer must not shove the tree's columns out of alignment: the marker is
// appended after padding, never folded into the padded label.
func TestPausedMarkerDoesNotBreakColumnAlignment(t *testing.T) {
	snap := engine.Snapshot{Topics: []engine.TopicStat{{
		Topic: "orders",
		Groups: []engine.GroupStat{
			{GroupID: "with-pause", Consumed: 5, Paused: 2},
			{GroupID: "without", Consumed: 5},
		},
	}}}

	out := plain(viewOf(t, snap))
	var idx []int
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "with-pause") || strings.Contains(line, "without") {
			idx = append(idx, strings.LastIndex(line, "5"))
		}
	}
	if len(idx) != 2 {
		t.Fatalf("expected two group rows, got %d:\n%s", len(idx), out)
	}
	if idx[0] != idx[1] {
		t.Fatalf("the paused row's consumed column sits at %d, the other at %d:\n%s", idx[0], idx[1], out)
	}
	if !strings.Contains(out, "(2 paused)") {
		t.Fatalf("the paused marker went missing:\n%s", out)
	}
}

func TestRecentEventsKeepsTheNewest(t *testing.T) {
	events := make([]engine.Event, 20)
	for i := range events {
		events[i] = engine.Event{Message: fmt.Sprintf("event-%02d", i)}
	}

	tests := []struct {
		name       string
		events     []engine.Event
		n          int
		wantEmpty  bool
		wantFirst  string
		wantAbsent string
	}{
		{name: "no events renders nothing", events: nil, n: 6, wantEmpty: true},
		{name: "fewer than the cap", events: events[:3], n: 6, wantFirst: "event-00"},
		{name: "more than the cap keeps the tail", events: events, n: 6, wantFirst: "event-14", wantAbsent: "event-13"},
		{name: "cap of zero keeps nothing", events: events, n: 0, wantAbsent: "event-19"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ansiRe.ReplaceAllString(recentEvents(tt.events, tt.n, 0), "")
			if tt.wantEmpty {
				if got != "" {
					t.Fatalf("want no events block, got %q", got)
				}
				return
			}
			if tt.wantFirst != "" && !strings.Contains(got, tt.wantFirst) {
				t.Errorf("want %q in:\n%s", tt.wantFirst, got)
			}
			if tt.wantAbsent != "" && strings.Contains(got, tt.wantAbsent) {
				t.Errorf("did not want %q in:\n%s", tt.wantAbsent, got)
			}
		})
	}
}

func TestEventLevelsAreColoured(t *testing.T) {
	tests := []struct {
		level scenario.EventLevel
		style string
	}{
		{scenario.EventInfo, dimStyle.Render("msg")},
		{scenario.EventWarn, warnStyle.Render("msg")},
		{scenario.EventAlert, alertStyle.Render("msg")},
		{scenario.EventRecovery, okStyle.Render("msg")},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.level), func(t *testing.T) {
			got := recentEvents([]engine.Event{{Message: "msg", Level: tt.level}}, 6, 0)
			if !strings.Contains(got, tt.style) {
				t.Fatalf("level %v did not render as %q:\n%q", tt.level, tt.style, got)
			}
		})
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		name string
		in   string
		n    int
		want string
	}{
		{"short enough", "orders", 28, "orders"},
		{"exactly at the limit", strings.Repeat("a", 28), 28, strings.Repeat("a", 28)},
		{"one over", strings.Repeat("a", 29), 28, strings.Repeat("a", 27) + "…"},
		{"empty", "", 28, ""},
		{"n of one", "orders", 1, "…"},
		{"n of zero", "orders", 0, ""},
		{"negative n does not panic", "orders", -3, ""},
		// The reason this is cell-aware rather than rune-aware: a kanji occupies two
		// terminal cells, so counting runes overshoots the column and shears the table.
		{"multi-byte, under the limit in cells", "日本語トピック", 28, "日本語トピック"},
		{"multi-byte, over the limit", strings.Repeat("日", 30), 5, "日日…"},
		{"emoji", strings.Repeat("🔥", 10), 4, "🔥…"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncate(tt.in, tt.n)
			if got != tt.want {
				t.Fatalf("truncate(%q, %d) = %q, want %q", tt.in, tt.n, got, tt.want)
			}
			if !utf8.ValidString(got) {
				t.Fatalf("truncate produced invalid UTF-8: %q", got)
			}
			if tt.n > 0 && lipgloss.Width(got) > tt.n {
				t.Fatalf("truncate returned %d cells, want at most %d", lipgloss.Width(got), tt.n)
			}
		})
	}
}

func TestFmtDuration(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want string
	}{
		{0, "0m00s"},
		{999 * time.Millisecond, "0m01s"},
		{59 * time.Second, "0m59s"},
		{90 * time.Second, "1m30s"},
		{59*time.Minute + 59*time.Second, "59m59s"},
		{time.Hour, "1h00m00s"},
		{25*time.Hour + 3*time.Minute + 4*time.Second, "25h03m04s"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := fmtDuration(tt.in); got != tt.want {
				t.Fatalf("fmtDuration(%s) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// The header carries everything an operator needs to know the run is alive: scenario,
// elapsed, budget, and whether shutdown has begun.
func TestHeaderReportsRunState(t *testing.T) {
	out := plain(viewOf(t, sampleSnapshot()))
	for _, want := range []string{"khaos", "black-friday", "elapsed 1m35s", "/ 5m00s"} {
		if !strings.Contains(out, want) {
			t.Errorf("header is missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "shutting down") {
		t.Error("a running scenario must not claim it is shutting down")
	}

	stopping := sampleSnapshot()
	stopping.Stopping = true
	if !strings.Contains(plain(viewOf(t, stopping)), "shutting down") {
		t.Error("a stopping run must say so")
	}

	// A run with no duration configured shows elapsed but no budget.
	open := sampleSnapshot()
	open.Deadline = 0
	if strings.Contains(plain(viewOf(t, open)), "/ ") {
		t.Error("a run with no deadline must not render a budget")
	}
}

func TestFlowTableRendersOnlyWhenThereAreFlows(t *testing.T) {
	without := plain(viewOf(t, engine.Snapshot{Topics: []engine.TopicStat{{Topic: "orders"}}}))
	if strings.Contains(without, "INFLIGHT") {
		t.Errorf("flow table rendered with no flows:\n%s", without)
	}

	with := plain(viewOf(t, sampleSnapshot()))
	for _, want := range []string{"FLOW", "checkout", "INFLIGHT", "saturated x5"} {
		if !strings.Contains(with, want) {
			t.Errorf("flow table is missing %q:\n%s", want, with)
		}
	}
}

func TestTotalsLineReportsErrorsAndRebalances(t *testing.T) {
	out := plain(viewOf(t, sampleSnapshot()))
	for _, want := range []string{"TOTAL", "12,845", "12,498", "4 errors", "2 rebalances"} {
		if !strings.Contains(out, want) {
			t.Errorf("totals line is missing %q:\n%s", want, out)
		}
	}

	quiet := plain(viewOf(t, engine.Snapshot{TotalProduced: 1, TotalConsumed: 1}))
	if strings.Contains(quiet, "errors") || strings.Contains(quiet, "rebalances") {
		t.Errorf("a clean run must not advertise zero errors or rebalances:\n%s", quiet)
	}
}

// brokerLagSnapshot is a run with real lag polling on: one topic measured across two
// groups, one group inside it unmeasured, and one topic not measured at all.
func brokerLagSnapshot() engine.Snapshot {
	return engine.Snapshot{
		Scenario: "with-lag",
		Topics: []engine.TopicStat{
			{
				Topic:    "orders",
				Produced: 1000,
				Consumed: 990,
				Lag:      10,
				// Real lag disagrees with the self-report by two orders of magnitude,
				// which is the entire reason this column exists: khaos read the records
				// but the group has not committed them.
				BrokerLag: map[string]int64{"order-processors": 800},
				Groups: []engine.GroupStat{
					{GroupID: "order-processors", Consumers: 2, Consumed: 990},
					{GroupID: "order-auditors", Consumers: 1, Consumed: 500},
				},
			},
			{
				Topic:    "payments",
				Produced: 50,
				Consumed: 50,
				Lag:      0,
				Groups:   []engine.GroupStat{{GroupID: "payment-processors", Consumed: 50}},
			},
		},
		TotalProduced: 1050,
		TotalConsumed: 1040,
	}
}

// The default is unchanged: no polling, no second column, nothing renamed.
func TestBrokerLagColumnIsAbsentWhenNothingWasMeasured(t *testing.T) {
	out := plain(viewOf(t, sampleSnapshot()))
	if strings.Contains(out, "BROKER") {
		t.Errorf("broker lag column rendered with no measurements:\n%s", out)
	}
	if strings.Contains(out, "LAG(SELF)") {
		t.Errorf("the self-reported column was renamed with nothing to distinguish it from:\n%s", out)
	}
	if !strings.Contains(out, "LAG") {
		t.Errorf("the lag column disappeared:\n%s", out)
	}
	if strings.Contains(out, "unknown") {
		t.Errorf("unmeasured lag leaked into a table that has no broker column:\n%s", out)
	}
}

// Both columns are shown side by side, labelled, and they may disagree. The owner asked
// for the self-report to stay visible through the transition rather than be silently
// replaced by a number sourced somewhere else.
func TestBrokerLagColumnShowsBothLagSources(t *testing.T) {
	out := plain(viewOf(t, brokerLagSnapshot()))

	for _, want := range []string{"LAG(SELF)", "LAG(BROKER)"} {
		if !strings.Contains(out, want) {
			t.Fatalf("header is missing %q:\n%s", want, out)
		}
	}

	var row string
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(strings.TrimLeft(line, "│ "), "orders") {
			row = line
		}
	}
	if row == "" {
		t.Fatalf("no row for the measured topic:\n%s", out)
	}
	// Both numbers are present as their own cells, self-report before broker, exactly as
	// disagreeing as the fixture says they are.
	fields := strings.Fields(strings.Trim(row, "│ "))
	self, broker := -1, -1
	for i, f := range fields {
		switch f {
		case "10":
			self = i
		case "800":
			broker = i
		}
	}
	if self == -1 || broker == -1 {
		t.Fatalf("row = %v, want self lag 10 alongside broker lag 800", fields)
	}
	if self >= broker {
		t.Fatalf("row = %v, want the self-reported lag to the left of the broker lag", fields)
	}
}

// A topic nobody could measure says so, in a word, rather than showing a zero. Same for a
// single group inside a topic that was otherwise measured.
func TestBrokerLagRendersUnknownRatherThanZero(t *testing.T) {
	out := plain(viewOf(t, brokerLagSnapshot()))

	byPrefix := func(prefix string) string {
		for _, line := range strings.Split(out, "\n") {
			if strings.Contains(line, prefix) {
				return line
			}
		}
		t.Fatalf("no line containing %q:\n%s", prefix, out)
		return ""
	}
	// The broker column is the table's last, so after stripping the border the row ends
	// with its broker-lag cell.
	lastCell := func(line string) string {
		return strings.TrimRight(line, "│ ")
	}

	if got := byPrefix("payments"); !strings.HasSuffix(lastCell(got), "unknown") {
		t.Errorf("unmeasured topic row = %q, want it to end in unknown", got)
	}
	if got := byPrefix("order-auditors"); !strings.HasSuffix(lastCell(got), "unknown") {
		t.Errorf("unmeasured group row = %q, want it to end in unknown", got)
	}
	if got := byPrefix("order-processors"); !strings.HasSuffix(lastCell(got), "800") {
		t.Errorf("measured group row = %q, want it to end in its own lag", got)
	}
	// The TOTAL line sums only what was measured.
	if got := byPrefix("TOTAL"); !strings.Contains(got, "800") {
		t.Errorf("totals line = %q, want the measured broker lag total", got)
	}
}

// The broker column obeys the same threshold as the self-reported one, and unmeasured is
// visually distinct from both.
func TestBrokerLagColouring(t *testing.T) {
	tests := []struct {
		name  string
		lag   int64
		known bool
		want  string
	}{
		{"zero", 0, true, okStyle.Render("0")},
		{"at the threshold", 100, true, okStyle.Render("100")},
		{"one past it", 101, true, alertStyle.Render("101")},
		{"unmeasured", 0, false, dimStyle.Render("unknown")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := brokerLagCell(tt.lag, tt.known); got != tt.want {
				t.Fatalf("brokerLagCell(%d, %v) = %q, want %q", tt.lag, tt.known, got, tt.want)
			}
		})
	}

	// The threshold has to be the same one, not merely a similar number: the two columns
	// are meant to be read against each other.
	if lagAlertThreshold != 100 {
		t.Fatalf("lagAlertThreshold = %d, want 100",
			lagAlertThreshold)
	}
}

// Adding a column must not shear the table. The unmeasured cells are the risky ones --
// they are a word where every other row has digits, and they are styled dim. In a
// bordered table a sheared row is one whose closing border drifts, so every table line
// must render at exactly the same width.
func TestBrokerLagColumnKeepsColumnsAligned(t *testing.T) {
	out := plain(viewOf(t, brokerLagSnapshot()))

	var widths []int
	var rows int
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "│") {
			widths = append(widths, lipgloss.Width(line))
		}
		switch {
		case strings.Contains(line, "│ orders"), strings.Contains(line, "│ payments"),
			strings.Contains(line, "order-processors"), strings.Contains(line, "order-auditors"):
			rows++
		}
	}
	if rows != 4 {
		t.Fatalf("expected 4 table rows, found %d:\n%s", rows, out)
	}
	for _, w := range widths {
		if w != widths[0] {
			t.Fatalf("table lines have differing widths %v; the broker column is not aligned:\n%s",
				widths, out)
		}
	}
}

// Whatever the poller produced, the dashboard must survive it.
func TestBrokerLagSurvivesDegenerateValues(t *testing.T) {
	snaps := []engine.Snapshot{
		{Topics: []engine.TopicStat{{Topic: "empty-map", BrokerLag: map[string]int64{}}}},
		{Topics: []engine.TopicStat{{
			Topic:     "huge",
			BrokerLag: map[string]int64{"g": 1 << 62},
			Groups:    []engine.GroupStat{{GroupID: "g"}},
		}}},
		{Topics: []engine.TopicStat{{
			Topic:     "negative",
			BrokerLag: map[string]int64{"g": -1},
			Groups:    []engine.GroupStat{{GroupID: "other"}},
		}}},
		{Topics: []engine.TopicStat{{
			Topic:     strings.Repeat("ünïcödé-", 20),
			BrokerLag: map[string]int64{strings.Repeat("g", 400): 5},
			Groups:    []engine.GroupStat{{GroupID: strings.Repeat("g", 400), Paused: 2}},
		}}},
	}

	for i, snap := range snaps {
		out := viewOf(t, snap).Content
		if !utf8.ValidString(out) {
			t.Errorf("snapshot %d rendered invalid UTF-8", i)
		}
		if !strings.Contains(ansiRe.ReplaceAllString(out, ""), "LAG") {
			t.Errorf("snapshot %d lost the lag columns", i)
		}
	}
}
