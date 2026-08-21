package tui

import (
	"fmt"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
	"charm.land/lipgloss/v2/table"
	"github.com/aleksandarskrbic/khaos/internal/engine"
	"github.com/aleksandarskrbic/khaos/internal/scenario"
	"github.com/aleksandarskrbic/khaos/internal/theme"
)

// Colours come from the shared palette in internal/theme: cyan names, green produced,
// yellow consumed, magenta failed, blue DLQ, dim child rows, red-over-green lag -- a fixed
// vocabulary so the same number always wears the same colour across every screen.
//
// Headers and other emphasis text are bold in the terminal's own foreground rather than
// painted ANSI white: bright-white-on-white is invisible on a light background, and the
// whole point of palette indices is that the terminal theme keeps everything legible.
var (
	titleStyle  = lipgloss.NewStyle().Bold(true).Foreground(theme.Magenta)
	headerStyle = lipgloss.NewStyle().Bold(true)
	dimStyle    = lipgloss.NewStyle().Faint(true)
	okStyle     = lipgloss.NewStyle().Foreground(theme.Green)
	warnStyle   = lipgloss.NewStyle().Foreground(theme.Yellow)
	alertStyle  = lipgloss.NewStyle().Bold(true).Foreground(theme.Red)
	borderStyle = lipgloss.NewStyle().Foreground(theme.Gray)

	topicStyle = lipgloss.NewStyle().Bold(true).Foreground(theme.Cyan)
	prodStyle  = lipgloss.NewStyle().Foreground(theme.Green)
	prodBold   = prodStyle.Bold(true)
	consStyle  = lipgloss.NewStyle().Foreground(theme.Yellow)
	consBold   = consStyle.Bold(true)
	failStyle  = lipgloss.NewStyle().Foreground(theme.Magenta)
	dlqStyle   = lipgloss.NewStyle().Foreground(theme.Blue)
)

// Width limits: below minWidth the layout stops adapting and clips; above maxWidth extra
// terminal columns stay empty rather than stretching rows unreadably wide. maxNameWidth
// caps the name column for the same reason -- past ~48 cells the gap to the numbers grows
// wider than the numbers themselves.
const (
	minWidth     = 40
	maxWidth     = 150
	minNameWidth = 14
	maxNameWidth = 48
)

// narrowWidth is where the layout tightens: inter-column padding shrinks and long column
// titles compact, buying the name column room it otherwise loses whole names to.
const narrowWidth = 96

func (m model) render() string {
	s := m.snap
	w := m.width
	if w <= 0 {
		w = 100
	}
	w = max(minWidth, min(w, maxWidth))
	h := m.height
	if h <= 0 {
		h = 40
	}

	// Height budget: fixed chrome first (header, totals, quit hint, optional rule and
	// issues line), then remaining space splits between the topic table, flow table (capped
	// at a third) and events, which give up lines one by one first since losing an old event
	// beats losing sight of a topic. The recurring 4 is a bordered table's frame -- top
	// border, header, header rule, bottom border -- and the floor is an 8-line frame
	// (header, one-row table, totals, hint); below that the layout clips rather than
	// contorts.
	issues := m.issuesLine(w)
	showRule := h >= 16
	showIssues := issues != "" && h >= 12
	fixed := 3
	if showRule {
		fixed++
	}
	if showIssues {
		fixed++
	}

	eventsWant := 0
	if len(s.Events) > 0 {
		eventsWant = min(6, len(s.Events))
	}
	showFlows := len(s.Flows) > 0 && h >= 13
	flowFrame := 0
	if showFlows {
		flowFrame = 4
	}

	avail := h - fixed - eventsHeight(eventsWant) - 4 - flowFrame
	for avail < 4 && eventsWant > 0 {
		eventsWant--
		avail = h - fixed - eventsHeight(eventsWant) - 4 - flowFrame
	}
	flowBudget := 0
	if showFlows {
		flowBudget = min(len(s.Flows), max(1, avail/3))
	}
	topicBudget := max(avail-flowBudget, 1)

	// Layout.
	//
	// The topic table is rendered first because its width is the frame everything else
	// aligns to: header, rules, flow table, events and totals all share one right edge.
	topics := m.topicTable(w, topicBudget)
	tw := lipgloss.Width(topics)

	var b strings.Builder
	b.WriteString(m.header(tw))
	b.WriteString("\n")
	b.WriteString(topics)
	b.WriteString("\n")
	if showFlows {
		b.WriteString(m.flowTable(tw, flowBudget))
		b.WriteString("\n")
	}
	if showRule {
		b.WriteString(borderStyle.Render(strings.Repeat("─", tw)))
		b.WriteString("\n")
	}
	b.WriteString(m.totalsBar(tw, showFlows))
	b.WriteString("\n")
	if showIssues {
		b.WriteString(issues)
		b.WriteString("\n")
	}
	if ev := recentEvents(s.Events, eventsWant, tw); ev != "" {
		b.WriteString(ev)
	}
	b.WriteString(dimStyle.Render(" q quit"))
	return b.String()
}

func eventsHeight(n int) int {
	if n <= 0 {
		return 0
	}
	return n + 1 // section title + one line per event
}

// header is one line: identity and run state on the left, time on the right. As width runs
// out, the progress bar yields first, then the scenario name, but never the health badge or
// shutdown notice -- those exist only here. State markers grow from the left into the empty
// gap so they never shift the bar and clock sideways mid-run.
func (m model) header(w int) string {
	s := m.snap

	buildRight := func(withBar bool) string {
		r := ""
		if withBar && s.Deadline > 0 {
			if bar := progressBar(s.Elapsed, s.Deadline, barWidth(w)); bar != "" {
				r += bar + "  "
			}
		}
		r += dimStyle.Render("elapsed ") + headerStyle.Render(fmtDuration(s.Elapsed))
		if s.Deadline > 0 {
			r += dimStyle.Render(" / " + fmtDuration(s.Deadline))
		}
		return r + " "
	}

	title := " " + titleStyle.Render("khaos")
	// Healthy comes from the engine and drives /healthz; a run that has recorded an
	// unrecoverable condition should say so on the screen too, not only over HTTP. The
	// zero-value snapshot before the first tick is not "degraded", just unknown.
	badge := ""
	if !s.At.IsZero() {
		if s.Healthy {
			badge = "  " + okStyle.Render("●")
		} else {
			badge = "  " + alertStyle.Render("● degraded")
		}
	}
	stop := ""
	if s.Stopping {
		stop = "  " + warnStyle.Render("shutting down")
	}

	right := buildRight(true)
	sep := 0
	if s.Scenario != "" {
		sep = lipgloss.Width(" · ")
	}
	scenMax := func() int {
		return w - lipgloss.Width(right) - lipgloss.Width(title) -
			lipgloss.Width(badge) - lipgloss.Width(stop) - sep - 2
	}
	if scenMax() < 8 {
		right = buildRight(false)
	}

	left := title
	if s.Scenario != "" {
		if scen := truncate(s.Scenario, max(scenMax(), 0)); scen != "" {
			left += dimStyle.Render(" · ") + headerStyle.Render(scen)
		}
	}
	left += badge + stop

	gap := max(w-lipgloss.Width(left)-lipgloss.Width(right), 1)
	// The final truncate is a belt: whatever survived above must not wrap, because a
	// wrapped header shifts every line beneath it for that frame.
	return truncate(left+strings.Repeat(" ", gap)+right, w)
}

func barWidth(w int) int {
	switch {
	case w >= 110:
		return 20
	case w >= 90:
		return 14
	case w >= 76:
		return 10
	}
	return 0
}

// progressBar draws run progress against the -d deadline. Elapsed and Deadline were
// already in the snapshot; rendering them only as text wasted the one number the user
// most wants to see at a glance.
func progressBar(elapsed, deadline time.Duration, width int) string {
	if width < 4 || deadline <= 0 {
		return ""
	}
	frac := float64(elapsed) / float64(deadline)
	frac = max(0, min(frac, 1))
	filled := int(frac*float64(width) + 0.5)
	return okStyle.Render(strings.Repeat("█", filled)) + dimStyle.Render(strings.Repeat("░", width-filled))
}

// showFailures reports whether the Failed and DLQ columns should render: on when some
// consumer has failure simulation enabled, a config decision made once at startup and
// passed in via ShowFailureColumns. Without an explicit gate this falls back to inferring
// it from live counters.
func (m model) showFailures() bool {
	switch m.failCols {
	case gateOn:
		return true
	case gateOff:
		return false
	}
	for _, t := range m.snap.Topics {
		if t.Failed > 0 || t.DLQ > 0 || t.Retries > 0 {
			return true
		}
	}
	return false
}

// showBrokerLag reports whether the LAG(BROKER) column is in play, decided by --lag-poll
// via ShowBrokerLag. Without an explicit gate: any topic carrying a measurement.
//
// Nil BrokerLag is "not measured", which is the default (--lag-poll unset) and also what a
// cluster that denies DESCRIBE on consumer groups leaves behind.
func (m model) showBrokerLag() bool {
	switch m.lagCols {
	case gateOn:
		return true
	case gateOff:
		return false
	}
	for _, t := range m.snap.Topics {
		if t.BrokerLag != nil {
			return true
		}
	}
	return false
}

// padFor is the per-column padding (left + right) at width w: airy by default, tighter
// when the terminal is narrow so the name column keeps room to breathe.
func padFor(w int) int {
	if w < narrowWidth {
		return 2
	}
	return 3
}

// styledTable is the one table look every khaos screen shares: rounded border, no inner
// rules, bold headers, name column left-aligned and every numeric column right-aligned so
// they share a comparable edge. Width(w) is only a clamp -- callers size the name column
// themselves via firstColWidth so the numbers stay a tight block rather than being scattered
// by even column expansion.
func styledTable(headers []string, rows [][]string, w, pad int) string {
	t := table.New().
		Border(lipgloss.RoundedBorder()).
		BorderStyle(borderStyle).
		BorderRow(false).
		BorderColumn(false).
		Wrap(false).
		StyleFunc(func(row, col int) lipgloss.Style {
			st := lipgloss.NewStyle().PaddingLeft(1).PaddingRight(pad - 1)
			if col > 0 {
				st = st.AlignHorizontal(lipgloss.Right)
			}
			if row == table.HeaderRow {
				return st.Inherit(headerStyle)
			}
			return st
		}).
		Headers(headers...).
		Rows(rows...).
		Width(w)
	return t.String()
}

// colSpec is one numeric column: a stable key the cell maps use, and the title shown in
// the header row (which may compact at narrow widths while the key stays put).
type colSpec struct {
	key   string
	title string
}

// colWidths measures what each numeric column needs: its widest cell or its title,
// whichever wins.
func colWidths(specs []colSpec, cells []map[string]string) []int {
	out := make([]int, len(specs))
	for i, sp := range specs {
		w := lipgloss.Width(sp.title)
		for _, c := range cells {
			w = max(w, lipgloss.Width(c[sp.key]))
		}
		out[i] = w
	}
	return out
}

// firstColWidth hands every terminal column not claimed by the numeric block to the name
// column, which is where the slack belongs: names truncate, numbers must not.
func firstColWidth(w int, numeric []int, pad int) int {
	used := 2 + pad // borders + the name column's own padding
	for _, n := range numeric {
		used += n + pad
	}
	return w - used
}

// padHeader pins a column to width n by padding its title, the only per-column width
// control lipgloss's table offers.
func padHeader(h string, n int) string {
	if d := n - lipgloss.Width(h); d > 0 {
		return h + strings.Repeat(" ", d)
	}
	return h
}

// Width thresholds for the derived columns, a function of terminal width and the
// config-gated column set only -- never of how many digits the counters have grown to --
// so a column present at the first frame stays present for the whole run.
func rateThreshold(broker, failures bool) int {
	t := 78
	if broker {
		t += 14
	}
	if failures {
		t += 16
	}
	return t
}

func (m model) topicTable(w, budget int) string {
	broker := m.showBrokerLag()
	failures := m.showFailures()
	narrow := w < narrowWidth
	pad := padFor(w)

	// The two lag columns coexist rather than one replacing the other: LAG(SELF) is
	// produced-minus-consumed from khaos's own counters, LAG(BROKER) is committed offset
	// versus log end offset straight from the cluster, and their disagreements are
	// informative rather than noise: SELF far below BROKER means khaos's consumers are
	// not committing (or something else is producing), and BROKER far below SELF means
	// another consumer in the same group is doing the work.
	lagTitle, brokerTitle := "LAG", "LAG(BROKER)"
	if broker {
		lagTitle = "LAG(SELF)"
		if narrow {
			lagTitle, brokerTitle = "SELF", "BROKER"
		}
	}
	specs := []colSpec{{"produced", "PRODUCED"}, {"consumed", "CONSUMED"}, {"lag", lagTitle}}
	if broker {
		specs = append(specs, colSpec{"broker", brokerTitle})
	}
	if failures {
		specs = append(specs, colSpec{"failed", "FAILED"}, colSpec{"dlq", "DLQ"})
	}

	// Build the rows that fit the height budget. Labels are rendered later, once the
	// name-column width is known; numeric cells are final now and drive that width.
	type rowData struct {
		label func(nameW int) string
		cells map[string]string
	}
	var data []rowData
	totalRows := 0
	for _, t := range m.snap.Topics {
		totalRows += 1 + len(t.Groups)
	}
	if totalRows > budget {
		budget = max(budget-1, 1) // reserve a row for the "… N more" note
	}
	shown := 0
	for i := range m.snap.Topics {
		if shown >= budget {
			break
		}
		t := m.snap.Topics[i]
		data = append(data, rowData{
			label: func(nameW int) string { return topicStyle.Render(truncate(t.Topic, nameW)) },
			cells: m.topicCells(t, broker, failures),
		})
		shown++
		for gi := range t.Groups {
			if shown >= budget {
				break
			}
			g := t.Groups[gi]
			last := gi == len(t.Groups)-1
			data = append(data, rowData{
				label: func(nameW int) string { return groupLabel(g, last, nameW) },
				cells: groupCells(t, g, broker, failures),
			})
			shown++
		}
	}
	// Counted in rows, not topics: a cut that lands inside a topic's group list hides
	// group rows too, and a note that undercounts makes the frame lie about coverage.
	hidden := totalRows - shown
	allCells := make([]map[string]string, len(data))
	for i, r := range data {
		allCells[i] = r.cells
	}

	// The derived columns join by the fixed thresholds above, provided they have
	// anything to say at all this run.
	has := func(key string) bool {
		for _, c := range allCells {
			if c[key] != "" {
				return true
			}
		}
		return false
	}
	if has("rate") && w >= rateThreshold(broker, failures) {
		specs = append(specs, colSpec{"rate", "MSG/S"})
		if has("bytes") && w >= rateThreshold(broker, failures)+16 {
			specs = append(specs, colSpec{"bytes", "BYTES"})
		}
	}

	// Name-column width: whatever the numeric block leaves behind, within its bounds.
	// If even the minimum does not fit, retry with compact padding before letting the
	// table clamp -- a clamped table shrinks columns of its own choosing.
	nameW := firstColWidth(w, colWidths(specs, allCells), pad)
	if nameW < minNameWidth && pad > 2 {
		pad = 2
		nameW = firstColWidth(w, colWidths(specs, allCells), pad)
	}
	nameW = max(min(nameW, maxNameWidth), minNameWidth)
	tw := min(w, nameW+pad+2+sumPlus(colWidths(specs, allCells), pad))

	headers := make([]string, 0, len(specs)+1)
	headers = append(headers, padHeader("TOPIC", nameW))
	for _, sp := range specs {
		headers = append(headers, sp.title)
	}
	rows := make([][]string, 0, len(data)+1)
	for _, r := range data {
		row := make([]string, 0, len(headers))
		row = append(row, r.label(nameW))
		for _, sp := range specs {
			row = append(row, r.cells[sp.key])
		}
		rows = append(rows, row)
	}
	if hidden > 0 {
		note := make([]string, len(headers))
		note[0] = dimStyle.Render(truncate("… "+plural(int64(hidden), "more row"), nameW))
		rows = append(rows, note)
	}
	return styledTable(headers, rows, tw, pad)
}

func sumPlus(ws []int, pad int) int {
	total := 0
	for _, w := range ws {
		total += w + pad
	}
	return total
}

// topicCells is a topic row's numeric cells, keyed by colSpec key. Rate and bytes are
// always computed; whether their columns render is the layout's decision.
func (m model) topicCells(t engine.TopicStat, broker, failures bool) map[string]string {
	cells := map[string]string{
		"produced": prodBold.Render(comma(t.Produced)),
		"consumed": consBold.Render(comma(t.Consumed)),
		"lag":      lagCell(t.Lag),
	}
	if broker {
		total, known := topicBrokerLag(t)
		cells["broker"] = brokerLagCell(total, known)
	}
	if failures {
		// Blank rather than zero: the sparseness is what makes a real failure count stand out.
		cells["failed"] = blankZero(t.Failed, failStyle)
		cells["dlq"] = blankZero(t.DLQ, dlqStyle)
	}
	if r, ok := m.topicRate(t.Topic); ok {
		cells["rate"] = dimStyle.Render(humanRate(r))
	}
	if t.Bytes > 0 {
		cells["bytes"] = dimStyle.Render(humanBytes(t.Bytes))
	}
	return cells
}

// groupCells is one consumer group's numeric cells, dim so the tree reads as subordinate
// to its topic row, with empty cells where a number would only repeat that row above.
func groupCells(t engine.TopicStat, g engine.GroupStat, broker, failures bool) map[string]string {
	cells := map[string]string{
		"consumed": dimStyle.Render(comma(g.Consumed)),
	}
	if broker {
		// The group row is where broker lag actually belongs -- lag is per group, and the
		// topic figure above is only their sum. "unknown" is deliberate: a dash or blank
		// cell would read as zero at a glance, which nil BrokerLag must never imply.
		if lag, known := t.BrokerLag[g.GroupID]; known {
			cells["broker"] = dimStyle.Render(comma(lag))
		} else {
			cells["broker"] = dimStyle.Render("unknown")
		}
	}
	if failures {
		cells["failed"] = blankZero(g.Failed, dimStyle)
		cells["dlq"] = blankZero(g.DLQ, dimStyle)
	}
	return cells
}

// groupLabel builds the indented tree label, never wider than nameW. When space runs out
// the ×N consumer count drops first, then the group id truncates hard, leaving the paused
// warning as the last thing standing since that matters more than any part of the name.
func groupLabel(g engine.GroupStat, last bool, nameW int) string {
	branch := "  ├─ "
	if last {
		branch = "  └─ "
	}
	suffix := ""
	if g.Consumers > 1 {
		suffix = fmt.Sprintf(" ×%d", g.Consumers)
	}
	paused := ""
	if g.Paused > 0 {
		paused = fmt.Sprintf(" (%d paused)", g.Paused)
	}
	idMax := nameW - lipgloss.Width(branch) - lipgloss.Width(suffix) - lipgloss.Width(paused)
	if idMax < 5 {
		suffix = ""
		idMax = nameW - lipgloss.Width(branch) - lipgloss.Width(paused)
	}
	if idMax < 3 {
		paused = ""
		idMax = nameW - lipgloss.Width(branch)
	}
	out := dimStyle.Render(branch + truncate(g.GroupID, max(idMax, 1)) + suffix)
	if paused != "" {
		out += warnStyle.Render(paused)
	}
	return truncate(out, nameW)
}

func blankZero(n int64, sty lipgloss.Style) string {
	if n == 0 {
		return ""
	}
	return sty.Render(comma(n))
}

// topicBrokerLag sums a topic's measured lag across consumer groups, the same convention
// the self-reported Lag column uses, keeping the two comparable at a glance. The bool is
// false when the topic was not measured at all; a group missing from a measured topic is
// simply absent from the sum, making the total a lower bound.
func topicBrokerLag(t engine.TopicStat) (int64, bool) {
	if t.BrokerLag == nil {
		return 0, false
	}
	var total int64
	for _, lag := range t.BrokerLag {
		total += lag
	}
	return total, true
}

// flowTable renders at most budget flows into a table w wide, noting what it cut --
// scenarios aggregate, so the flow list is as unbounded as the topic list.
func (m model) flowTable(w, budget int) string {
	pad := padFor(w)
	specs := []colSpec{
		{"started", "STARTED"}, {"completed", "COMPLETED"}, {"msgs", "MSGS"},
		{"inflight", "INFLIGHT"}, {"errors", "ERRORS"},
	}
	flows := m.snap.Flows
	hidden := 0
	if len(flows) > budget {
		hidden = len(flows) - max(budget-1, 1) // the note takes one of the budgeted rows
		flows = flows[:max(budget-1, 1)]
	}
	cells := make([]map[string]string, 0, len(flows))
	for _, f := range flows {
		cells = append(cells, map[string]string{
			"started":   comma(f.Started),
			"completed": prodStyle.Render(comma(f.Completed)),
			"msgs":      consStyle.Render(comma(f.Messages)),
			"inflight":  comma(f.InFlight),
			"errors":    blankZero(f.Errors, alertStyle),
		})
	}
	nameW := max(firstColWidth(w, colWidths(specs, cells), pad), 8)

	headers := make([]string, 0, len(specs)+1)
	headers = append(headers, padHeader("FLOW", nameW))
	for _, sp := range specs {
		headers = append(headers, sp.title)
	}
	rows := make([][]string, 0, len(cells)+1)
	for i, f := range flows {
		sat := ""
		if f.Saturated > 0 {
			sat = fmt.Sprintf("  saturated x%d", f.Saturated)
		}
		name := topicStyle.Render(truncate(f.Name, max(nameW-lipgloss.Width(sat), 5)))
		if sat != "" {
			name = truncate(name+warnStyle.Render(sat), nameW)
		}
		row := []string{name}
		for _, sp := range specs {
			row = append(row, cells[i][sp.key])
		}
		rows = append(rows, row)
	}
	if hidden > 0 {
		note := make([]string, len(headers))
		note[0] = dimStyle.Render(truncate("… "+plural(int64(hidden), "more flow"), nameW))
		rows = append(rows, note)
	}
	return styledTable(headers, rows, w, pad)
}

// totalsBar is the bold last line: totals, rates, lag, traffic volume, errors. It degrades
// in steps as the terminal narrows, shedding derived extras first while totals and lag
// never drop, and clips rather than wraps as a last resort.
func (m model) totalsBar(w int, flowsShown bool) string {
	s := m.snap
	pr, cr, br, haveRates := m.totalRates()

	var brokerSeg string
	if m.showBrokerLag() {
		// Only the measured topics contribute. A run where one topic's group is denied
		// DESCRIBE still gets a total, and it is a total of what was actually measured
		// rather than a figure with a silent zero folded into it.
		var total int64
		measured := false
		for _, t := range s.Topics {
			if sum, known := topicBrokerLag(t); known {
				total += sum
				measured = true
			}
		}
		brokerSeg = dimStyle.Render("broker ") + brokerLagCell(total, measured)
	}
	var totBytes int64
	for _, t := range s.Topics {
		totBytes += t.Bytes
	}

	// Each level sheds the least essential remaining piece: bytes, then the sparkline,
	// then the rates, then the words themselves.
	build := func(level int) string {
		segs := []string{headerStyle.Render("TOTAL")}
		if level < 4 {
			prod := prodBold.Render(comma(s.TotalProduced)) + dimStyle.Render(" produced")
			cons := consBold.Render(comma(s.TotalConsumed)) + dimStyle.Render(" consumed")
			if haveRates && level < 3 {
				prod += dimStyle.Render(" · ") + prodStyle.Render(humanRate(pr))
				cons += dimStyle.Render(" · ") + consStyle.Render(humanRate(cr))
			}
			segs = append(segs, prod, cons)
		} else {
			segs = append(segs,
				prodBold.Render("↑ "+comma(s.TotalProduced)),
				consBold.Render("↓ "+comma(s.TotalConsumed)))
		}
		segs = append(segs, dimStyle.Render("lag ")+lagCell(s.TotalProduced-s.TotalConsumed))
		if brokerSeg != "" {
			segs = append(segs, brokerSeg)
		}
		if totBytes > 0 && level < 1 {
			bs := dimStyle.Render(humanBytes(totBytes))
			if haveRates {
				bs += dimStyle.Render(" · " + humanByteRate(br))
			}
			segs = append(segs, bs)
		}
		if level < 2 {
			if sv := m.sparkVals(8); sv != nil {
				segs = append(segs, prodStyle.Render(sparkline(sv)))
			}
		}
		if s.TotalErrors > 0 {
			segs = append(segs, alertStyle.Render(plural(s.TotalErrors, "error")))
		}
		if s.Rebalances > 0 {
			segs = append(segs, dimStyle.Render(plural(s.Rebalances, "rebalance")))
		}
		// A flow table squeezed out by a short terminal is disclosed rather than lost.
		if len(s.Flows) > 0 && !flowsShown {
			segs = append(segs, dimStyle.Render(plural(int64(len(s.Flows)), "flow")+" not shown"))
		}
		sep := "   "
		if level >= 4 {
			sep = "  "
		}
		return " " + strings.Join(segs, sep)
	}
	for level := 0; level <= 4; level++ {
		if line := build(level); lipgloss.Width(line) <= w {
			return line
		}
	}
	return truncate(build(4), w)
}

// issuesLine surfaces produce/consume/commit errors and duplicates as one aggregated line,
// present only when something is non-zero, so a clean run pays no space for it.
func (m model) issuesLine(w int) string {
	var pe, ce, cme, dup int64
	for _, t := range m.snap.Topics {
		pe += t.ProduceErr
		ce += t.ConsumeErr
		cme += t.CommitErr
		dup += t.Duplicates
	}
	if pe == 0 && ce == 0 && cme == 0 && dup == 0 {
		return ""
	}
	var parts []string
	add := func(n int64, noun string) {
		if n > 0 {
			parts = append(parts, warnStyle.Render(comma(n))+dimStyle.Render(" "+pluralNoun(n, noun)))
		}
	}
	add(pe, "produce error")
	add(ce, "consume error")
	add(cme, "commit error")
	add(dup, "duplicate")
	return truncate(" "+strings.Join(parts, "   "), w)
}

func plural(n int64, noun string) string {
	return comma(n) + " " + pluralNoun(n, noun)
}

func pluralNoun(n int64, noun string) string {
	if n == 1 {
		return noun
	}
	return noun + "s"
}

// lagAlertThreshold is the count above which a lag cell turns red, shared by both lag
// columns so they can be read against each other without holding two thresholds in your head.
const lagAlertThreshold = 100

// lagCell colours lag red above the threshold, otherwise green.
func lagCell(lag int64) string {
	s := comma(lag)
	if lag > lagAlertThreshold {
		return alertStyle.Render(s)
	}
	return okStyle.Render(s)
}

// brokerLagCell is a real-lag value with the shared threshold colouring, or a dim
// "unknown" when the value was never obtained -- see groupCells for why that word.
func brokerLagCell(lag int64, known bool) string {
	switch {
	case !known:
		return dimStyle.Render("unknown")
	case lag > lagAlertThreshold:
		return alertStyle.Render(comma(lag))
	}
	return okStyle.Render(comma(lag))
}

// recentEvents renders the newest n events, one per line, cell-truncated to width w
// (w <= 0 leaves lines unbounded, for tests that assert on full content). One line per
// event is a hard promise the height budget depends on; embedded newlines (the lag poller
// formats multi-cause errors this way, see internal/engine/lag.go) collapse to a separator
// instead of becoming extra lines.
func recentEvents(events []engine.Event, n, w int) string {
	if len(events) == 0 || n <= 0 {
		return ""
	}
	if len(events) > n {
		events = events[len(events)-n:]
	}
	var b strings.Builder
	title := " " + headerStyle.Render("EVENTS")
	if w >= 12 {
		title += " " + borderStyle.Render(strings.Repeat("─", w-lipgloss.Width(title)-1))
	}
	b.WriteString(title)
	b.WriteString("\n")
	for _, e := range events {
		style := dimStyle
		switch e.Level {
		case scenario.EventWarn:
			style = warnStyle
		case scenario.EventAlert:
			style = alertStyle
		case scenario.EventRecovery:
			style = okStyle
		}
		msg := strings.ReplaceAll(strings.ReplaceAll(e.Message, "\r", ""), "\n", " · ")
		line := " " + dimStyle.Render(e.At.Format("15:04:05")) + "  " + style.Render(msg)
		if w > 0 {
			line = truncate(line, w)
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}
