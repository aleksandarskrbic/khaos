package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
	"charm.land/lipgloss/v2/table"
	"github.com/aleksandarskrbic/khaos/internal/engine"
)

// summary prints the end-of-run report: the same table shape the live TUI showed --
// topics down the side, a bold TOTAL row under each column -- so the numbers land in the
// scrollback aligned and colour-coded instead of as a bare key-value list.
func summary(w io.Writer, s engine.Snapshot, hadTUI bool) {
	if hadTUI {
		fmt.Fprintln(w)
	}
	fmt.Fprintf(w, "%s %s%s\n",
		styTitle.Render("khaos"),
		styHeader.Render(s.Scenario),
		styDim.Render(" · elapsed "+s.Elapsed.Round(time.Second).String()))

	// The TOTAL row is a TopicStat too, so one row builder serves both.
	total := engine.TopicStat{Topic: "TOTAL", Produced: s.TotalProduced, Consumed: s.TotalConsumed}
	total.Lag = total.Produced - total.Consumed
	failures := false
	for _, t := range s.Topics {
		total.Failed += t.Failed
		total.DLQ += t.DLQ
		if t.Failed > 0 || t.DLQ > 0 {
			failures = true
		}
	}
	headers := []string{"TOPIC", "PRODUCED", "CONSUMED", "LAG"}
	if failures {
		headers = append(headers, "FAILED", "DLQ")
	}
	rows := make([][]string, 0, len(s.Topics)+1)
	for _, t := range s.Topics {
		rows = append(rows, summaryRow(t, styScenario, failures))
	}
	rows = append(rows, summaryRow(total, styHeader, failures))
	fmt.Fprintln(w, summaryTable(headers, rows))

	if len(s.Flows) > 0 {
		frows := make([][]string, 0, len(s.Flows))
		for _, f := range s.Flows {
			frows = append(frows, []string{
				styScenario.Render(f.Name),
				thousands(f.Started),
				styOK.Render(thousands(f.Completed)),
				styWarn.Render(thousands(f.Messages)),
			})
		}
		fmt.Fprintln(w, summaryTable([]string{"FLOW", "STARTED", "COMPLETED", "MSGS"}, frows))
	}

	var notes []string
	if s.TotalErrors > 0 {
		notes = append(notes, styErr.Render(fmt.Sprintf("%d %s", s.TotalErrors, pluralWord(s.TotalErrors, "error"))))
	}
	if s.Rebalances > 0 {
		notes = append(notes, styDim.Render(fmt.Sprintf("%d %s", s.Rebalances, pluralWord(s.Rebalances, "rebalance"))))
	}
	if len(notes) > 0 {
		fmt.Fprintln(w, " "+strings.Join(notes, "   "))
	}
}

// summaryRow renders one line of the topic table; the FAILED and DLQ cells are only
// present when the table has those columns.
func summaryRow(t engine.TopicStat, nameStyle lipgloss.Style, failures bool) []string {
	row := []string{
		nameStyle.Render(t.Topic),
		styOK.Render(thousands(t.Produced)),
		styWarn.Render(thousands(t.Consumed)),
		summaryLag(t.Lag),
	}
	if failures {
		row = append(row, summaryBlankZero(t.Failed), summaryBlankZero(t.DLQ))
	}
	return row
}

// summaryTable is the shared khaos table look, with numeric columns right-aligned.
func summaryTable(headers []string, rows [][]string) string {
	return table.New().
		Border(lipgloss.RoundedBorder()).
		BorderStyle(styBorder).
		BorderRow(false).
		BorderColumn(false).
		Wrap(false).
		StyleFunc(func(row, col int) lipgloss.Style {
			st := lipgloss.NewStyle().PaddingLeft(1).PaddingRight(2)
			if col > 0 {
				st = st.AlignHorizontal(lipgloss.Right)
			}
			if row == table.HeaderRow {
				return st.Inherit(styHeader)
			}
			return st
		}).
		Headers(headers...).
		Rows(rows...).
		String()
}

// summaryLag keeps the live TUI's threshold: red past 100, green otherwise.
func summaryLag(lag int64) string {
	if lag > 100 {
		return styErr.Render(thousands(lag))
	}
	return styOK.Render(thousands(lag))
}

// summaryBlankZero keeps the blank-instead-of-zero convention for failure cells.
func summaryBlankZero(n int64) string {
	if n == 0 {
		return ""
	}
	return thousands(n)
}

// thousands renders n with separators: 59,962.
func thousands(n int64) string {
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

func pluralWord(n int64, noun string) string {
	if n == 1 {
		return noun
	}
	return noun + "s"
}
