package main

import (
	"os"

	"charm.land/lipgloss/v2"
	"charm.land/lipgloss/v2/table"
	"github.com/aleksandarskrbic/khaos/internal/theme"
	"golang.org/x/term"
)

// Presentation styles for command output: bordered tables, a title, cyan scenario names,
// bold-magenta category headings, built with lipgloss and colours from internal/theme --
// the same palette the live TUI uses, so every khaos screen reads as one product.
//
// Colour is suppressed when stdout is not a terminal, so piping into a file or another
// process yields clean text with no escape sequences.
var (
	colorEnabled = detectColor()

	styTitle = style(lipgloss.NewStyle().Bold(true).Foreground(theme.Magenta))
	// Bold in the terminal's own foreground, not ANSI white: bright-white-on-white is
	// invisible on a light background.
	styHeader   = style(lipgloss.NewStyle().Bold(true))
	styCategory = style(lipgloss.NewStyle().Bold(true).Foreground(theme.Magenta))
	styScenario = style(lipgloss.NewStyle().Foreground(theme.Cyan))
	styDim      = style(lipgloss.NewStyle().Faint(true))
	styOK       = style(lipgloss.NewStyle().Bold(true).Foreground(theme.Green))
	styWarn     = style(lipgloss.NewStyle().Foreground(theme.Yellow))
	styErr      = style(lipgloss.NewStyle().Bold(true).Foreground(theme.Red))
	styBorder   = style(lipgloss.NewStyle().Foreground(theme.Gray))
)

// detectColor reports whether stdout can carry ANSI styling.
//
// NO_COLOR is honoured because it is the de facto standard and costs one line.
func detectColor() bool {
	if os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
		return false
	}
	return term.IsTerminal(int(os.Stdout.Fd()))
}

// style strips the styling from s when colour is off, so callers never branch.
func style(s lipgloss.Style) lipgloss.Style {
	if !colorEnabled {
		return lipgloss.NewStyle()
	}
	return s
}

// newListTable is the shared bordered two-column table look for `khaos list` and
// `khaos help`.
func newListTable(headers ...string) *table.Table {
	return table.New().
		Border(lipgloss.RoundedBorder()).
		BorderStyle(styBorder).
		BorderRow(false).
		BorderColumn(false).
		Width(tableWidth()).
		Wrap(true).
		StyleFunc(func(row, col int) lipgloss.Style {
			pad := lipgloss.NewStyle().PaddingLeft(1).PaddingRight(2)
			if row == table.HeaderRow {
				return pad.Inherit(styHeader)
			}
			return pad
		}).
		Headers(headers...)
}
