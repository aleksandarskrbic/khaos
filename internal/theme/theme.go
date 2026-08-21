// Package theme is the one palette every khaos surface draws from: cyan for names, green
// for produced, yellow for consumed, magenta for failures, blue for DLQ. Colors are ANSI
// palette indices rather than hex values, so the terminal's own theme decides what "green"
// looks like, keeping output legible on both light and dark backgrounds with no
// adaptive-colour machinery.
//
// This package holds colours only. Emphasis (bold, faint) is chosen where a colour is
// used: the static commands in cmd/khaos strip styling themselves when stdout is not a
// terminal, and the live TUI leaves degradation to Bubble Tea's renderer.
package theme

import (
	"image/color"

	"charm.land/lipgloss/v2"
)

var (
	Red     color.Color = lipgloss.Color("9")
	Green   color.Color = lipgloss.Color("10")
	Yellow  color.Color = lipgloss.Color("11")
	Blue    color.Color = lipgloss.Color("12")
	Magenta color.Color = lipgloss.Color("13")
	Cyan    color.Color = lipgloss.Color("14")
	Gray    color.Color = lipgloss.Color("8")
)
