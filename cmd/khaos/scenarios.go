package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"charm.land/lipgloss/v2"
	"charm.land/lipgloss/v2/table"
	"github.com/aleksandarskrbic/khaos/internal/scenario"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// loadScenario resolves a bundled name or a path and loads it, reporting every diagnostic.
//
// Resolve must always feed Load: bundled scenarios resolve to an embed:// URI that
// os.ReadFile cannot open, and only Load understands them.
func loadScenario(w io.Writer, nameOrPath string) (*scenario.Scenario, error) {
	path, err := scenario.Resolve(nameOrPath)
	if err != nil {
		return nil, err
	}

	sc, diags, err := scenario.Load(path)
	if err != nil {
		return nil, err
	}
	printDiagnostics(w, nameOrPath, diags)
	if diags.HasErrors() {
		return nil, errSilent
	}
	return sc, nil
}

// printDiagnostics writes findings to w, which is stderr in every caller.
//
// Warnings are printed and the run continues; only errors mark the result invalid.
// Diagnostics go to stderr so that piping stdout stays clean.
func printDiagnostics(w io.Writer, name string, diags scenario.Diagnostics) {
	for _, d := range diags {
		kind := styWarn.Render("warning")
		if d.Severity == scenario.SeverityError {
			kind = styErr.Render("error")
		}
		loc := name
		if d.Line > 0 {
			loc = fmt.Sprintf("%s:%d", name, d.Line)
		}
		fmt.Fprintf(w, "%s: %s: %s: %s\n",
			styDim.Render(loc), kind, styScenario.Render(d.Path), d.Message)
	}
}

// categoryInfo is the display name and blurb for each scenario directory.
//
// The ORDER matters: categories sort by their position in categoryOrder and fall back to
// a title-cased directory name for anything unlisted.
var categoryOrder = []string{"traffic", "chaos", "flows", "serialization"}

var categoryInfo = map[string][2]string{
	"traffic":       {"Traffic Patterns", "Basic traffic generation scenarios"},
	"chaos":         {"Chaos Engineering", "Fault injection and incident scenarios"},
	"flows":         {"Event Flows", "Correlated multi-topic event flows"},
	"serialization": {"Serialization", "Avro and Protobuf format examples"},
}

func newListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List bundled scenarios",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(c *cobra.Command, _ []string) error {
			found, err := scenario.Discover()
			if err != nil {
				return err
			}
			if len(found) == 0 {
				fmt.Fprintln(c.ErrOrStderr(), styWarn.Render("No scenarios found."))
				return nil
			}

			// Group by leading directory.
			byCategory := make(map[string][]string)
			for name := range found {
				category, _, ok := strings.Cut(name, "/")
				if !ok {
					category = ""
				}
				byCategory[category] = append(byCategory[category], name)
			}

			categories := make([]string, 0, len(byCategory))
			for cat := range byCategory {
				categories = append(categories, cat)
			}
			sort.Slice(categories, func(i, j int) bool {
				return categoryRank(categories[i]) < categoryRank(categories[j])
			})

			t := table.New().
				Border(lipgloss.RoundedBorder()).
				BorderStyle(styBorder).
				BorderRow(false).
				BorderColumn(false).
				Width(tableWidth()).
				Wrap(true).
				// Cells carry no padding of their own, so without this the scenario name
				// and its description butt straight up against each other.
				StyleFunc(func(row, col int) lipgloss.Style {
					pad := lipgloss.NewStyle().PaddingLeft(1).PaddingRight(2)
					if row == table.HeaderRow {
						return pad.Inherit(styHeader)
					}
					return pad
				}).
				Headers("SCENARIO", "DESCRIPTION")

			for i, cat := range categories {
				if i > 0 {
					t.Row("", "") // blank spacer between category groups
				}
				display, blurb := categoryDisplay(cat)
				t.Row(styCategory.Render(display), styDim.Render(blurb))

				names := byCategory[cat]
				sort.Strings(names)
				for _, name := range names {
					desc := ""
					// A scenario that fails to parse is still listed, by name, rather than
					// skipped silently: a broken file should be visible, not invisible.
					if sc, diags, err := scenario.Load(found[name]); err == nil && !diags.HasErrors() {
						desc = sc.Description
					}
					t.Row("  "+styScenario.Render(name), desc)
				}
			}

			// The title is a compatibility surface: the homebrew-core formula's test block
			// asserts the output contains "Available Scenarios".
			out := c.OutOrStdout()
			fmt.Fprintln(out, styTitle.Render("Available Scenarios"))
			fmt.Fprintln(out, t)
			fmt.Fprintln(out, styDim.Render("Usage: khaos run <scenario>"))
			fmt.Fprintln(out, styDim.Render("Example: khaos run traffic/high-throughput"))
			return nil
		},
	}
}

// tableWidth caps output at the terminal width so a long description wraps instead of
// blowing the table past the edge of the window. 100 is the fallback when stdout is not a
// terminal, which keeps piped output stable rather than dependent on the environment.
func tableWidth() int {
	if !colorEnabled {
		return 100
	}
	w, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || w < 60 {
		return 100
	}
	if w > 120 {
		w = 120
	}
	return w
}

// categoryRank orders known categories first, in categoryOrder's declared order.
func categoryRank(cat string) int {
	for i, c := range categoryOrder {
		if c == cat {
			return i
		}
	}
	return 99
}

// categoryDisplay returns the heading and blurb, title-casing unknown directories.
func categoryDisplay(cat string) (string, string) {
	if info, ok := categoryInfo[cat]; ok {
		return info[0], info[1]
	}
	if cat == "" {
		return "Other", ""
	}
	return strings.ToUpper(cat[:1]) + cat[1:], ""
}

func newValidateCmd() *cobra.Command {
	var strict bool
	cmd := &cobra.Command{
		Use:   "validate [scenario...]",
		Short: "Validate scenario files",
		// With no arguments every bundled scenario is checked.
		Long: "Validate scenarios without running them.\n\n" +
			"A scenario may be a bundled name (e.g. traffic/high-throughput) or a path to a\n" +
			"YAML file. With no arguments every bundled scenario is checked.\n\n" +
			"Reports every problem at once rather than stopping at the first, with line\n" +
			"numbers. Exits non-zero if there are errors; warnings alone exit zero.",
		Args: cobra.ArbitraryArgs,
		RunE: func(c *cobra.Command, args []string) error {
			stdout, stderr := c.OutOrStdout(), c.ErrOrStderr()

			names := args
			if len(names) == 0 {
				// `khaos validate` with no arguments validates the whole bundled corpus,
				// which is what makes it usable as a CI gate over every shipped scenario.
				found, err := scenario.Discover()
				if err != nil {
					return err
				}
				names = make([]string, 0, len(found))
				for name := range found {
					names = append(names, name)
				}
				sort.Strings(names)
			}

			failed := 0
			for _, name := range names {
				ok, err := validateOne(stdout, stderr, name, strict)
				if err != nil {
					return err
				}
				if !ok {
					failed++
				}
			}

			if failed > 0 {
				fmt.Fprintf(stderr, "%d of %d scenario(s) invalid\n", failed, len(names))
				return errSilent
			}
			// Printed even for a single scenario, for consistent script output.
			fmt.Fprintf(stdout, "all %d scenario(s) valid\n", len(names))
			return nil
		},
	}
	cmd.Flags().BoolVar(&strict, "strict", false,
		"also reject keys no khaos version has ever read")
	return cmd
}

// validateOne checks a single scenario and reports it, returning whether it was clean.
// A resolution failure -- an unknown name or an unreadable path -- is returned as an
// error, because it is not a verdict about the file's contents.
func validateOne(stdout, stderr io.Writer, name string, strict bool) (bool, error) {
	// Accepts a path as well as a bundled name, matching `khaos run`.
	path, err := scenario.Resolve(name)
	if err != nil {
		return false, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		// Bundled scenarios resolve to embed:// URIs, which only Load can open. Load does
		// not honour --strict, so a bundled name is checked in the default mode; the
		// bundled corpus has no unknown keys by construction.
		_, diags, lerr := scenario.Load(path)
		if lerr != nil {
			return false, lerr
		}
		return report(stdout, stderr, name, diags), nil
	}

	var diags scenario.Diagnostics
	if strict {
		_, diags = scenario.DecodeStrict(data)
	} else {
		_, diags = scenario.Decode(data)
	}
	return report(stdout, stderr, name, diags), nil
}

// report prints one scenario's diagnostics and returns whether it is valid.
func report(stdout, stderr io.Writer, name string, diags scenario.Diagnostics) bool {
	printDiagnostics(stderr, name, diags)
	if diags.HasErrors() {
		fmt.Fprintf(stderr, "%s %s: %d error(s), %d warning(s)\n",
			styErr.Render("✗"), name, len(diags.Errors()), len(diags.Warnings()))
		return false
	}
	// "valid" is kept in the text: it is the string a CI job is most likely to grep for.
	if w := len(diags.Warnings()); w > 0 {
		fmt.Fprintf(stdout, "%s %s: valid (%d warning(s))\n", styWarn.Render("!"), name, w)
		return true
	}
	fmt.Fprintf(stdout, "%s %s: valid\n", styOK.Render("✓"), name)
	return true
}
