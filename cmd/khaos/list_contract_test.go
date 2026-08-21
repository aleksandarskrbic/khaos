package main

import (
	"context"
	"strings"
	"testing"
)

// `list` writes through the cobra command's writers, so runCLI's buffer captures it and no
// test has to swap the process-wide os.Stdout. The helper that used to do that swap was a
// data race waiting to happen: os.Stdout is read by every other goroutine in the process,
// including the engine's teardown diagnostics, so a single parallel test would have tripped
// the race detector.
func listOutput(t *testing.T) string {
	t.Helper()
	out, err := runCLI(t, context.Background(), "list")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	return out
}

// The homebrew-core formula's test block asserts `khaos list` contains "Available
// Scenarios" (Formula/k/khaos.rb). Dropping that string silently fails the formula test
// on every future release, so it is a contract, not decoration. The formula's second
// assertion, on "traffic/high-throughput", is pinned here too.
func TestListOutputContract(t *testing.T) {
	t.Parallel()
	out := listOutput(t)

	for _, want := range []string{
		"Available Scenarios", // asserted by the homebrew formula
		"Traffic Patterns",    // category headings
		"Chaos Engineering",
		"Event Flows",
		"Serialization",
		"traffic/high-throughput",     // asserted by the homebrew formula
		"Usage: khaos run <scenario>", // trailing usage hint
	} {
		if !strings.Contains(out, want) {
			t.Errorf("list output missing %q\n---\n%s", want, out)
		}
	}
}

// Categories appear in categoryOrder's declared order, not alphabetically.
func TestListCategoryOrder(t *testing.T) {
	t.Parallel()
	out := listOutput(t)

	order := []string{"Traffic Patterns", "Chaos Engineering", "Event Flows", "Serialization"}
	prev := -1
	for _, heading := range order {
		at := strings.Index(out, heading)
		if at < 0 {
			t.Fatalf("missing category %q", heading)
		}
		if at < prev {
			t.Errorf("category %q appears out of order", heading)
		}
		prev = at
	}
}

// A category khaos has no blurb for -- `testing/` is one today -- must still be listed,
// title-cased, after the four known ones.
func TestListIncludesUnknownCategoriesLast(t *testing.T) {
	t.Parallel()
	out := listOutput(t)

	// Rows sit inside a lipgloss border, so a heading is no longer at column 0; match the
	// heading text itself rather than assuming it follows a newline.
	known := strings.Index(out, "Serialization")
	unknown := strings.Index(out, "Testing")
	if unknown < 0 {
		t.Fatalf("unknown category `testing` missing from list output\n---\n%s", out)
	}
	if unknown < known {
		t.Error("an unlisted category sorted before a known one")
	}
}
