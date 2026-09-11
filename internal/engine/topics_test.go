package engine

import (
	"slices"
	"testing"
)

// TestTopicTableAddIsIdempotent pins the property buildStepConsumers depends on: a flow
// step naming an already-declared topic shares that topic's row and counters rather than
// opening a second one.
func TestTopicTableAddIsIdempotent(t *testing.T) {
	tb := newTopicTableBuilder()

	first := tb.add("orders", "declared")
	second := tb.add("orders", "a-flow")

	if first != second {
		t.Error("add returned different counters for the same topic")
	}

	table := tb.freeze()
	if got := table.rows(); !slices.Equal(got, []string{"orders"}) {
		t.Errorf("rows = %v, want one entry", got)
	}
	// The first mention owns the label: a topic declared under `topics:` and then touched
	// by a flow step belongs to the scenario that declared it.
	if got := table.metaFor("orders").scenarioName; got != "declared" {
		t.Errorf("scenarioName = %q, want the first mention %q", got, "declared")
	}
	if table.counters("orders") != first {
		t.Error("frozen table does not carry the counters add handed out")
	}
}

// TestTopicTableOrderIsFirstMention pins that rows render in the order topics were added,
// not in map order.
func TestTopicTableOrderIsFirstMention(t *testing.T) {
	tb := newTopicTableBuilder()
	for _, name := range []string{"charlie", "alpha", "bravo"} {
		tb.add(name, "s")
	}
	tb.add("alpha", "s") // a repeat must not move it

	want := []string{"charlie", "alpha", "bravo"}
	if got := tb.freeze().rows(); !slices.Equal(got, want) {
		t.Errorf("rows = %v, want %v", got, want)
	}
}

// TestTopicTableGroupsAreOrderedAndDeduplicated pins what the lag poller relies on: the
// group order decides the order of admin calls and therefore of any reported failures, so
// it must not vary from run to run.
func TestTopicTableGroupsAreOrderedAndDeduplicated(t *testing.T) {
	tb := newTopicTableBuilder()
	tb.add("orders", "s")
	tb.addGroup("orders", "orders-group-1")
	tb.addGroup("orders", "orders-group-2")
	tb.add("shipments", "s")
	tb.addGroup("shipments", "shared-group")
	// The same group id against two topics is one group to the broker.
	tb.addGroup("orders", "shared-group")

	want := []string{"orders-group-1", "orders-group-2", "shared-group"}
	if got := tb.freeze().groups(); !slices.Equal(got, want) {
		t.Errorf("groups = %v, want %v", got, want)
	}
}

// TestNilTopicTableIsReadable pins that every reader is safe on an Engine that has not
// finished building, so no caller needs a nil check of its own.
func TestNilTopicTableIsReadable(t *testing.T) {
	var table *topicTable

	if got := table.rows(); got != nil {
		t.Errorf("rows = %v, want nil", got)
	}
	if got := table.counters("orders"); got != nil {
		t.Errorf("counters = %v, want nil", got)
	}
	if got := table.metaFor("orders"); got.scenarioName != "" || got.groups != nil {
		t.Errorf("metaFor = %+v, want the zero value", got)
	}
	if got := table.groups(); got != nil {
		t.Errorf("groups = %v, want nil", got)
	}
}

// TestFreezeEmptiesTheBuilder pins that a frozen table cannot be mutated through the
// builder that produced it, which is the whole reason the table can be read unlocked
// while the run is going.
func TestFreezeEmptiesTheBuilder(t *testing.T) {
	tb := newTopicTableBuilder()
	tb.add("orders", "s")
	table := tb.freeze()

	defer func() {
		if recover() == nil {
			t.Error("add on a frozen builder silently succeeded, want a panic")
		}
		if got := table.rows(); !slices.Equal(got, []string{"orders"}) {
			t.Errorf("published table changed after freeze: rows = %v", got)
		}
	}()
	tb.add("late", "s")
}
