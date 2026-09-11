package engine

// topicTable is the engine's published view of its topics: which topics have a row, in
// what order, and the counters and metadata each row is rendered from.
//
// Its job is to make one invariant structural instead of remembered. Snapshot and the lag
// poller both read this while every producer and consumer in the run is going, and
// neither takes a lock. That is sound only because the table stops changing before the
// run starts. It used to be three maps hanging off Engine, safe only for as long as every
// future edit remembered not to write them mid-run -- an invariant asserted by comments in
// three separate files and enforced by nothing.
//
// A *topicTable has no setter and no exported mutation of any kind: the builder is
// consumed by freeze, so after build there is no way to write one, by accident or
// otherwise. Every method is safe on a nil receiver, which is what an Engine that has not
// finished building has.
//
// The mutable state is the atomic pointer inside each *counters. Those are shared with the
// producers and consumers that write them, and they synchronise themselves.
type topicTable struct {
	order []string
	stats map[string]*counters
	meta  map[string]topicMeta
}

// rows returns the topics that have a row, in render order.
func (t *topicTable) rows() []string {
	if t == nil {
		return nil
	}
	return t.order
}

// counters returns a topic's counter set, or nil when the topic has no row. A nil answer
// means the topic was never registered during build.
func (t *topicTable) counters(topic string) *counters {
	if t == nil {
		return nil
	}
	return t.stats[topic]
}

// metaFor returns a topic's metadata, or the zero value when the topic has no row.
func (t *topicTable) metaFor(topic string) topicMeta {
	if t == nil {
		return topicMeta{}
	}
	return t.meta[topic]
}

// groups returns every consumer group in the table, deduplicated, in table order.
//
// Ordered rather than ranged out of a map because the result decides the order of the lag
// poller's admin calls and therefore of any failures it reports; map iteration order would
// make that vary from run to run.
func (t *topicTable) groups() []string {
	seen := make(map[string]bool)
	var out []string
	for _, name := range t.rows() {
		for _, g := range t.metaFor(name).groups {
			if seen[g] {
				continue
			}
			seen[g] = true
			out = append(out, g)
		}
	}
	return out
}

// topicTableBuilder accumulates topics during build.
//
// Not safe for concurrent use, and deliberately not made so: build runs before any
// goroutine in the run exists, and anything that needed to add a topic later would be
// reintroducing exactly the race topicTable exists to rule out.
type topicTableBuilder struct {
	table *topicTable
}

func newTopicTableBuilder() *topicTableBuilder {
	return &topicTableBuilder{table: &topicTable{
		stats: make(map[string]*counters),
		meta:  make(map[string]topicMeta),
	}}
}

// add registers a topic and returns its counters, creating them on first mention.
//
// Idempotent by design: a flow step whose `consumers:` block names an already-declared
// topic must share that topic's existing row and counters, not open a second one. The
// first scenario to mention a topic owns its label, for the same reason.
func (b *topicTableBuilder) add(topic, scenarioName string) *counters {
	stats, ok := b.table.stats[topic]
	if !ok {
		stats = &counters{}
		b.table.stats[topic] = stats
		b.table.order = append(b.table.order, topic)
	}
	if m := b.table.meta[topic]; m.scenarioName == "" {
		m.scenarioName = scenarioName
		b.table.meta[topic] = m
	}
	return stats
}

// addGroup records a consumer group against a topic, in registration order.
func (b *topicTableBuilder) addGroup(topic, groupID string) {
	m := b.table.meta[topic]
	m.groups = append(m.groups, groupID)
	b.table.meta[topic] = m
}

// freeze publishes the table and empties the builder.
//
// The builder is unusable afterwards -- a later add panics on a nil map rather than
// mutating a table something is already reading.
func (b *topicTableBuilder) freeze() *topicTable {
	t := b.table
	b.table = nil
	return t
}
