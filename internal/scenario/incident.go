package scenario

import (
	"fmt"
	"math/rand/v2"
	"strconv"
)

// Incident types accepted in `incidents:`.
//
// This is a closed set; an unrecognised `type:` is a hard error.
const (
	IncidentStopBroker           = "stop_broker"
	IncidentStartBroker          = "start_broker"
	IncidentPauseConsumer        = "pause_consumer"
	IncidentRebalanceConsumer    = "rebalance_consumer"
	IncidentIncreaseConsumerDlay = "increase_consumer_delay"
	IncidentChangeProducerRate   = "change_producer_rate"
)

// Schedule controls when an incident fires.
//
// AtSeconds fires once at that offset from run start; EverySeconds fires repeatedly.
// Both are optional and both are parsed from the same flat level as the incident
// itself, not from a nested `schedule:` block.
type Schedule struct {
	AtSeconds           *int `yaml:"at_seconds"`
	EverySeconds        *int `yaml:"every_seconds"`
	InitialDelaySeconds int  `yaml:"initial_delay_seconds"`
}

// ConsumerTarget selects which consumers an incident acts on.
//
// Filters compose: topic, then group, then indices, then a random subset by count or
// percentage. An empty target matches everything.
type ConsumerTarget struct {
	Topic      string `yaml:"topic"`
	Group      string `yaml:"group"`
	Percentage *int   `yaml:"percentage"`
	Count      *int   `yaml:"count"`

	// Indices selects candidates by their position in ctx.Consumers -- registration
	// order across the whole run, not position within an already topic/group-filtered
	// subset. It composes with Topic/Group as another AND'd predicate. Mutually
	// exclusive with Percentage/Count at the YAML level (validate.go enforces this).
	Indices []int `yaml:"indices"`
}

// ProducerTarget selects which producers an incident acts on. There is no Group here:
// producers have no group concept.
type ProducerTarget struct {
	Topic      string `yaml:"topic"`
	Percentage *int   `yaml:"percentage"`
	Count      *int   `yaml:"count"`
	Indices    []int  `yaml:"indices"` // see ConsumerTarget.Indices
}

// ID is a stable opaque handle for one producer or consumer.
//
// A recreated consumer always gets a new ID; the old one is simply gone from the
// registry. This makes it impossible for a targeted incident scheduled after a
// rebalance to silently select a closed consumer.
type ID string

// ConsumerRef is the engine-independent view of a consumer that incidents need.
//
// Keeping this a plain value rather than a pointer to a live consumer is what lets the
// whole incident layer be unit-tested without a running Kafka cluster.
type ConsumerRef struct {
	ID      ID
	Topic   string
	GroupID string
	Topics  []string

	// Conf is the consumer's configuration. It is deliberately NOT carried into the
	// CreateConsumer command: the replacement is rebuilt from only group_id and
	// processing_delay_ms, so a consumer recreated by rebalance_consumer loses
	// failure_rate, commit_failure_rate, on_failure and max_retries and reverts to
	// model defaults.
	Conf              ConsumerConf
	ProcessingDelayMS int
}

// ProducerRef is the engine-independent view of a producer.
type ProducerRef struct {
	ID    ID
	Topic string
}

// Context is the state an incident reads when expanding into commands.
//
// Rand is explicit rather than a package-level generator so that target selection is
// reproducible in tests.
type Context struct {
	Consumers      []ConsumerRef
	Producers      []ProducerRef
	RebalanceCount int
	Rand           *rand.Rand
}

// Command is one primitive action produced by an incident.
//
// Incidents are pure functions from context to an ordered command list, so they
// unit-test without a broker. EmitEvent is a value rather than a direct write, so the
// output layer decides how to render it and the engine stays independent of any UI.
type Command interface{ isCommand() }

// EmitEvent is an incident-emitted event for the output layer to render.
type EmitEvent struct {
	Message string
	Level   EventLevel
}

// EventLevel is the severity of an emitted event. The output layer maps these to
// colours; the engine itself does not know about colours.
type EventLevel int

const (
	EventInfo EventLevel = iota
	EventWarn
	EventAlert
	EventRecovery
)

// SetConsumerDelay sets a consumer's simulated processing delay.
type SetConsumerDelay struct {
	ID      ID
	DelayMS int
}

// SetProducerRate sets a producer's target message rate.
type SetProducerRate struct {
	ID   ID
	Rate float64
}

// IncrementRebalanceCount bumps the scenario's rebalance counter for GroupID.
type IncrementRebalanceCount struct{ GroupID string }

// StopConsumers pauses consumers without tearing them down. Pause is its own
// primitive, distinct from shutdown, and resuming unblocks the existing goroutine
// rather than spawning a new one.
type StopConsumers struct{ IDs []ID }

// ResumeConsumers unpauses consumers paused by StopConsumers.
type ResumeConsumers struct{ IDs []ID }

// StopConsumer closes one consumer for good, as the first half of a rebalance.
type StopConsumer struct{ ID ID }

// CreateConsumer registers a replacement consumer after a StopConsumer, as a new
// registration with a new ID rather than an overwrite of an existing slot.
type CreateConsumer struct {
	GroupID           string
	Topics            []string
	ProcessingDelayMS int
	Conf              ConsumerConf
}

// StopBroker / StartBroker act on the local Docker cluster. Against an external
// cluster they are filtered out entirely.
type StopBroker struct{ Broker string }

// StartBroker restarts a broker stopped by StopBroker.
type StartBroker struct{ Broker string }

// Delay pauses the command sequence.
//
// Delays live inside command sequences, which is why the scheduler runs one goroutine
// per incident rather than one goroutine applying every mutation: a single executor
// would stall every other due incident for the length of this delay and change
// scenario timing.
type Delay struct{ Seconds float64 }

func (EmitEvent) isCommand()               {}
func (SetConsumerDelay) isCommand()        {}
func (SetProducerRate) isCommand()         {}
func (IncrementRebalanceCount) isCommand() {}
func (StopConsumers) isCommand()           {}
func (ResumeConsumers) isCommand()         {}
func (StopConsumer) isCommand()            {}
func (CreateConsumer) isCommand()          {}
func (StopBroker) isCommand()              {}
func (StartBroker) isCommand()             {}
func (Delay) isCommand()                   {}

// Incident is one scheduled fault, implemented by six concrete types below.
type Incident interface {
	Type() string
	Sched() Schedule
	Commands(ctx *Context) []Command
}

// IncidentGroup repeats a set of incidents on an interval. Each member incident still
// carries its own schedule, interpreted relative to each repetition.
type IncidentGroup struct {
	Repeat          int
	IntervalSeconds int
	Incidents       []Incident
}

// ---------------------------------------------------------------------------
// Target selection
// ---------------------------------------------------------------------------

// selectConsumers applies target filters in order: topic, then group, then indices,
// then a random subset by count or percentage. With no narrowing at all, every consumer
// matches.
func selectConsumers(ctx *Context, t ConsumerTarget) []ConsumerRef {
	indices := indexSet(t.Indices)
	candidates := make([]ConsumerRef, 0, len(ctx.Consumers))
	for i, c := range ctx.Consumers {
		if t.Topic != "" && c.Topic != t.Topic {
			continue
		}
		if t.Group != "" && c.GroupID != t.Group {
			continue
		}
		if indices != nil && !indices[i] {
			continue
		}
		candidates = append(candidates, c)
	}
	return narrow(ctx, candidates, t.Count, t.Percentage)
}

// selectProducers applies the same topic, indices and count/percentage narrowing as
// selectConsumers, minus the group filter producers don't have.
func selectProducers(ctx *Context, t ProducerTarget) []ProducerRef {
	indices := indexSet(t.Indices)
	candidates := make([]ProducerRef, 0, len(ctx.Producers))
	for i, p := range ctx.Producers {
		if t.Topic != "" && p.Topic != t.Topic {
			continue
		}
		if indices != nil && !indices[i] {
			continue
		}
		candidates = append(candidates, p)
	}
	return narrow(ctx, candidates, t.Count, t.Percentage)
}

// indexSet turns Indices into a membership set keyed by position in ctx.Consumers /
// ctx.Producers (registration order), or nil when Indices is empty so the caller can
// skip the filter entirely rather than matching nothing.
func indexSet(indices []int) map[int]bool {
	if len(indices) == 0 {
		return nil
	}
	set := make(map[int]bool, len(indices))
	for _, i := range indices {
		set[i] = true
	}
	return set
}

// narrow applies the count/percentage subset rules.
//
// The percentage formula is max(1, len*pct/100) using integer division, with a floor
// of one so a small percentage against a small pool still hits something.
func narrow[T any](ctx *Context, candidates []T, count, percentage *int) []T {
	switch {
	case count != nil:
		return sample(ctx, candidates, *count)
	case percentage != nil:
		n := len(candidates) * *percentage / 100
		if n < 1 {
			n = 1
		}
		return sample(ctx, candidates, n)
	default:
		return candidates
	}
}

// sample draws n distinct elements without replacement.
func sample[T any](ctx *Context, candidates []T, n int) []T {
	if n >= len(candidates) {
		return candidates
	}
	if n <= 0 {
		return nil
	}
	shuffled := make([]T, len(candidates))
	copy(shuffled, candidates)
	ctx.Rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	return shuffled[:n]
}

func consumerIDs(cs []ConsumerRef) []ID {
	out := make([]ID, len(cs))
	for i, c := range cs {
		out[i] = c.ID
	}
	return out
}

// ---------------------------------------------------------------------------
// Incident implementations
// ---------------------------------------------------------------------------

// StopBrokerIncident stops a broker container.
type StopBrokerIncident struct {
	Broker   string
	Schedule Schedule
}

func (i StopBrokerIncident) Type() string    { return IncidentStopBroker }
func (i StopBrokerIncident) Sched() Schedule { return i.Schedule }

func (i StopBrokerIncident) Commands(_ *Context) []Command {
	return []Command{
		EmitEvent{Message: fmt.Sprintf(">>> INCIDENT: Stopping %s", i.Broker), Level: EventAlert},
		EmitEvent{Message: "ISR will shrink, leadership will change", Level: EventWarn},
		StopBroker{Broker: i.Broker},
	}
}

// StartBrokerIncident restarts a broker stopped by StopBrokerIncident.
type StartBrokerIncident struct {
	Broker   string
	Schedule Schedule
}

func (i StartBrokerIncident) Type() string    { return IncidentStartBroker }
func (i StartBrokerIncident) Sched() Schedule { return i.Schedule }

func (i StartBrokerIncident) Commands(_ *Context) []Command {
	return []Command{
		EmitEvent{Message: fmt.Sprintf(">>> RECOVERY: Starting %s", i.Broker), Level: EventRecovery},
		EmitEvent{Message: "ISR will expand back", Level: EventWarn},
		StartBroker{Broker: i.Broker},
	}
}

// PauseConsumers pauses matching consumers for a fixed duration, then resumes them.
type PauseConsumers struct {
	DurationSeconds int
	Target          ConsumerTarget
	Schedule        Schedule
}

func (i PauseConsumers) Type() string    { return IncidentPauseConsumer }
func (i PauseConsumers) Sched() Schedule { return i.Schedule }

func (i PauseConsumers) Commands(ctx *Context) []Command {
	selected := selectConsumers(ctx, i.Target)
	if len(selected) == 0 {
		return []Command{noMatch("consumers")}
	}
	ids := consumerIDs(selected)
	return []Command{
		EmitEvent{
			Message: fmt.Sprintf(">>> INCIDENT: Pausing %d consumers for %ds", len(ids), i.DurationSeconds),
			Level:   EventAlert,
		},
		StopConsumers{IDs: ids},
		Delay{Seconds: float64(i.DurationSeconds)},
		EmitEvent{Message: ">>> Consumers resuming", Level: EventRecovery},
		ResumeConsumers{IDs: ids},
	}
}

// RebalanceConsumer closes one randomly chosen matching consumer, waits 3s, then
// registers a replacement in the same group. The 3s delay is hardcoded.
type RebalanceConsumer struct {
	Target   ConsumerTarget
	Schedule Schedule
}

func (i RebalanceConsumer) Type() string    { return IncidentRebalanceConsumer }
func (i RebalanceConsumer) Sched() Schedule { return i.Schedule }

func (i RebalanceConsumer) Commands(ctx *Context) []Command {
	selected := selectConsumers(ctx, i.Target)
	if len(selected) == 0 {
		return []Command{noMatch("consumers")}
	}
	victim := selected[ctx.Rand.IntN(len(selected))]
	return []Command{
		EmitEvent{
			Message: fmt.Sprintf(">>> REBALANCE #%d: Closing consumer %s", ctx.RebalanceCount+1, victim.ID),
			Level:   EventAlert,
		},
		IncrementRebalanceCount{GroupID: victim.GroupID},
		StopConsumer{ID: victim.ID},
		Delay{Seconds: 3},
		CreateConsumer{
			GroupID:           victim.GroupID,
			Topics:            victim.Topics,
			ProcessingDelayMS: victim.ProcessingDelayMS,
			// Conf deliberately omitted -- see ConsumerRef.Conf. The replacement gets
			// model defaults.
		},
		EmitEvent{Message: fmt.Sprintf(">>> Consumer %s rejoined group", victim.ID), Level: EventWarn},
	}
}

// IncreaseConsumerDelay sets a new processing delay on matching consumers.
type IncreaseConsumerDelay struct {
	DelayMS  int
	Target   ConsumerTarget
	Schedule Schedule
}

func (i IncreaseConsumerDelay) Type() string    { return IncidentIncreaseConsumerDlay }
func (i IncreaseConsumerDelay) Sched() Schedule { return i.Schedule }

func (i IncreaseConsumerDelay) Commands(ctx *Context) []Command {
	selected := selectConsumers(ctx, i.Target)
	if len(selected) == 0 {
		return []Command{noMatch("consumers")}
	}
	cmds := make([]Command, 0, len(selected)+1)
	cmds = append(cmds, EmitEvent{
		Message: fmt.Sprintf(">>> INCIDENT: Setting delay to %dms for %d consumers", i.DelayMS, len(selected)),
		Level:   EventAlert,
	})
	for _, c := range selected {
		cmds = append(cmds, SetConsumerDelay{ID: c.ID, DelayMS: i.DelayMS})
	}
	return cmds
}

// ChangeProducerRate retunes matching producers to a new target rate, live.
type ChangeProducerRate struct {
	Rate     float64
	Target   ProducerTarget
	Schedule Schedule
}

func (i ChangeProducerRate) Type() string    { return IncidentChangeProducerRate }
func (i ChangeProducerRate) Sched() Schedule { return i.Schedule }

func (i ChangeProducerRate) Commands(ctx *Context) []Command {
	selected := selectProducers(ctx, i.Target)
	if len(selected) == 0 {
		return []Command{noMatch("producers")}
	}
	cmds := make([]Command, 0, len(selected)+1)
	cmds = append(cmds, EmitEvent{
		Message: fmt.Sprintf(">>> INCIDENT: Changing rate to %s msg/s for %d producers",
			rateText(i.Rate), len(selected)),
		Level: EventWarn,
	})
	for _, p := range selected {
		cmds = append(cmds, SetProducerRate{ID: p.ID, Rate: i.Rate})
	}
	return cmds
}

// rateText renders a producer rate for the incident event as fixed-notation shortest
// form, e.g. "1000000" rather than the "1e+06" that %g would produce past six
// significant digits. A rate written as a whole number and one written as "200.0" both
// render as "200"; telling them apart would mean carrying the raw YAML token through
// decode for the sake of one log line.
func rateText(rate float64) string {
	return strconv.FormatFloat(rate, 'f', -1, 64)
}

// noMatch is the shared "target selected nothing" event.
func noMatch(kind string) Command {
	return EmitEvent{Message: fmt.Sprintf(">>> No %s matched target", kind), Level: EventWarn}
}
