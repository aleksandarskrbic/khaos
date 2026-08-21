package scenario

import (
	"math/rand/v2"
	"reflect"
	"testing"
)

// These tests assert exact command lists, which is what makes the incident layer fully
// testable without a broker.

func testCtx(consumers []ConsumerRef, producers []ProducerRef, rebalances int) *Context {
	return &Context{
		Consumers:      consumers,
		Producers:      producers,
		RebalanceCount: rebalances,
		// Fixed seed: target selection uses randomness, and tests must be reproducible.
		Rand: rand.New(rand.NewPCG(1, 2)),
	}
}

func consumer(id, topic, group string) ConsumerRef {
	return ConsumerRef{
		ID:      ID(id),
		Topic:   topic,
		GroupID: group,
		Topics:  []string{topic},
		Conf:    defaultConsumerConf(),
	}
}

func producer(id, topic string) ProducerRef {
	return ProducerRef{ID: ID(id), Topic: topic}
}

func TestBrokerIncidentCommands(t *testing.T) {
	tests := []struct {
		name     string
		incident Incident
		want     []Command
	}{
		{
			name:     "stop_broker",
			incident: StopBrokerIncident{Broker: "kafka-1"},
			want: []Command{
				EmitEvent{Message: ">>> INCIDENT: Stopping kafka-1", Level: EventAlert},
				EmitEvent{Message: "ISR will shrink, leadership will change", Level: EventWarn},
				StopBroker{Broker: "kafka-1"},
			},
		},
		{
			name:     "start_broker",
			incident: StartBrokerIncident{Broker: "kafka-2"},
			want: []Command{
				EmitEvent{Message: ">>> RECOVERY: Starting kafka-2", Level: EventRecovery},
				EmitEvent{Message: "ISR will expand back", Level: EventWarn},
				StartBroker{Broker: "kafka-2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.incident.Commands(testCtx(nil, nil, 0))
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("commands mismatch\n got: %#v\nwant: %#v", got, tt.want)
			}
		})
	}
}

func TestPauseConsumersCommands(t *testing.T) {
	consumers := []ConsumerRef{
		consumer("c1", "orders", "g1"),
		consumer("c2", "orders", "g1"),
	}
	inc := PauseConsumers{DurationSeconds: 10}

	got := inc.Commands(testCtx(consumers, nil, 0))

	want := []Command{
		EmitEvent{Message: ">>> INCIDENT: Pausing 2 consumers for 10s", Level: EventAlert},
		StopConsumers{IDs: []ID{"c1", "c2"}},
		Delay{Seconds: 10},
		EmitEvent{Message: ">>> Consumers resuming", Level: EventRecovery},
		ResumeConsumers{IDs: []ID{"c1", "c2"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("commands mismatch\n got: %#v\nwant: %#v", got, want)
	}
}

// TestPauseConsumersEmbedsDelayInSequence pins that the delay is part of the command
// sequence, not a property of the schedule.
func TestPauseConsumersEmbedsDelayInSequence(t *testing.T) {
	inc := PauseConsumers{DurationSeconds: 30}
	got := inc.Commands(testCtx([]ConsumerRef{consumer("c1", "orders", "g1")}, nil, 0))

	var found bool
	for _, c := range got {
		if d, ok := c.(Delay); ok {
			found = true
			if d.Seconds != 30 {
				t.Errorf("Delay = %v, want 30", d.Seconds)
			}
		}
	}
	if !found {
		t.Fatal("expected a Delay command inside the pause sequence")
	}
}

func TestRebalanceConsumerCommands(t *testing.T) {
	c := consumer("c1", "orders", "payments-group-1")
	c.ProcessingDelayMS = 25
	got := RebalanceConsumer{}.Commands(testCtx([]ConsumerRef{c}, nil, 4))

	want := []Command{
		EmitEvent{Message: ">>> REBALANCE #5: Closing consumer c1", Level: EventAlert},
		IncrementRebalanceCount{},
		StopConsumer{ID: "c1"},
		Delay{Seconds: 3},
		CreateConsumer{
			GroupID:           "payments-group-1",
			Topics:            []string{"orders"},
			ProcessingDelayMS: 25,
			// No Conf: the replacement is rebuilt from only group_id and
			// processing_delay_ms.
		},
		EmitEvent{Message: ">>> Consumer c1 rejoined group", Level: EventWarn},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("commands mismatch\n got: %#v\nwant: %#v", got, want)
	}
}

// Pins that a replacement consumer is rebuilt from only group_id and
// processing_delay_ms, discarding failure-simulation config and reverting to model
// defaults.
func TestRebalanceConsumerDropsFailureConfig(t *testing.T) {
	c := consumer("c1", "orders", "g1")
	c.Conf = ConsumerConf{
		FailureRate:       0.25,
		CommitFailureRate: 0.1,
		OnFailure:         "dlq",
		MaxRetries:        7,
	}

	got := RebalanceConsumer{}.Commands(testCtx([]ConsumerRef{c}, nil, 0))

	for _, cmd := range got {
		if cc, ok := cmd.(CreateConsumer); ok {
			if !reflect.DeepEqual(cc.Conf, ConsumerConf{}) {
				t.Errorf("replacement carried failure config %#v; recreation must drop it", cc.Conf)
			}
			return
		}
	}
	t.Fatal("no CreateConsumer command produced")
}

func TestIncreaseConsumerDelayCommands(t *testing.T) {
	consumers := []ConsumerRef{
		consumer("c1", "orders", "g1"),
		consumer("c2", "orders", "g1"),
	}
	got := IncreaseConsumerDelay{DelayMS: 500}.Commands(testCtx(consumers, nil, 0))

	want := []Command{
		EmitEvent{Message: ">>> INCIDENT: Setting delay to 500ms for 2 consumers", Level: EventAlert},
		SetConsumerDelay{ID: "c1", DelayMS: 500},
		SetConsumerDelay{ID: "c2", DelayMS: 500},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("commands mismatch\n got: %#v\nwant: %#v", got, want)
	}
}

func TestChangeProducerRateCommands(t *testing.T) {
	producers := []ProducerRef{producer("p1", "orders"), producer("p2", "orders")}
	got := ChangeProducerRate{Rate: 200}.Commands(testCtx(nil, producers, 0))

	want := []Command{
		EmitEvent{Message: ">>> INCIDENT: Changing rate to 200 msg/s for 2 producers", Level: EventWarn},
		SetProducerRate{ID: "p1", Rate: 200},
		SetProducerRate{ID: "p2", Rate: 200},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("commands mismatch\n got: %#v\nwant: %#v", got, want)
	}
}

func TestNoMatchProducesWarningOnly(t *testing.T) {
	tests := []struct {
		name     string
		incident Incident
		want     Command
	}{
		{"pause", PauseConsumers{DurationSeconds: 5}, EmitEvent{Message: ">>> No consumers matched target", Level: EventWarn}},
		{"rebalance", RebalanceConsumer{}, EmitEvent{Message: ">>> No consumers matched target", Level: EventWarn}},
		{"delay", IncreaseConsumerDelay{DelayMS: 10}, EmitEvent{Message: ">>> No consumers matched target", Level: EventWarn}},
		{"rate", ChangeProducerRate{Rate: 10}, EmitEvent{Message: ">>> No producers matched target", Level: EventWarn}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.incident.Commands(testCtx(nil, nil, 0))
			if len(got) != 1 || !reflect.DeepEqual(got[0], tt.want) {
				t.Errorf("got %#v, want exactly [%#v]", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Target selection.
// ---------------------------------------------------------------------------

func TestSelectConsumers(t *testing.T) {
	all := []ConsumerRef{
		consumer("c0", "orders", "g1"),
		consumer("c1", "orders", "g2"),
		consumer("c2", "payments", "g1"),
		consumer("c3", "payments", "g2"),
	}

	pct := func(n int) *int { return &n }

	tests := []struct {
		name   string
		target ConsumerTarget
		want   []ID
	}{
		{"empty target matches all", ConsumerTarget{}, []ID{"c0", "c1", "c2", "c3"}},
		{"by topic", ConsumerTarget{Topic: "orders"}, []ID{"c0", "c1"}},
		{"by group", ConsumerTarget{Group: "g1"}, []ID{"c0", "c2"}},
		{"topic and group compose", ConsumerTarget{Topic: "payments", Group: "g2"}, []ID{"c3"}},
		{"no match", ConsumerTarget{Topic: "nonexistent"}, []ID{}},
		// indices is accepted but IGNORED: the incident widens to every candidate
		// instead of the ones named.
		{"indices ignored", ConsumerTarget{Indices: []int{0, 2}}, []ID{"c0", "c1", "c2", "c3"}},
		{"indices ignored alongside topic", ConsumerTarget{Topic: "payments", Indices: []int{2}}, []ID{"c2", "c3"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := consumerIDs(selectConsumers(testCtx(all, nil, 0), tt.target))
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}

	t.Run("count caps the selection", func(t *testing.T) {
		got := selectConsumers(testCtx(all, nil, 0), ConsumerTarget{Count: pct(2)})
		if len(got) != 2 {
			t.Errorf("got %d consumers, want 2", len(got))
		}
	})

	t.Run("count larger than pool returns whole pool", func(t *testing.T) {
		got := selectConsumers(testCtx(all, nil, 0), ConsumerTarget{Count: pct(99)})
		if len(got) != 4 {
			t.Errorf("got %d consumers, want 4", len(got))
		}
	})

	// n = max(1, len(candidates) * percentage / 100): integer division with a floor
	// of one, so 10% of 4 selects 1 rather than 0.
	t.Run("percentage floors at one", func(t *testing.T) {
		got := selectConsumers(testCtx(all, nil, 0), ConsumerTarget{Percentage: pct(10)})
		if len(got) != 1 {
			t.Errorf("got %d consumers, want 1", len(got))
		}
	})

	t.Run("percentage uses integer division", func(t *testing.T) {
		got := selectConsumers(testCtx(all, nil, 0), ConsumerTarget{Percentage: pct(50)})
		if len(got) != 2 {
			t.Errorf("got %d consumers, want 2", len(got))
		}
	})
}

func TestSelectProducers(t *testing.T) {
	all := []ProducerRef{
		producer("p0", "orders"),
		producer("p1", "orders"),
		producer("p2", "payments"),
	}

	got := selectProducers(testCtx(nil, all, 0), ProducerTarget{Topic: "orders"})
	if len(got) != 2 || got[0].ID != "p0" || got[1].ID != "p1" {
		t.Errorf("got %v, want [p0 p1]", got)
	}

	n := 1
	one := selectProducers(testCtx(nil, all, 0), ProducerTarget{Count: &n})
	if len(one) != 1 {
		t.Errorf("got %d producers, want 1", len(one))
	}
}
