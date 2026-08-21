package engine

import (
	"context"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/aleksandarskrbic/khaos/internal/kafka"
	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

func intPtr(v int) *int { return &v }

// consumerIDSet is the set of ids currently registered.
func consumerIDSet(e *Engine) map[scenario.ID]bool {
	out := make(map[scenario.ID]bool)
	for _, c := range e.reg.allConsumers() {
		out[c.ID] = true
	}
	return out
}

// TestIncidentsMutateLiveComponents drives real incidents against a live run, asserting
// four things that can only be observed in sequence:
//
//  1. a pause_consumer parks a live consumer and Snapshot reports it;
//  2. a rebalance_consumer closes one and registers a replacement under a new id;
//  3. Snapshot's rebalance count and group membership follow;
//  4. an incident scheduled AFTER the rebalance lands on the REPLACEMENT, never on the
//     consumer that was closed.
//
// Snapshot is polled from this goroutine throughout, so the race detector also sees every
// read of engine state overlap the run.
func TestIncidentsMutateLiveComponents(t *testing.T) {
	addrs := newFakeCluster(t)

	tp := jsonTopic("incident-topic")
	tp.Partitions = 2
	tp.ConsumersPerGroup = 2

	const finalDelayMS = 250

	sc := &scenario.Scenario{
		Name:   "incidents",
		Topics: []scenario.Topic{tp},
		Incidents: []scenario.Incident{
			// Parks every consumer on the topic for two seconds from t=1.
			scenario.PauseConsumers{
				DurationSeconds: 2,
				Target:          scenario.ConsumerTarget{Topic: tp.Name},
				Schedule:        scenario.Schedule{AtSeconds: intPtr(1)},
			},
			// Closes one consumer at t=1 and registers its replacement at t=4: the 3s
			// gap is hardcoded in the incident.
			scenario.RebalanceConsumer{
				Target:   scenario.ConsumerTarget{Topic: tp.Name},
				Schedule: scenario.Schedule{AtSeconds: intPtr(1)},
			},
			// Ticks every second from t=6. Recurring rather than a single at_seconds so
			// the assertion does not hinge on the rebalance finishing by an exact
			// instant: closing a consumer leaves the group, which is a network round
			// trip whose duration varies. It starts at t=6 -- after the rebalance
			// EXPANDED at t=1 -- so the replacement is created carrying the topic's
			// delay of 0 and can only reach finalDelayMS by being targeted after it
			// joined.
			scenario.IncreaseConsumerDelay{
				DelayMS: finalDelayMS,
				Target:  scenario.ConsumerTarget{Topic: tp.Name},
				Schedule: scenario.Schedule{
					InitialDelaySeconds: 5,
					EverySeconds:        intPtr(1),
				},
			},
		},
	}

	eng, err := New(context.Background(), Config{
		Kafka:     kafka.Config{BootstrapServers: addrs},
		Scenarios: []*scenario.Scenario{sc},
		Duration:  10 * time.Second,
		Seed:      11,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	before := consumerIDSet(eng)
	if len(before) != 2 {
		t.Fatalf("expected 2 consumers before the run, got %d", len(before))
	}

	done := make(chan error, 1)
	go func() { done <- eng.Run(context.Background()) }()

	// Read immediately rather than waiting for the first tick: Run stamps the run start
	// on entry, so this overlaps that write. It used to be a plain time.Time field and
	// this read raced it.
	_ = eng.Snapshot()

	var sawPaused bool
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
loop:
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			break loop
		case <-poll.C:
			snap := eng.Snapshot()
			for _, ts := range snap.Topics {
				for _, gs := range ts.Groups {
					if gs.Paused > 0 {
						sawPaused = true
					}
				}
			}
		case <-time.After(30 * time.Second):
			t.Fatal("run did not finish")
		}
	}

	if !sawPaused {
		t.Error("no snapshot ever reported a paused consumer")
	}

	snap := eng.Snapshot()

	// The assertions below are all about what happened when; without the log a failure is
	// undiagnosable after the fact.
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		for _, ev := range snap.Events {
			t.Logf("event %s %s", ev.At.Format("15:04:05.000"), ev.Message)
		}
	})

	if snap.Rebalances != 1 {
		t.Errorf("Rebalances = %d, want 1", snap.Rebalances)
	}

	after := consumerIDSet(eng)
	if len(after) != 2 {
		t.Fatalf("expected 2 live consumers after the rebalance, got %d", len(after))
	}

	var victims, replacements int
	for id := range before {
		if !after[id] {
			victims++
		}
	}
	for id := range after {
		if !before[id] {
			replacements++
		}
	}
	if victims != 1 || replacements != 1 {
		t.Fatalf("expected exactly one consumer replaced, got %d closed and %d created",
			victims, replacements)
	}

	// The delay incident fired at t=5, after the replacement joined at t=4. If it had
	// targeted the closed consumer the replacement would still be at the topic's
	// configured delay.
	for _, c := range eng.reg.allConsumers() {
		if got := c.processingDelayMS(); got != finalDelayMS {
			t.Errorf("consumer %s delay = %dms, want %dms: the post-rebalance incident missed it",
				c.ID, got, finalDelayMS)
		}
	}

	if len(snap.Topics) != 1 {
		t.Fatalf("expected one topic in the snapshot, got %d", len(snap.Topics))
	}
	if len(snap.Topics[0].Groups) != 1 || snap.Topics[0].Groups[0].Consumers != 2 {
		t.Errorf("snapshot group membership did not follow the rebalance: %+v", snap.Topics[0].Groups)
	}

	var sawRebalanceEvent bool
	for _, ev := range snap.Events {
		if strings.Contains(ev.Message, "REBALANCE") {
			sawRebalanceEvent = true
		}
	}
	if !sawRebalanceEvent {
		t.Error("no rebalance event was recorded")
	}
}

// TestSchedulerConcurrentIncidentsShareOneRNG pins the fix for a data race.
//
// scheduler.Run gives every incident its own goroutine and every one of them expands
// against the same *rand.Rand (scenario.Context.Rand, drawn by sample and
// RebalanceConsumer at internal/scenario/incident.go:289,386). math/rand/v2 is not safe
// for concurrent use, so before the rndMu fix this test failed under -race -- and in
// production it silently biased which components a chaos run hit.
//
// No broker: the commands under test only touch the pause gate and the delay field, which
// is exactly the point of keeping incidents pure over refs.
func TestSchedulerConcurrentIncidentsShareOneRNG(t *testing.T) {
	events := newEventRing(eventCapacity)
	reg := newRegistry()
	for i := 0; i < 8; i++ {
		id := reg.mintID("consumer")
		reg.addConsumer(newConsumer(consumerOpts{
			id:      id,
			topic:   "t",
			groupID: "g",
			topics:  []string{"t"},
			topicC:  &counters{},
			events:  events,
		}))
	}

	s := &scheduler{
		reg:     reg,
		events:  events,
		brokers: noopBrokers{events: events},
		rnd:     rand.New(rand.NewPCG(1, 2)),
	}

	// Neither at_seconds nor every_seconds: every incident fires once, immediately, so
	// all 64 goroutines expand at the same instant. Count forces the random subset path.
	var incidents []scenario.Incident
	for i := 0; i < 32; i++ {
		incidents = append(incidents,
			scenario.PauseConsumers{
				DurationSeconds: 0,
				Target:          scenario.ConsumerTarget{Count: intPtr(3)},
			},
			scenario.IncreaseConsumerDelay{
				DelayMS: 5,
				Target:  scenario.ConsumerTarget{Percentage: intPtr(50)},
			},
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.Run(ctx, incidents, nil); err != nil {
		t.Fatalf("scheduler.Run: %v", err)
	}

	// Every pause is followed by a resume in the same command sequence, so nothing may be
	// left parked.
	for _, c := range reg.allConsumers() {
		if c.Paused() {
			t.Errorf("consumer %s left paused after every incident resumed", c.ID)
		}
	}
}
