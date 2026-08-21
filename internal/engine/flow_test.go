package engine

import (
	"context"
	"testing"
	"time"

	"github.com/aleksandarskrbic/khaos/internal/kafka"
	"github.com/aleksandarskrbic/khaos/internal/scenario"
	"github.com/twmb/franz-go/pkg/kfake"
)

// newStrictFakeCluster starts a cluster that will NOT auto-create topics.
//
// Three brokers because flow step topics are created with replication_factor 3.
// Auto-creation is off so that a test asserting flow messages landed is really asserting
// khaos created the topics itself.
func newStrictFakeCluster(t *testing.T) []string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(3))
	if err != nil {
		t.Fatalf("start fake cluster: %v", err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

func twoStepFlow(name string, secondStepDelayMS int) scenario.Flow {
	return scenario.Flow{
		Name:        name,
		Rate:        20,
		Correlation: scenario.Correlation{Type: scenario.CorrelationUUID},
		Steps: []scenario.FlowStep{
			{Topic: name + "-created", EventType: "created"},
			{Topic: name + "-shipped", EventType: "shipped", DelayMS: secondStepDelayMS},
		},
	}
}

// TestFlowRunCreatesItsTopicsAndCompletes pins that a flow creates its own step topics and
// completes end to end on a cluster with auto-creation disabled.
func TestFlowRunCreatesItsTopicsAndCompletes(t *testing.T) {
	addrs := newStrictFakeCluster(t)

	sc := &scenario.Scenario{
		Name:  "flows",
		Flows: []scenario.Flow{twoStepFlow("orderflow", 50)},
	}

	eng, err := New(context.Background(), Config{
		Kafka:     kafka.Config{BootstrapServers: addrs},
		Scenarios: []*scenario.Scenario{sc},
		Duration:  2 * time.Second,
		Seed:      5,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	snap := eng.Snapshot()
	if len(snap.Flows) != 1 {
		t.Fatalf("expected one flow in the snapshot, got %+v", snap.Flows)
	}
	fs := snap.Flows[0]
	if fs.Name != "orderflow" {
		t.Errorf("flow name = %q, want %q", fs.Name, "orderflow")
	}
	if fs.Started == 0 {
		t.Fatal("no flow instance was issued")
	}
	if fs.Completed == 0 {
		t.Fatalf("no flow instance completed: started=%d messages=%d errors=%d",
			fs.Started, fs.Messages, fs.Errors)
	}
	if fs.Messages < fs.Completed*2 {
		t.Errorf("messages=%d for %d completed two-step instances", fs.Messages, fs.Completed)
	}
	if fs.Errors != 0 {
		t.Errorf("flow reported %d errors", fs.Errors)
	}
	// Run joins in-flight instances rather than sleeping and hoping, so nothing may be
	// left in flight once Run has returned.
	if fs.InFlight != 0 {
		t.Errorf("%d flow instances still in flight after Run returned", fs.InFlight)
	}
}

// TestDrainBudget pins the post-cancellation ceiling.
func TestDrainBudget(t *testing.T) {
	tests := []struct {
		name  string
		steps []scenario.FlowStep
		want  time.Duration
	}{
		{
			name:  "no delays is slack only",
			steps: []scenario.FlowStep{{Topic: "a"}, {Topic: "b"}},
			want:  flowDrainSlack,
		},
		{
			name: "sums every step, not the max",
			steps: []scenario.FlowStep{
				{Topic: "a", DelayMS: 100},
				{Topic: "b", DelayMS: 400},
				{Topic: "c", DelayMS: 500},
			},
			want: flowDrainSlack + time.Second,
		},
		{
			name:  "negative delays are ignored",
			steps: []scenario.FlowStep{{Topic: "a", DelayMS: -5000}},
			want:  flowDrainSlack,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := drainBudget(scenario.Flow{Steps: tc.steps}); got != tc.want {
				t.Errorf("drainBudget = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestFlowDrainDoesNotOutlastItsBudget proves a wedged flow instance cannot hold the run
// open.
//
// Instances deliberately run on a context detached from the run deadline so a
// mid-sequence flow finishes rather than being truncated. Detached without a ceiling is a
// hang: the join happens inside Run, before teardown exists, so the teardown deadline
// cannot rescue it. A flow whose second step waits 30s would have pinned Run open for 30s
// after Ctrl-C.
func TestFlowDrainDoesNotOutlastItsBudget(t *testing.T) {
	addrs := newStrictFakeCluster(t)

	sc := &scenario.Scenario{
		Name:  "slowflow",
		Flows: []scenario.Flow{twoStepFlow("slowflow", 30_000)},
	}

	eng, err := New(context.Background(), Config{
		Kafka:     kafka.Config{BootstrapServers: addrs},
		Scenarios: []*scenario.Scenario{sc},
		Seed:      3,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if len(eng.flows) != 1 {
		t.Fatalf("expected one flow runner, got %d", len(eng.flows))
	}
	if got, want := eng.flows[0].drain, flowDrainSlack+30*time.Second; got != want {
		t.Fatalf("drain budget = %s, want %s", got, want)
	}
	// Shrink the ceiling so the test does not have to wait out the real one. Safe to do
	// here: Run has not started, so nothing else can see this field yet.
	eng.flows[0].drain = 300 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()

	time.Sleep(400 * time.Millisecond)
	cancel()

	start := time.Now()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return: an in-flight flow instance outlasted its drain budget")
	}

	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("Run took %s to return after cancellation", elapsed)
	}
	if got := eng.Snapshot().Flows[0].InFlight; got != 0 {
		t.Errorf("%d flow instances still in flight after Run returned", got)
	}
}
