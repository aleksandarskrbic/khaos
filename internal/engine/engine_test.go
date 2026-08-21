package engine

import (
	"bytes"
	"context"
	"math/rand/v2"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aleksandarskrbic/khaos/internal/generate"

	"github.com/aleksandarskrbic/khaos/internal/kafka"
	"github.com/aleksandarskrbic/khaos/internal/scenario"
	"github.com/twmb/franz-go/pkg/kfake"
	"go.uber.org/goleak"
)

// TestMain enforces the no-goroutine-leak rule for the whole package: every `go func`
// must live inside an errgroup or a tracked pool.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// kfake and kgo keep internal workers alive briefly past Close.
		goleak.IgnoreTopFunction("github.com/twmb/franz-go/pkg/kfake.(*Cluster).run"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kgo.(*broker).handleReqs"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kgo.(*cursorOffsetNext).processRespPartition"),
	)
}

// newFakeCluster starts an in-process Kafka speaking the real protocol: millisecond
// startup, no Docker, real wire protocol, making the engine's whole pipeline testable in
// CI.
func newFakeCluster(t *testing.T) []string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.AllowAutoTopicCreation())
	if err != nil {
		t.Fatalf("start fake cluster: %v", err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

// jsonTopic is a minimal topic with no field schema, exercising the synthetic-payload path.
func jsonTopic(name string) scenario.Topic {
	tp := defaultTopicForTest()
	tp.Name = name
	return tp
}

func defaultTopicForTest() scenario.Topic {
	var sc *scenario.Scenario
	// Round-tripping through the decoder guarantees the test uses the same defaults real
	// scenarios get, rather than a hand-written struct that could drift from them.
	sc, diags := scenario.Decode([]byte(`
name: test
description: test
topics:
  - name: placeholder
    partitions: 1
    replication_factor: 1
    num_producers: 1
    num_consumer_groups: 1
    consumers_per_group: 1
    producer_rate: 200
`))
	if diags.HasErrors() {
		panic(diags.String())
	}
	return sc.Topics[0]
}

// TestEngineEndToEnd is the architectural proof: config -> generate -> encode -> produce
// -> consume -> snapshot -> clean shutdown, with no terminal attached.
func TestEngineEndToEnd(t *testing.T) {
	addrs := newFakeCluster(t)

	sc := &scenario.Scenario{
		Name:        "e2e",
		Description: "end to end",
		Topics:      []scenario.Topic{jsonTopic("e2e-topic")},
	}

	eng, err := New(context.Background(), Config{
		Kafka:     kafka.Config{BootstrapServers: addrs},
		Scenarios: []*scenario.Scenario{sc},
		Duration:  2 * time.Second,
		Seed:      42,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	snap := eng.Snapshot()
	if snap.TotalProduced == 0 {
		t.Fatal("produced nothing")
	}
	if len(snap.Topics) != 1 || snap.Topics[0].Topic != "e2e-topic" {
		t.Fatalf("unexpected topics in snapshot: %+v", snap.Topics)
	}
	if snap.Topics[0].Bytes == 0 {
		t.Error("no bytes recorded")
	}
	// The run had a 2s deadline at 200 msg/s. Assert a band, never an equality: actual
	// throughput depends on scheduling and broker acknowledgement latency.
	if snap.TotalProduced > 1000 {
		t.Errorf("produced %d, far above the configured rate", snap.TotalProduced)
	}
	t.Logf("produced=%d consumed=%d bytes=%d elapsed=%s",
		snap.TotalProduced, snap.TotalConsumed, snap.Topics[0].Bytes, snap.Elapsed)
}

// TestEngineStopsOnContextCancel proves the run is terminable with no renderer present:
// an infinite run must be stoppable by context cancellation alone.
func TestEngineStopsOnContextCancel(t *testing.T) {
	addrs := newFakeCluster(t)

	sc := &scenario.Scenario{
		Name:   "cancel",
		Topics: []scenario.Topic{jsonTopic("cancel-topic")},
	}

	eng, err := New(context.Background(), Config{
		Kafka:     kafka.Config{BootstrapServers: addrs},
		Scenarios: []*scenario.Scenario{sc},
		Duration:  0, // run until cancelled
		Seed:      7,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()

	time.Sleep(500 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after cancel: %v", err)
		}
	case <-time.After(teardownDeadline + 5*time.Second):
		t.Fatal("engine did not stop after context cancellation")
	}
}

// TestSnapshotIsIndependent asserts a Snapshot is a value a consumer can hold safely.
//
// The TUI holds one across frames, so if it aliased engine state the renderer would race
// every producer in the run.
func TestSnapshotIsIndependent(t *testing.T) {
	addrs := newFakeCluster(t)

	sc := &scenario.Scenario{Name: "snap", Topics: []scenario.Topic{jsonTopic("snap-topic")}}
	eng, err := New(context.Background(), Config{
		Kafka:     kafka.Config{BootstrapServers: addrs},
		Scenarios: []*scenario.Scenario{sc},
		Duration:  time.Second,
		Seed:      1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	eng.events.add(Event{At: time.Now(), Message: "marker"})
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	first := eng.Snapshot()
	before := first.Topics[0].Produced
	first.Topics[0].Produced = -999

	if got := eng.Snapshot().Topics[0].Produced; got != before {
		t.Errorf("mutating a snapshot changed engine state: got %d, want %d", got, before)
	}

	// Events and Groups are the two slices that could plausibly alias -- one is the
	// event ring's backing array, the other is derived per call from live consumers.
	if len(first.Events) == 0 {
		t.Fatal("snapshot carried no events")
	}
	first.Events[0].Message = "clobbered"
	if got := eng.Snapshot().Events[0].Message; got == "clobbered" {
		t.Error("Snapshot().Events aliases the engine's event ring")
	}

	if len(first.Topics[0].Groups) == 0 {
		t.Fatal("snapshot carried no consumer groups")
	}
	beforeGroup := first.Topics[0].Groups[0].Consumed
	first.Topics[0].Groups[0].Consumed = -999
	if got := eng.Snapshot().Topics[0].Groups[0].Consumed; got != beforeGroup {
		t.Errorf("mutating a snapshot's group changed engine state: got %d, want %d",
			got, beforeGroup)
	}
}

// timestampField matches the wall-clock field every synthetic payload carries.
var timestampField = regexp.MustCompile(`"timestamp":\d+`)

// TestSeedReproducibility asserts two runs with the same seed generate identical payloads.
//
// The timestamp is normalised before comparing: it is wall-clock, so it is not seeded and
// cannot be. Comparing it made this test a coin flip on whether the millisecond ticked
// between the two collections, which is exactly how it failed intermittently rather than
// reporting a real reproducibility regression.
func TestSeedReproducibility(t *testing.T) {
	collect := func(seed uint64) []byte {
		tp := jsonTopic("seed-topic")
		rnd := (&Engine{seed: seed}).rngFor("producer", tp.Name, 0)
		gen, err := valueFuncForTest(tp, rnd)
		if err != nil {
			t.Fatalf("value func: %v", err)
		}
		var out []byte
		for i := 0; i < 5; i++ {
			b, err := gen()
			if err != nil {
				t.Fatalf("generate: %v", err)
			}
			out = append(out, b...)
		}
		return timestampField.ReplaceAll(out, []byte(`"timestamp":0`))
	}

	a, b := collect(99), collect(99)
	if string(a) != string(b) {
		t.Errorf("same seed produced different output:\n %s\n %s", a, b)
	}
	if string(a) == string(collect(100)) {
		t.Error("different seeds produced identical output")
	}
	// Guard the normalisation itself: if the payload ever stops carrying a timestamp the
	// substitution silently becomes a no-op and the test keeps passing for the wrong
	// reason.
	if !bytes.Contains(a, []byte(`"timestamp":0`)) {
		t.Error("payload no longer carries a timestamp field; the normalisation is dead")
	}
}

// TestSnapshotConcurrentWithRun hammers the read API while the run mutates everything
// behind it.
//
// Snapshot is documented as the engine's entire read API and the TUI, the headless log
// loop and the Prometheus collector all poll it on their own schedules. Nothing exercised
// that overlap, so a non-atomic field on Engine could sit there indefinitely -- which is
// what happened to the run start time.
func TestSnapshotConcurrentWithRun(t *testing.T) {
	addrs := newFakeCluster(t)

	tp := jsonTopic("concurrent-snap")
	tp.NumProducers = 3
	tp.ConsumersPerGroup = 2

	eng, err := New(context.Background(), Config{
		Kafka:     kafka.Config{BootstrapServers: addrs},
		Scenarios: []*scenario.Scenario{{Name: "snap", Topics: []scenario.Topic{tp}}},
		Duration:  2 * time.Second,
		Seed:      21,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap := eng.Snapshot()
				// Touch everything, so the detector sees the reads and a nil deref would
				// surface here rather than in a renderer.
				_ = snap.Elapsed
				for _, ts := range snap.Topics {
					for _, gs := range ts.Groups {
						_ = gs.Consumed
					}
				}
				for _, ev := range snap.Events {
					_ = ev.Message
				}
			}
		}()
	}

	runErr := eng.Run(context.Background())
	close(stop)
	readers.Wait()

	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
}

// TestNewClosesClientsWhenBuildFails covers the half-built engine.
//
// New creates clients topic by topic and can fail on any of them, and the engine it was
// building is thrown away -- so whatever it already opened has to be closed on the way
// out or the process keeps a full set of Kafka connections and their goroutines for a
// configuration error. The package-wide goleak check is what actually enforces this.
func TestNewClosesClientsWhenBuildFails(t *testing.T) {
	addrs := newFakeCluster(t)

	good := jsonTopic("halfbuilt-ok")
	bad := jsonTopic("halfbuilt-bad")
	// Rejected by kgo when the client is constructed, which is late enough that the first
	// topic's producer and consumer clients are already open.
	bad.ProducerConfig.CompressionType = "definitely-not-a-codec"

	_, err := New(context.Background(), Config{
		Kafka:     kafka.Config{BootstrapServers: addrs},
		Scenarios: []*scenario.Scenario{{Name: "half", Topics: []scenario.Topic{good, bad}}},
		Seed:      2,
	})
	if err == nil {
		t.Fatal("New succeeded with an invalid compression codec")
	}
	if !strings.Contains(err.Error(), bad.Name) {
		t.Errorf("error does not name the offending topic: %v", err)
	}
}

// TestEventRingIsBounded guards the memory-stability requirement for week-long runs.
func TestEventRingIsBounded(t *testing.T) {
	r := newEventRing(4)
	for i := 0; i < 100; i++ {
		r.add(Event{Message: string(rune('a' + i%26))})
	}
	if got := len(r.snapshot()); got != 4 {
		t.Errorf("ring held %d events, want 4", got)
	}
}

// valueFuncForTest builds the payload generator without a codec, for the fieldless path.
func valueFuncForTest(t scenario.Topic, rnd *rand.Rand) (func() ([]byte, error), error) {
	raw := generate.NewRawJSONGen(t.MessageSchema, rnd)
	return func() ([]byte, error) { return raw.Next(), nil }, nil
}
