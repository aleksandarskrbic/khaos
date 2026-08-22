package engine

import (
	"context"
	"testing"
	"time"

	"github.com/aleksandarskrbic/khaos/internal/kafka"
	"github.com/aleksandarskrbic/khaos/internal/scenario"
	"github.com/aleksandarskrbic/khaos/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus"
)

// counterTotal sums every series of a counter family, across all label combinations.
func counterTotal(t *testing.T, reg *prometheus.Registry, family string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var total float64
	for _, f := range families {
		if f.GetName() != family {
			continue
		}
		for _, m := range f.GetMetric() {
			total += m.GetCounter().GetValue()
		}
	}
	return total
}

// TestEngineFeedsPrometheusMetrics proves Config.Metrics is actually populated by a live
// run, not merely registered and left at zero. Before this was wired up, --metrics-addr
// served /metrics with the whole khaos_* family present but permanently at zero, because
// nothing in the engine ever called Inc()/Add() on it (cmd/khaos/run.go used to pass a nil
// registry, so /metrics 404'd outright; fixing that alone would still have left every
// counter dead).
func TestEngineFeedsPrometheusMetrics(t *testing.T) {
	addrs := newFakeCluster(t)

	reg := prometheus.NewRegistry()
	metrics := telemetry.NewMetrics(reg)

	sc := &scenario.Scenario{
		Name:   "metrics",
		Topics: []scenario.Topic{jsonTopic("metrics-topic")},
	}

	eng, err := New(context.Background(), Config{
		Kafka:     kafka.Config{BootstrapServers: addrs},
		Scenarios: []*scenario.Scenario{sc},
		Duration:  2 * time.Second,
		Seed:      7,
		Metrics:   metrics,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	snap := eng.Snapshot()
	if snap.TotalProduced == 0 || snap.TotalConsumed == 0 {
		t.Fatalf("run produced no traffic to measure: produced=%d consumed=%d", snap.TotalProduced, snap.TotalConsumed)
	}

	// The Prometheus counters must agree with the engine's own ground truth (Snapshot),
	// not just be non-zero.
	if got := counterTotal(t, reg, "khaos_messages_generated_total"); int64(got) != snap.TotalProduced {
		t.Errorf("khaos_messages_generated_total = %v, want %d (Snapshot.TotalProduced)", got, snap.TotalProduced)
	}
	if got := counterTotal(t, reg, "khaos_messages_consumed_total"); int64(got) != snap.TotalConsumed {
		t.Errorf("khaos_messages_consumed_total = %v, want %d (Snapshot.TotalConsumed)", got, snap.TotalConsumed)
	}
	if got := counterTotal(t, reg, "khaos_bytes_generated_total"); got == 0 {
		t.Error("khaos_bytes_generated_total = 0, want > 0")
	}
}

// TestEngineWithoutMetricsDoesNotPanic pins that a nil Config.Metrics -- the default, and
// what every engine gets without --metrics-addr -- is a true no-op, not a nil-pointer
// dereference the first time a producer or consumer touches it.
func TestEngineWithoutMetricsDoesNotPanic(t *testing.T) {
	addrs := newFakeCluster(t)

	sc := &scenario.Scenario{
		Name:   "no-metrics",
		Topics: []scenario.Topic{jsonTopic("no-metrics-topic")},
	}

	eng, err := New(context.Background(), Config{
		Kafka:     kafka.Config{BootstrapServers: addrs},
		Scenarios: []*scenario.Scenario{sc},
		Duration:  time.Second,
		Seed:      7,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestEngineFeedsRebalanceMetric proves a rebalance_consumer incident is reflected in
// khaos_rebalances_total, labelled by the group that actually rebalanced.
func TestEngineFeedsRebalanceMetric(t *testing.T) {
	addrs := newFakeCluster(t)

	reg := prometheus.NewRegistry()
	metrics := telemetry.NewMetrics(reg)

	tp := jsonTopic("rebalance-metric-topic")
	tp.ConsumersPerGroup = 2

	sc := &scenario.Scenario{
		Name:   "rebalance-metric",
		Topics: []scenario.Topic{tp},
		Incidents: []scenario.Incident{
			scenario.RebalanceConsumer{
				Target:   scenario.ConsumerTarget{Topic: tp.Name},
				Schedule: scenario.Schedule{AtSeconds: intPtr(0)},
			},
		},
	}

	eng, err := New(context.Background(), Config{
		Kafka:     kafka.Config{BootstrapServers: addrs},
		Scenarios: []*scenario.Scenario{sc},
		Duration:  5 * time.Second,
		Seed:      3,
		Metrics:   metrics,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	snap := eng.Snapshot()
	if snap.Rebalances == 0 {
		t.Fatal("expected the scheduled rebalance_consumer to fire")
	}
	if got := counterTotal(t, reg, "khaos_rebalances_total"); int64(got) != snap.Rebalances {
		t.Errorf("khaos_rebalances_total = %v, want %d (Snapshot.Rebalances)", got, snap.Rebalances)
	}
}
