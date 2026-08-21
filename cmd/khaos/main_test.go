package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
)

// These are end-to-end CLI tests: real flag parsing, real scenario loading, a real engine,
// and a real Kafka wire protocol via kfake. They are the closest thing to running the
// binary that can live in CI, and they cover the path a user actually takes.
//
// Three brokers because the shipped scenarios specify replication_factor: 3.
//
// Each test gets its OWN cluster rather than sharing one. kfake is in-process and cheap,
// and sharing is not safe here: khaos deletes and recreates every topic at startup, and
// the shipped scenarios reuse topic names -- `events` belongs to both high-throughput and
// rebalance-storm, `transactions` to both throughput-drop and hot-partition. Two parallel
// runs on one cluster would delete each other's topics mid-flight.
func fakeBrokers(t *testing.T) string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(3), kfake.AllowAutoTopicCreation())
	if err != nil {
		t.Fatalf("start fake cluster: %v", err)
	}
	t.Cleanup(c.Close)
	return strings.Join(c.ListenAddrs(), ",")
}

// runCLI invokes the root command exactly as main does.
//
// Output is captured through cobra's writers, never by swapping os.Stdout, so these tests
// are safe to run in parallel.
func runCLI(t *testing.T, ctx context.Context, args ...string) (string, error) {
	t.Helper()
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(ctx)
	return out.String(), err
}

// runDuration is how long the end-to-end scenarios produce for. It is short on purpose:
// these tests assert that the whole pipeline runs and tears down cleanly, not that any
// particular throughput is reached. Every incident in the shipped chaos scenarios is
// scheduled well past this (T+15s at the earliest), so a longer run would not cover more
// of them either -- only the schedule-and-shutdown path, which TestIncidentSchedule covers
// directly.
const runDuration = "2s"

// TestSimulateAgainstFakeCluster runs several shipped scenarios end to end through the
// CLI. Each one exercises a different subsystem.
func TestSimulateAgainstFakeCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		scenario string
		why      string
	}{
		{"plain traffic", "traffic/high-throughput", "producers, consumers, JSON payloads"},
		{"hot partition", "traffic/hot-partition", "skewed key distribution"},
		{"consumer lag", "traffic/consumer-lag", "consumer processing delay"},
		{"client-side incident", "chaos/throughput-drop", "increase_consumer_delay incident is scheduled"},
		{"rebalance incident", "chaos/rebalance-storm", "consumer teardown and recreation mid-run"},
		{"broker incident on external cluster", "chaos/leadership-churn", "broker faults degrade to events, not failures"},
		{"flows", "flows/order-flow", "correlated multi-step flow instances"},
		{"duplicates and DLQ", "testing/consumer-failures", "consumer failure injection and dead-letter routing"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Safe to parallelise: every subtest builds its own root command, its own
			// engine and its own kfake cluster, and writes only to its own buffer. No
			// package-level state is touched.
			t.Parallel()

			brokers := fakeBrokers(t)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			out, err := runCLI(t, ctx, "simulate", tt.scenario,
				"--bootstrap-servers", brokers,
				"--duration", runDuration,
				"--tui", "off",
				"--log-level", "error",
			)
			if err != nil {
				t.Fatalf("%s (%s) failed: %v\noutput:\n%s", tt.scenario, tt.why, err, out)
			}
			if !strings.Contains(out, "PRODUCED") || !strings.Contains(out, "TOTAL") {
				t.Errorf("%s printed no summary table\noutput:\n%s", tt.scenario, out)
			}
		})
	}
}

// TestIncidentFiresEndToEnd proves an incident really happens during a CLI run, which the
// short scenarios above do not: every shipped chaos scenario schedules its first incident
// at T+15s or later, so a 2s run only ever covers scheduling, never firing. Rather than
// pay 36 seconds to watch rebalance-storm (initial_delay 15s + every 20s), this uses a
// purpose-built scenario whose incident lands two seconds in.
func TestIncidentFiresEndToEnd(t *testing.T) {
	t.Parallel()
	brokers := fakeBrokers(t)

	path := writeScenario(t, `name: incident-timing
description: fires one rebalance two seconds in
topics:
  - name: incident-timing
    partitions: 3
    replication_factor: 3
    num_producers: 1
    num_consumer_groups: 1
    consumers_per_group: 2
    producer_rate: 100
incidents:
  - type: rebalance_consumer
    at_seconds: 2
`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	out, err := runCLI(t, ctx, "simulate", path,
		"--bootstrap-servers", brokers,
		"--duration", "5s",
		"--tui", "off",
		"--log-level", "error",
	)
	if err != nil {
		t.Fatalf("scheduled incident run: %v\n%s", err, out)
	}
	// The summary prints the rebalance count only when it is non-zero, so its presence is
	// proof the scheduled incident fired rather than being scheduled and forgotten.
	if !strings.Contains(out, "rebalance") {
		t.Errorf("the incident scheduled at T+2s never fired in a 5s run\n%s", out)
	}
}

// TestSimulateStopsAtDuration asserts the run honours its deadline with no terminal
// attached.
func TestSimulateStopsAtDuration(t *testing.T) {
	t.Parallel()
	brokers := fakeBrokers(t)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	if _, err := runCLI(t, ctx, "simulate", "traffic/high-throughput",
		"--bootstrap-servers", brokers,
		"--duration", "3s",
		"--tui", "off",
		"--log-level", "error",
	); err != nil {
		t.Fatalf("simulate: %v", err)
	}

	elapsed := time.Since(start)
	if elapsed < 3*time.Second {
		t.Errorf("returned after %s, before the 3s deadline", elapsed)
	}
	// Generous ceiling: this asserts the engine is not hanging, not that it is fast.
	if elapsed > 25*time.Second {
		t.Errorf("took %s to stop after a 3s run; teardown is not prompt", elapsed)
	}
}

// TestSimulateCancellation asserts a run with no deadline stops when interrupted, which
// is the 24/7 daemon's shutdown path.
func TestSimulateCancellation(t *testing.T) {
	t.Parallel()
	brokers := fakeBrokers(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := runCLI(t, ctx, "simulate", "traffic/high-throughput",
			"--bootstrap-servers", brokers,
			"--duration", "0", // until interrupted
			"--tui", "off",
			"--log-level", "error",
		)
		done <- err
	}()

	time.Sleep(2 * time.Second)
	cancel()

	select {
	case err := <-done:
		// Cancellation can land while the engine is still in topic setup, where it
		// surfaces through the net dialer as "operation was canceled" rather than
		// "context canceled". Both are the clean outcome this test wants.
		if err != nil && !strings.Contains(err.Error(), "canceled") {
			t.Fatalf("unexpected error on cancel: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run did not stop after cancellation")
	}
}

// TestValidateExitCodes locks the contract that `khaos validate` is usable from CI.
func TestValidateExitCodes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	if _, err := runCLI(t, ctx, "validate", "traffic/high-throughput"); err != nil {
		t.Errorf("valid scenario reported an error: %v", err)
	}
	if _, err := runCLI(t, ctx, "validate", "definitely/not/a/scenario"); err == nil {
		t.Error("unknown scenario should fail")
	}
}

// TestListSucceeds asserts the bundled corpus is reachable from a compiled binary, i.e.
// the go:embed fallback works when there is no scenarios/ directory alongside.
func TestListSucceeds(t *testing.T) {
	t.Parallel()
	if _, err := runCLI(t, context.Background(), "list"); err != nil {
		t.Fatalf("list: %v", err)
	}
}
