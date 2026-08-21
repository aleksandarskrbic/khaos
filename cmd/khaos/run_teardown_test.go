package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

// fakeCluster stands in for the Docker-backed localcluster.Cluster so `run`'s teardown
// path can be exercised in CI. Every field is atomic because the engine may call
// StopBroker/StartBroker from its own goroutines while the test reads the counters.
type fakeCluster struct {
	running  bool // whether BootstrapServers succeeds before Up is called
	brokers  string
	upErr    error
	downErr  error
	ups      atomic.Int64
	downs    atomic.Int64
	stops    atomic.Int64
	restarts atomic.Int64
}

func (f *fakeCluster) Up(context.Context) error {
	f.ups.Add(1)
	if f.upErr != nil {
		return f.upErr
	}
	f.running = true
	return nil
}

func (f *fakeCluster) Down(context.Context, bool) error {
	f.downs.Add(1)
	f.running = false
	return f.downErr
}

// BootstrapServers mirrors the REAL implementation, which falls back to a default address
// and does NOT fail when nothing is running.
//
// An earlier version of this double returned an error when the cluster was down. That was
// wrong, and it was load-bearing: `run` used the error as its is-it-up test, the double
// made that work, and the real command shipped broken -- `khaos run` on a stopped cluster
// died with "connection refused" instead of starting it. A test double that is more
// helpful than the thing it replaces hides exactly the bug it should catch.
func (f *fakeCluster) BootstrapServers(context.Context) (string, error) {
	if !f.running {
		return "127.0.0.1:9092", nil
	}
	return f.brokers, nil
}

// Running is the real is-it-up test.
func (f *fakeCluster) Running(context.Context) bool { return f.running }

func (f *fakeCluster) StopBroker(context.Context, string) error  { f.stops.Add(1); return nil }
func (f *fakeCluster) StartBroker(context.Context, string) error { f.restarts.Add(1); return nil }

// runWithFakeCluster drives `khaos run` against a cluster that needs no Docker.
func runWithFakeCluster(t *testing.T, cl *fakeCluster, args ...string) (string, error) {
	t.Helper()

	root := &cobra.Command{Use: "khaos", SilenceUsage: true, SilenceErrors: true}
	root.AddCommand(runCmd(func(bool) (localCluster, error) { return cl, nil }))

	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(append([]string{"run"}, args...))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	err := root.ExecuteContext(ctx)
	return out.String(), err
}

// A failed scenario that leaves three brokers and a Kafka UI running is the single most
// expensive thing this CLI can get wrong on a laptop, so the teardown is pinned for the
// success path, the failure path and both under --keep-cluster.
func TestRunAlwaysStopsTheClusterUnlessKeepCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		bootstrap func(t *testing.T) string // where the scenario points
		extra     []string
		wantErr   bool
		wantDowns int64
	}{
		{
			name:      "successful run stops the cluster",
			bootstrap: func(t *testing.T) string { return fakeBrokers(t) },
			wantDowns: 1,
		},
		{
			name: "failed run still stops the cluster",
			// Nothing listens on port 1, so engine setup fails and execute returns an
			// error before the run ever starts.
			bootstrap: func(*testing.T) string { return "127.0.0.1:1" },
			wantErr:   true,
			wantDowns: 1,
		},
		{
			name:      "--keep-cluster leaves it running after success",
			bootstrap: func(t *testing.T) string { return fakeBrokers(t) },
			extra:     []string{"--keep-cluster"},
			wantDowns: 0,
		},
		{
			name:      "--keep-cluster is honoured on the failure path too",
			bootstrap: func(*testing.T) string { return "127.0.0.1:1" },
			extra:     []string{"-k"},
			wantErr:   true,
			wantDowns: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cl := &fakeCluster{running: true, brokers: tt.bootstrap(t)}
			args := append([]string{
				"traffic/high-throughput",
				"--duration", runDuration,
				"--tui", "off",
				"--log-level", "error",
			}, tt.extra...)

			out, err := runWithFakeCluster(t, cl, args...)
			if tt.wantErr && err == nil {
				t.Fatalf("expected a failure\n%s", out)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected failure: %v\n%s", err, out)
			}
			if got := cl.downs.Load(); got != tt.wantDowns {
				t.Errorf("Down called %d times, want %d\n%s", got, tt.wantDowns, out)
			}
		})
	}
}

// A cluster that is already up is not started again, and one that is not up is.
func TestRunStartsTheClusterOnlyWhenItIsDown(t *testing.T) {
	t.Parallel()

	t.Run("already running", func(t *testing.T) {
		t.Parallel()
		cl := &fakeCluster{running: true, brokers: fakeBrokers(t)}
		out, err := runWithFakeCluster(t, cl, "traffic/high-throughput",
			"--duration", runDuration, "--tui", "off", "--log-level", "error")
		if err != nil {
			t.Fatalf("run: %v\n%s", err, out)
		}
		if got := cl.ups.Load(); got != 0 {
			t.Errorf("Up called %d times on an already-running cluster", got)
		}
	})

	t.Run("not running", func(t *testing.T) {
		t.Parallel()
		// Not running to begin with; Up flips it and BootstrapServers then answers.
		cl := &fakeCluster{running: false, brokers: fakeBrokers(t)}
		out, err := runWithFakeCluster(t, cl, "traffic/high-throughput",
			"--duration", runDuration, "--tui", "off", "--log-level", "error")
		if err != nil {
			t.Fatalf("run: %v\n%s", err, out)
		}
		if got := cl.ups.Load(); got != 1 {
			t.Errorf("Up called %d times on a stopped cluster, want 1", got)
		}
		if !strings.Contains(out, "Starting Kafka cluster") {
			t.Errorf("no notice that the cluster was being started\n%s", out)
		}
	})
}

// -b overrides the cluster's own bootstrap servers: the cluster is still managed, but
// traffic goes elsewhere.
func TestRunBootstrapOverrideStillManagesTheCluster(t *testing.T) {
	t.Parallel()

	cl := &fakeCluster{running: true, brokers: "127.0.0.1:1"} // would fail if used
	out, err := runWithFakeCluster(t, cl, "traffic/high-throughput",
		"--bootstrap-servers", fakeBrokers(t),
		"--duration", runDuration, "--tui", "off", "--log-level", "error")
	if err != nil {
		t.Fatalf("run with -b: %v\n%s", err, out)
	}
	if got := cl.downs.Load(); got != 1 {
		t.Errorf("Down called %d times, want 1", got)
	}
}

// A cluster that will not stop must not turn a successful run into a failure, but it must
// say so: a silent failure here leaves containers running with no trace in the log.
func TestRunReportsButDoesNotFailOnTeardownError(t *testing.T) {
	t.Parallel()

	cl := &fakeCluster{running: true, brokers: fakeBrokers(t), downErr: errors.New("docker is wedged")}
	out, err := runWithFakeCluster(t, cl, "traffic/high-throughput",
		"--duration", runDuration, "--tui", "off", "--log-level", "error")
	if err != nil {
		t.Fatalf("a teardown failure should not fail the run: %v\n%s", err, out)
	}
	if !strings.Contains(out, "could not stop cluster") {
		t.Errorf("teardown failure was swallowed\n%s", out)
	}
}

// If the cluster cannot be started there is nothing to tear down.
func TestRunDoesNotTearDownAClusterItCouldNotStart(t *testing.T) {
	t.Parallel()

	cl := &fakeCluster{running: false, upErr: errors.New("port 9092 is already allocated")}
	out, err := runWithFakeCluster(t, cl, "traffic/high-throughput",
		"--duration", runDuration, "--tui", "off", "--log-level", "error")
	if err == nil {
		t.Fatalf("a cluster that will not start should fail the run\n%s", out)
	}
	if got := cl.downs.Load(); got != 0 {
		t.Errorf("Down called %d times after a failed start, want 0", got)
	}
}
