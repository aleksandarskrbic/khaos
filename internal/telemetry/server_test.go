package telemetry

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// startServer runs srv in the background and waits for it to bind, returning its base URL
// and a function that stops it and reports Run's error.
func startServer(t *testing.T, srv *Server) (string, func() error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	base := "http://" + waitForBind(t, srv)

	// stop is idempotent: tests call it explicitly to assert on Run's error and the
	// cleanup calls it again, so the result has to be memoized rather than re-read from
	// the (already drained) channel.
	var once sync.Once
	var stopErr error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case stopErr = <-done:
			case <-time.After(10 * time.Second):
				stopErr = errors.New("Run did not return after cancellation")
			}
		})
		return stopErr
	}
	t.Cleanup(func() { _ = stop() })
	return base, stop
}

// waitForBind polls Addr until Run has resolved the wildcard port.
func waitForBind(t *testing.T, srv *Server) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		addr := srv.Addr()
		if _, port, err := net.SplitHostPort(addr); err == nil && port != "0" {
			return addr
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("server never bound a port")
	return ""
}

// testClient does not pool connections. http.DefaultClient's idle connections outlive the
// test that made them and their readLoop goroutines would show up as leaks in TestMain,
// masking a real one in Server.Run.
var testClient = &http.Client{
	Timeout:   5 * time.Second,
	Transport: &http.Transport{DisableKeepAlives: true},
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := testClient.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(body)
}

func TestHealthzReflectsHealthFunc(t *testing.T) {
	tests := []struct {
		name       string
		health     func() error
		wantStatus int
		wantBody   string
	}{
		{"nil health func is always healthy", nil, http.StatusOK, "ok"},
		{"healthy", func() error { return nil }, http.StatusOK, "ok"},
		{"unhealthy", func() error { return errors.New("broker unreachable") }, http.StatusServiceUnavailable, "broker unreachable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base, _ := startServer(t, NewServer("127.0.0.1:0", prometheus.NewRegistry(), tt.health))

			status, body := get(t, base+"/healthz")
			if status != tt.wantStatus {
				t.Errorf("status = %d, want %d", status, tt.wantStatus)
			}
			if !strings.Contains(body, tt.wantBody) {
				t.Errorf("body = %q, want it to contain %q", body, tt.wantBody)
			}
		})
	}
}

// The health func is consulted per request, not cached at construction.
func TestHealthzIsEvaluatedPerRequest(t *testing.T) {
	var healthy bool
	health := func() error {
		if healthy {
			return nil
		}
		return errors.New("still starting")
	}

	base, _ := startServer(t, NewServer("127.0.0.1:0", prometheus.NewRegistry(), health))

	if status, _ := get(t, base+"/healthz"); status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 before becoming healthy", status)
	}
	healthy = true
	if status, _ := get(t, base+"/healthz"); status != http.StatusOK {
		t.Fatalf("status = %d, want 200 once healthy", status)
	}
}

func TestMetricsEndpointServesRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.GeneratedMessages.WithLabelValues("orders").Add(7)

	base, _ := startServer(t, NewServer("127.0.0.1:0", reg, nil))

	status, body := get(t, base+"/metrics")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if !strings.Contains(body, `khaos_messages_generated_total{topic="orders"} 7`) {
		t.Fatalf("metrics output missing the counter:\n%s", body)
	}
}

// An empty addr is how the CLI disables telemetry: Run must be a no-op, bind nothing and
// return immediately even with a context that is never cancelled.
func TestEmptyAddrIsANoOp(t *testing.T) {
	srv := NewServer("", prometheus.NewRegistry(), nil)

	done := make(chan error, 1)
	go func() { done <- srv.Run(context.Background()) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run blocked despite an empty addr")
	}

	if got := srv.Addr(); got != "" {
		t.Fatalf("Addr = %q, want empty", got)
	}
}

// Run is an errgroup member: cancelling the group's context must return nil, not an
// error, or a clean Ctrl-C would look like a failure.
func TestRunReturnsCleanlyOnCancellation(t *testing.T) {
	srv := NewServer("127.0.0.1:0", prometheus.NewRegistry(), nil)
	_, stop := startServer(t, srv)

	if err := stop(); err != nil {
		t.Fatalf("Run = %v, want nil after cancellation", err)
	}

	// The listener must actually be gone.
	addr := srv.Addr()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("still accepting connections on %s after shutdown", addr)
}

func TestRunReportsListenFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	srv := NewServer(ln.Addr().String(), prometheus.NewRegistry(), nil)
	if err := srv.Run(context.Background()); err == nil {
		t.Fatal("want an error when the port is already bound")
	}
}

func TestAddrBeforeAndAfterBind(t *testing.T) {
	srv := NewServer("127.0.0.1:0", prometheus.NewRegistry(), nil)
	if got := srv.Addr(); got != "127.0.0.1:0" {
		t.Fatalf("Addr before Run = %q, want the configured address", got)
	}

	startServer(t, srv)

	if got := srv.Addr(); got == "127.0.0.1:0" {
		t.Fatal("Addr after Run still reports the wildcard port")
	}
}

func TestUnknownPathIs404(t *testing.T) {
	base, _ := startServer(t, NewServer("127.0.0.1:0", prometheus.NewRegistry(), nil))

	if status, _ := get(t, base+"/nope"); status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", status)
	}
}

// A nil registry means "no metrics", not a nil-pointer dereference on the first scrape:
// cmd/khaos passes nil today (cmd/khaos/run.go), so this is the shipped configuration.
func TestNilRegistryServesHealthzButNotMetrics(t *testing.T) {
	base, _ := startServer(t, NewServer("127.0.0.1:0", nil, nil))

	if status, _ := get(t, base+"/healthz"); status != http.StatusOK {
		t.Fatalf("/healthz status = %d, want 200", status)
	}
	if status, _ := get(t, base+"/metrics"); status != http.StatusNotFound {
		t.Fatalf("/metrics status = %d, want 404 with no registry", status)
	}
}

// Run must return on cancellation *promptly*. It is joined on the shutdown path in
// cmd/khaos/run.go, so a slow return is dead time between Ctrl-C and the process exiting;
// the graceful drain is bounded by shutdownTimeout but an idle server has nothing to
// drain and must not wait for it.
func TestRunReturnsPromptlyOnCancellation(t *testing.T) {
	srv := NewServer("127.0.0.1:0", prometheus.NewRegistry(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	waitForBind(t, srv)

	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(shutdownTimeout):
		t.Fatalf("Run did not return within %s of cancellation", shutdownTimeout)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Run took %s to return from an idle server", elapsed)
	}
}

// Cancelling before Run is even called must still be a clean, immediate return rather than
// a bound listener nobody ever closes.
func TestRunWithAlreadyCancelledContext(t *testing.T) {
	srv := NewServer("127.0.0.1:0", prometheus.NewRegistry(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run blocked on an already-cancelled context")
	}

	// Whatever port it briefly took must be released before Run returned.
	if addr := srv.Addr(); addr != "127.0.0.1:0" {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			t.Fatalf("listener on %s outlived Run", addr)
		}
	}
}
