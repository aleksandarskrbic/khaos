package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// freeAddr finds an address nothing is listening on yet, for a flag that wants an address
// string rather than a pre-bound listener. There is a small window between Close and the
// CLI's own bind where another process could grab the port; in a CI/dev sandbox on
// 127.0.0.1 this has not been a source of flakes for the equivalent pattern already used in
// internal/localcluster/ready_test.go.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find a free port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// TestMetricsAddrServesRealMetrics is the end-to-end proof for --metrics-addr: a full CLI
// run against a fake cluster must serve /metrics with real, non-zero exposition text
// reflecting the traffic it generated -- not a 404 (the nil-registry bug) and not an empty
// or permanently-zero registry (the unfed-metrics bug).
func TestMetricsAddrServesRealMetrics(t *testing.T) {
	t.Parallel()
	brokers := fakeBrokers(t)
	metricsAddr := freeAddr(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := runCLI(t, ctx, "simulate", "traffic/high-throughput",
			"--bootstrap-servers", brokers,
			"--duration", "5s",
			"--tui", "off",
			"--log-level", "error",
			"--metrics-addr", metricsAddr,
		)
		done <- err
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	url := "http://" + metricsAddr + "/metrics"

	var body string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		b, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			t.Fatalf("read /metrics body: %v", readErr)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /metrics = %d, want 200\n%s", resp.StatusCode, b)
		}
		body = string(b)
		// Give the run a moment to have actually produced something before deciding the
		// counter is genuinely stuck at zero rather than just not scraped yet.
		if strings.Contains(body, "khaos_messages_generated_total{") {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if body == "" {
		t.Fatal("never got a response from /metrics")
	}
	if !strings.Contains(body, "khaos_messages_generated_total{") {
		t.Fatalf("/metrics never reported a non-zero khaos_messages_generated_total series:\n%s", body)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("simulate: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run did not finish after its 5s duration")
	}
}
