package localcluster

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestPollSucceedsOnFirstProbe(t *testing.T) {
	calls := 0
	err := poll(context.Background(), time.Second, time.Millisecond, "thing", func(context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if calls != 1 {
		t.Fatalf("probed %d times, want the first probe to run immediately", calls)
	}
}

func TestPollRetriesUntilReady(t *testing.T) {
	calls := 0
	err := poll(context.Background(), time.Second, time.Millisecond, "thing", func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("not yet")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if calls != 3 {
		t.Fatalf("probed %d times, want 3", calls)
	}
}

func TestPollTimesOut(t *testing.T) {
	err := poll(context.Background(), 20*time.Millisecond, 5*time.Millisecond, "kafka cluster", func(context.Context) error {
		return errors.New("connection refused")
	})
	if !errors.Is(err, ErrNotReady) {
		t.Fatalf("err = %v, want ErrNotReady", err)
	}
}

// Cancellation must win immediately, so a Ctrl-C during `cluster up` does not have to wait
// out the whole readiness budget.
func TestPollHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		done <- poll(ctx, time.Hour, time.Hour, "kafka cluster", func(context.Context) error {
			return errors.New("not yet")
		})
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("poll ignored a cancelled context")
	}
}

// fakeBroker answers every connection with a minimal ApiVersions response frame carrying
// the requested correlation id, like a live broker would.
func fakeBroker(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				var hdr [4]byte
				if _, err := io.ReadFull(conn, hdr[:]); err != nil {
					return
				}
				req := make([]byte, binary.BigEndian.Uint32(hdr[:]))
				if _, err := io.ReadFull(conn, req); err != nil || len(req) < 8 {
					return
				}
				corr := req[4:8] // api key + version precede the correlation id
				// correlation id + error code NONE + empty api_versions array
				body := append(append([]byte{}, corr...), 0, 0, 0, 0, 0, 0)
				resp := binary.BigEndian.AppendUint32(nil, uint32(len(body)))
				_, _ = conn.Write(append(resp, body...))
			}()
		}
	}()
	return ln
}

func TestProbeAllRequiresARealBrokerAnswer(t *testing.T) {
	broker := fakeBroker(t)
	defer broker.Close()

	ctx := context.Background()
	if err := probeAll(ctx, []string{broker.Addr().String()}); err != nil {
		t.Fatalf("probeAll against an answering broker: %v", err)
	}

	// Accept-then-close is exactly what docker's userland proxy does while the broker
	// inside is still booting. It used to satisfy the dial-based probe; it must not
	// satisfy this one.
	proxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer proxy.Close()
	go func() {
		for {
			conn, err := proxy.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	if err := probeAll(ctx, []string{proxy.Addr().String()}); err == nil {
		t.Fatal("probeAll was satisfied by a listener that closes without answering")
	}

	// A second, dead address must fail the whole probe: it requires all brokers, not
	// just one reachable one.
	dead := deadAddress(t)
	if err := probeAll(ctx, []string{broker.Addr().String(), dead}); err == nil {
		t.Fatal("probeAll succeeded with one unreachable address")
	}
}

// deadAddress returns a loopback address that nothing is listening on.
func deadAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}

func TestHostPort(t *testing.T) {
	if got := hostPort(9092); got != "127.0.0.1:9092" {
		t.Fatalf("hostPort = %q, want 127.0.0.1:9092", got)
	}
}
