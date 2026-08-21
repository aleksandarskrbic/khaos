package localcluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// Readiness budgets for the broker and Schema Registry poll loops.
const (
	kafkaReadyTimeout = 120 * time.Second
	kafkaPollInterval = 3 * time.Second
	kafkaProbeTimeout = 5 * time.Second
	srReadyTimeout    = 60 * time.Second
	srPollInterval    = 2 * time.Second
	srProbeTimeout    = 5 * time.Second
)

// SchemaRegistryURL is where the bundled Schema Registry stack publishes itself.
const SchemaRegistryURL = "http://localhost:8081"

// poll runs probe immediately and then every interval until it succeeds, the overall
// timeout expires, or ctx is cancelled, so a Ctrl-C during `cluster up` does not have to
// wait out the full readiness budget.
func poll(ctx context.Context, timeout, interval time.Duration, what string, probe func(context.Context) error) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastErr error
	for {
		lastErr = probe(ctx)
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("waiting for %s: %w", what, ctx.Err())
		}
		if !time.Now().Add(interval).Before(deadline) {
			return fmt.Errorf("%s %w (waited %s, last error: %v); try: docker compose -p %s logs",
				what, ErrNotReady, timeout, lastErr, ProjectName)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %s: %w", what, ctx.Err())
		case <-ticker.C:
		}
	}
}

// hostPort renders a loopback address as 127.0.0.1 rather than "localhost", so clients
// never resolve to ::1, where docker's published ports may not be listening.
func hostPort(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
}

// probeAll reports an error unless every broker answers a Kafka ApiVersions request.
//
// This package deliberately does not import internal/kafka, so the request is hand-rolled
// -- the simplest exchange the protocol has. A plain TCP dial is not enough: docker's
// userland proxy accepts connections the moment the container's port is published, long
// before the broker inside is listening, so a dial-based probe would declare the cluster
// ready on cold boots and the first real request would then fail. All brokers must answer,
// not just one, because the engine connects to all of them.
func probeAll(ctx context.Context, addrs []string) error {
	for _, addr := range addrs {
		probeCtx, cancel := context.WithTimeout(ctx, kafkaProbeTimeout)
		err := kafkaProbe(probeCtx, addr)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// kafkaProbe sends one ApiVersions v0 request and checks the broker frames a response
// with the matching correlation id. The response body is irrelevant -- even an error
// response proves a live broker on the other end.
func kafkaProbe(ctx context.Context, addr string) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	const clientID = "khaos-ready-probe"
	body := make([]byte, 0, 10+len(clientID))
	body = binary.BigEndian.AppendUint16(body, 18) // api key: ApiVersions
	body = binary.BigEndian.AppendUint16(body, 0)  // api version
	body = binary.BigEndian.AppendUint32(body, 1)  // correlation id
	body = binary.BigEndian.AppendUint16(body, uint16(len(clientID)))
	body = append(body, clientID...)
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	frame = append(frame, body...)
	if _, err := conn.Write(frame); err != nil {
		return fmt.Errorf("write to %s: %w", addr, err)
	}

	var hdr [8]byte // response length + correlation id
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return fmt.Errorf("read from %s: %w", addr, err)
	}
	if n := binary.BigEndian.Uint32(hdr[:4]); n < 4 || n > 1<<20 {
		return fmt.Errorf("%s: implausible response length %d", addr, n)
	}
	if got := binary.BigEndian.Uint32(hdr[4:]); got != 1 {
		return fmt.Errorf("%s: response correlation id %d, want 1", addr, got)
	}
	return nil
}

// waitForKafka blocks until every published broker answers the protocol probe.
func (c *Cluster) waitForKafka(ctx context.Context, bootstrap string) error {
	addrs := strings.Split(bootstrap, ",")
	return poll(ctx, kafkaReadyTimeout, kafkaPollInterval, "kafka cluster", func(ctx context.Context) error {
		return probeAll(ctx, addrs)
	})
}

// waitForSchemaRegistry blocks until GET /subjects returns 200.
func (c *Cluster) waitForSchemaRegistry(ctx context.Context) error {
	client := &http.Client{Timeout: srProbeTimeout}
	url := SchemaRegistryURL + "/subjects"

	return poll(ctx, srReadyTimeout, srPollInterval, "schema registry", func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("get %s: %w", url, err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("get %s: unexpected status %d", url, resp.StatusCode)
		}
		return nil
	})
}
