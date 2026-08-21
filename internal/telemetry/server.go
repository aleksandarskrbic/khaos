package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// shutdownTimeout bounds the graceful drain once the run context is cancelled. Two
// endpoints serving in-memory data cannot legitimately need longer, and khaos must not
// hold up process exit after Ctrl-C.
const shutdownTimeout = 5 * time.Second

// Server exposes /healthz and /metrics.
//
// It is a normal errgroup member: Run blocks for the lifetime of the process and returns
// when the context is cancelled, so the caller wires it up exactly like every other
// long-running component and does not have to remember a separate Close.
type Server struct {
	addr   string
	reg    *prometheus.Registry
	health func() error

	mu    sync.Mutex
	bound string // the resolved listen address, known only once Run has listened
}

// NewServer builds the telemetry server.
//
// An empty addr disables it: Run then returns nil immediately without listening, so the
// CLI turns telemetry off by passing "" rather than by threading a bool through.
// health may be nil, in which case /healthz always reports healthy.
func NewServer(addr string, reg *prometheus.Registry, health func() error) *Server {
	return &Server{addr: addr, reg: reg, health: health}
}

// Addr returns the address the server is listening on.
//
// Before Run has bound a listener this is the configured address, which may carry a
// wildcard port (":0"); afterwards it is the resolved one. That distinction matters to
// tests and to anyone logging where to point a browser.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bound != "" {
		return s.bound
	}
	return s.addr
}

// Run serves until ctx is cancelled, then drains and returns.
//
// A cancelled context is the expected way to stop, so it is not reported as an error;
// anything else -- a port already bound, a listener dying -- is.
func (s *Server) Run(ctx context.Context) error {
	if s.addr == "" {
		return nil
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("telemetry listen on %s: %w", s.addr, err)
	}

	s.mu.Lock()
	s.bound = ln.Addr().String()
	s.mu.Unlock()

	srv := &http.Server{
		Handler:           s.handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		// Serve always returns a non-nil error; ErrServerClosed is the clean path.
		serveErr <- srv.Serve(ln)
	}()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("telemetry serve: %w", err)

	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("telemetry shutdown: %w", err)
		}
		// Drain Serve's return so the goroutine cannot outlive Run.
		if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("telemetry serve: %w", err)
		}
		return nil
	}
}

// handler builds the mux. Two endpoints do not need a router library; http.ServeMux is
// the whole requirement.
func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")

		if s.health != nil {
			if err := s.health(); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprintf(w, "unhealthy: %v\n", err)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})

	if s.reg != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{
			// A scrape must never take khaos down, and the error text belongs in the
			// scrape response where the operator will actually see it.
			ErrorHandling: promhttp.ContinueOnError,
		}))
	}

	return mux
}
