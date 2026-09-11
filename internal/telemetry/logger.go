// Package telemetry provides khaos's structured logger, its Prometheus metric set and the
// small HTTP server that exposes /healthz and /metrics.
//
// The three are independent and wired up separately by cmd/khaos. NewLogger (logger.go)
// stands alone, NewMetrics (metrics.go) registers the whole counter set against a registry
// the caller owns, and NewServer (server.go) needs that registry plus a health func. A run
// without --metrics-addr builds the logger and neither of the other two, so an unscraped
// metric set costs a run nothing.
//
// Nothing in this package ever touches os.Stdout or os.Stderr. The caller supplies the
// writer, and under a bubbletea TUI that writer is a file or a pipe, never the terminal.
package telemetry

import (
	"io"
	"log/slog"
)

// NewLogger builds the process logger.
//
// jsonFormat picks between the two handlers: JSON for containers and log shippers, text
// for a human at a terminal -- exactly two, chosen by a bool, since slog already provides
// both handlers and wrapping them in another abstraction would buy nothing.
//
// w must not be os.Stdout when a TUI is running -- see the package doc.
func NewLogger(w io.Writer, jsonFormat bool, level slog.Level) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}

	var h slog.Handler
	if jsonFormat {
		h = slog.NewJSONHandler(w, opts)
	} else {
		h = slog.NewTextHandler(w, opts)
	}
	return slog.New(h)
}
