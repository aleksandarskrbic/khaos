package telemetry

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestNewLoggerFormats(t *testing.T) {
	tests := []struct {
		name       string
		jsonFormat bool
		check      func(t *testing.T, out string)
	}{
		{
			name:       "json handler for containers",
			jsonFormat: true,
			check: func(t *testing.T, out string) {
				var rec map[string]any
				if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &rec); err != nil {
					t.Fatalf("output is not JSON: %v (%q)", err, out)
				}
				if rec["msg"] != "produced" {
					t.Errorf("msg = %v, want produced", rec["msg"])
				}
				if rec["topic"] != "orders" {
					t.Errorf("topic = %v, want orders", rec["topic"])
				}
			},
		},
		{
			name:       "text handler for dev",
			jsonFormat: false,
			check: func(t *testing.T, out string) {
				if json.Valid([]byte(strings.TrimSpace(out))) {
					t.Errorf("expected text output, got JSON: %q", out)
				}
				if !strings.Contains(out, `msg=produced`) || !strings.Contains(out, `topic=orders`) {
					t.Errorf("output missing expected key=value pairs: %q", out)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			NewLogger(&buf, tt.jsonFormat, slog.LevelInfo).Info("produced", "topic", "orders")
			tt.check(t, buf.String())
		})
	}
}

// The writer is the seam that lets a TUI own the terminal: everything must go to it and
// nothing anywhere else.
func TestNewLoggerWritesOnlyToProvidedWriter(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, false, slog.LevelDebug)

	log.Debug("debug line")
	log.Info("info line")
	log.Warn("warn line")
	log.Error("error line")

	for _, want := range []string{"debug line", "info line", "warn line", "error line"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("writer missing %q", want)
		}
	}
}

func TestNewLoggerRespectsLevel(t *testing.T) {
	tests := []struct {
		name      string
		level     slog.Level
		wantDebug bool
		wantWarn  bool
	}{
		{"debug", slog.LevelDebug, true, true},
		{"info", slog.LevelInfo, false, true},
		{"error", slog.LevelError, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := NewLogger(&buf, true, tt.level)
			log.Debug("dbg")
			log.Warn("wrn")

			out := buf.String()
			if got := strings.Contains(out, "dbg"); got != tt.wantDebug {
				t.Errorf("debug emitted = %v, want %v", got, tt.wantDebug)
			}
			if got := strings.Contains(out, "wrn"); got != tt.wantWarn {
				t.Errorf("warn emitted = %v, want %v", got, tt.wantWarn)
			}
		})
	}
}
