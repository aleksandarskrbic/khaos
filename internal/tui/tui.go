// Package tui renders live run state in the terminal.
//
// The single most important property of this package is what it does NOT do: it never
// mutates engine state, never owns the run deadline, and never decides when the run stops.
// It polls engine.Snapshot on its own timer and draws whatever it gets, so a wedged UI
// shows stale numbers while the engine carries on unaffected.
package tui

import (
	"context"
	"errors"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/aleksandarskrbic/khaos/internal/engine"
)

// refreshInterval is how often the UI pulls a fresh snapshot and repaints. One clock
// drives both; decoupling the poll rate from the frame rate buys nothing.
const refreshInterval = 250 * time.Millisecond

// Source is the read side of the engine. Declared here rather than importing a concrete
// type so the TUI can be tested with a stub.
type Source interface {
	Snapshot() engine.Snapshot
}

type tickMsg time.Time

func tick() tea.Cmd {
	return tea.Tick(refreshInterval, func(t time.Time) tea.Msg { return tickMsg(t) })
}

// colGate is a three-state column switch: decided by config at startup, or -- when no
// Option was supplied -- inferred from live counters.
type colGate int8

const (
	gateAuto colGate = iota
	gateOn
	gateOff
)

func gate(on bool) colGate {
	if on {
		return gateOn
	}
	return gateOff
}

// Option configures the model at construction time.
type Option func(*model)

// ShowFailureColumns fixes the FAILED and DLQ columns on or off for the whole run, decided
// from scenario config so the table keeps one shape from the first frame. Inferring it from
// live counters instead means the columns pop in on the first simulated failure and shove
// every other column sideways mid-run.
func ShowFailureColumns(on bool) Option {
	return func(m *model) { m.failCols = gate(on) }
}

// ShowBrokerLag fixes the LAG(BROKER) column on or off for the whole run.
//
// Gated on --lag-poll at startup for the same reason as ShowFailureColumns: with polling
// enabled the column is there from the first frame, showing "unknown" until the first
// measurement lands, instead of appearing seconds into the run.
func ShowBrokerLag(on bool) Option {
	return func(m *model) { m.lagCols = gate(on) }
}

// sampleCap bounds the rate-history ring: 8 seconds at 4Hz. A fixed array rather than a
// slice so that copying the model -- which Bubble Tea does on every Update -- copies the
// history too, keeping Update pure without any copy-on-write bookkeeping.
const sampleCap = 32

// sample is one snapshot's counters, kept to derive rates by diffing against later ones.
// The engine measures totals only; msg/s and bytes/s exist purely in this package.
type sample struct {
	at                        time.Time
	produced, consumed, bytes int64
	topics                    map[string]topicSample
}

type topicSample struct {
	produced, consumed, bytes int64
}

type model struct {
	src    Source
	cancel context.CancelFunc
	snap   engine.Snapshot
	width  int
	height int

	failCols colGate
	lagCols  colGate

	// samples is a ring: samples[i%sampleCap] holds the i-th sample ever taken and
	// sampleN is the count taken, so the window survives indefinitely long runs.
	samples [sampleCap]sample
	sampleN int
}

// New builds the Bubble Tea model.
//
// cancel is how the user quits: pressing q or ctrl-c cancels the run context and the
// engine shuts down through its normal path. The UI never reaches into engine internals.
func New(src Source, cancel context.CancelFunc, opts ...Option) tea.Model {
	m := model{src: src, cancel: cancel, width: 100, height: 40}
	for _, o := range opts {
		o(&m)
	}
	return m
}

func (m model) Init() tea.Cmd { return tick() }

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tickMsg:
		// PULL. A value copy, never a pointer into the engine, so nothing here can race
		// the producers and consumers still running.
		m.snap = m.src.Snapshot()
		m = m.addSample(m.snap)
		return m, tick()

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case tea.KeyPressMsg:
		switch msg.String() {
		case "q", "ctrl+c", "esc":
			if m.cancel != nil {
				m.cancel()
			}
			return m, tea.Quit
		}
	}
	return m, nil
}

// addSample records the snapshot's counters for rate derivation. The maps stored here are
// built fresh each tick and never written again, so sharing them across model copies is
// safe; the array itself is copied with the model, which is what keeps Update pure.
func (m model) addSample(s engine.Snapshot) model {
	if s.At.IsZero() {
		return m
	}
	smp := sample{
		at:       s.At,
		produced: s.TotalProduced,
		consumed: s.TotalConsumed,
		topics:   make(map[string]topicSample, len(s.Topics)),
	}
	for _, t := range s.Topics {
		smp.topics[t.Topic] = topicSample{produced: t.Produced, consumed: t.Consumed, bytes: t.Bytes}
		smp.bytes += t.Bytes
	}
	// The same instant sampled twice (tests replaying a snapshot, a stalled engine clock)
	// overwrites in place rather than appending a zero-width interval.
	if m.sampleN > 0 && m.samples[(m.sampleN-1)%sampleCap].at.Equal(s.At) {
		m.samples[(m.sampleN-1)%sampleCap] = smp
		return m
	}
	m.samples[m.sampleN%sampleCap] = smp
	m.sampleN++
	return m
}

func (m model) View() tea.View {
	return tea.NewView(m.render())
}

// Run drives the Bubble Tea program until the UI exits or ctx is cancelled. Cancellation is
// delivered as a graceful Quit rather than tea.WithContext, because a context kill skips
// the final render and can tear the last frame; a 2-second Kill backstop still guarantees a
// wedged UI never holds the process hostage. The caller must NOT put this in the engine's
// errgroup: a failure here must not cancel the run (see internal/engine/run.go). opts exist
// so tests can supply an input and output that are not the real terminal.
func Run(ctx context.Context, src Source, cancel context.CancelFunc, uiOpts []Option, opts ...tea.ProgramOption) error {
	p := tea.NewProgram(New(src, cancel, uiOpts...), opts...)

	done := make(chan struct{})
	watcher := make(chan struct{})
	go func() {
		defer close(watcher)
		select {
		case <-done:
			return
		case <-ctx.Done():
		}
		// Quit is a no-op on a program that has not started yet, so keep asking until
		// the program is gone -- and past the grace period, kill it.
		grace := time.NewTimer(2 * time.Second)
		defer grace.Stop()
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			p.Quit()
			select {
			case <-done:
				return
			case <-grace.C:
				p.Kill()
				return
			case <-tick.C:
			}
		}
	}()

	_, err := p.Run()
	close(done)
	<-watcher
	if err != nil && !isCleanExit(err) {
		return err
	}
	return nil
}

// isCleanExit reports whether Bubble Tea stopped because it was asked to. Every khaos run
// ends with the run context cancelled, which tea.Program.Run reports as ErrProgramKilled
// wrapping context.Canceled -- returning that verbatim would print scary-looking noise to
// stderr after every successful run. A kill wrapping any other cause, above all
// ErrProgramPanic, is still a real error.
func isCleanExit(err error) bool {
	switch {
	case err == nil:
		return true
	case errors.Is(err, tea.ErrProgramPanic):
		return false
	case errors.Is(err, tea.ErrInterrupted):
		return true
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return true
	case err == tea.ErrProgramKilled:
		// Bubble Tea's own internal context, reported as the bare sentinel because there
		// is no cause to attach. Identity, not errors.Is: a killed program that wrapped a
		// real failure also matches the sentinel and must still be reported.
		return true
	}
	return false
}
