package tui

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/aleksandarskrbic/khaos/internal/engine"
	"github.com/aleksandarskrbic/khaos/internal/scenario"
	"go.uber.org/goleak"
)

// The TUI owns a ticker and, in Run, a whole Bubble Tea program. A Run that returns while
// the program's goroutines are still up would mean cmd/khaos joins on outDone and then
// exits with the terminal still being written to.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// stubSource is the engine's read side, canned.
//
// It exists to prove the architectural claim in the package doc: the TUI needs nothing
// from the engine but Snapshot(), so the whole component tests with no broker, no run and
// no terminal. It is mutex-guarded because Run drives Update from Bubble Tea's own
// goroutine while the test reads the counters.
type stubSource struct {
	mu    sync.Mutex
	calls int
	snaps []engine.Snapshot // consumed in order; the last one repeats forever
}

func newStubSource(snaps ...engine.Snapshot) *stubSource {
	if len(snaps) == 0 {
		snaps = []engine.Snapshot{{}}
	}
	return &stubSource{snaps: snaps}
}

func (s *stubSource) Snapshot() engine.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := min(s.calls, len(s.snaps)-1)
	s.calls++
	return s.snaps[i]
}

func (s *stubSource) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// sampleSnapshot is a realistic run: two topics, one with two consumer groups and one with
// failure simulation, a flow, and a few events.
func sampleSnapshot() engine.Snapshot {
	at := time.Date(2026, 3, 1, 12, 30, 15, 0, time.UTC)
	return engine.Snapshot{
		At:       at,
		Elapsed:  95 * time.Second,
		Deadline: 5 * time.Minute,
		Scenario: "black-friday",
		Topics: []engine.TopicStat{
			{
				Topic:    "orders",
				Produced: 12345,
				Consumed: 12000,
				Lag:      345,
				Groups: []engine.GroupStat{
					{GroupID: "order-processors", Consumers: 3, Consumed: 9000, Paused: 1},
					{GroupID: "order-auditors", Consumers: 1, Consumed: 3000},
				},
			},
			{
				Topic:    "payments",
				Produced: 500,
				Consumed: 498,
				Lag:      2,
				Failed:   7,
				DLQ:      3,
				Retries:  11,
				Groups:   []engine.GroupStat{{GroupID: "payment-settlers", Consumed: 498}},
			},
		},
		Flows: []engine.FlowStat{
			{Name: "checkout", Started: 40, Completed: 38, Messages: 114, InFlight: 2, Saturated: 5},
		},
		Events: []engine.Event{
			{At: at, Message: "scenario started", Level: scenario.EventInfo},
			{At: at, Message: ">>> INCIDENT: Stopping kafka-2", Level: scenario.EventAlert},
		},
		TotalProduced: 12845,
		TotalConsumed: 12498,
		TotalErrors:   4,
		Rebalances:    2,
		Healthy:       true,
	}
}

func keyPress(s string) tea.KeyPressMsg {
	switch s {
	case "ctrl+c":
		return tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl}
	case "esc":
		return tea.KeyPressMsg{Code: tea.KeyEscape}
	default:
		r := []rune(s)
		return tea.KeyPressMsg{Code: r[0], Text: s}
	}
}

// Source must stay a one-method interface: the moment the TUI can reach a second engine
// method it can also mutate a run, which this package is deliberately built to prevent.
func TestSourceIsTheEntireEngineContract(t *testing.T) {
	typ := reflect.TypeOf((*Source)(nil)).Elem()
	if got := typ.NumMethod(); got != 1 {
		var names []string
		for i := range got {
			names = append(names, typ.Method(i).Name)
		}
		t.Fatalf("Source has %d methods (%v), want exactly Snapshot", got, names)
	}
	if got := typ.Method(0).Name; got != "Snapshot" {
		t.Fatalf("Source's only method is %q, want Snapshot", got)
	}

	// And the real engine still satisfies it.
	var _ Source = (*engine.Engine)(nil)
}

func TestInitStartsTheTickerWithoutPulling(t *testing.T) {
	src := newStubSource(sampleSnapshot())
	m := New(src, func() {})

	if cmd := m.Init(); cmd == nil {
		t.Fatal("Init returned no command, so the UI would never refresh")
	}
	if got := src.Calls(); got != 0 {
		t.Fatalf("Init pulled %d snapshots, want 0 -- the first pull belongs to the first tick", got)
	}
}

func TestTickPullsAFreshSnapshotAndRendersIt(t *testing.T) {
	first := sampleSnapshot()
	second := sampleSnapshot()
	second.Topics[0].Produced = 99999
	second.TotalProduced = 100499

	src := newStubSource(first, second)
	m := New(src, func() {})

	m, cmd := m.Update(tickMsg(time.Now()))
	if cmd == nil {
		t.Fatal("tick did not schedule the next tick; the UI would freeze after one frame")
	}
	if got := src.Calls(); got != 1 {
		t.Fatalf("Snapshot called %d times, want 1 per tick", got)
	}
	if out := plain(m.View()); !strings.Contains(out, "12,345") {
		t.Fatalf("first frame does not show the first snapshot's numbers:\n%s", out)
	}

	m, _ = m.Update(tickMsg(time.Now()))
	out := plain(m.View())
	if !strings.Contains(out, "99,999") {
		t.Fatalf("second frame did not pick up the fresh snapshot:\n%s", out)
	}
	if strings.Contains(out, "12,345") {
		t.Fatalf("second frame still shows the stale snapshot:\n%s", out)
	}
	if got := src.Calls(); got != 2 {
		t.Fatalf("Snapshot called %d times, want one pull per tick", got)
	}
}

func TestQuitKeysCancelTheRun(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		wantCancel bool
	}{
		{"q quits", "q", true},
		{"ctrl+c quits", "ctrl+c", true},
		{"esc quits", "esc", true},
		{"shift+q does not", "Q", false},
		{"an ordinary key does not", "a", false},
		{"space does not", " ", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cancels int
			m := New(newStubSource(sampleSnapshot()), func() { cancels++ })

			_, cmd := m.Update(keyPress(tt.key))

			if !tt.wantCancel {
				if cancels != 0 {
					t.Errorf("%q cancelled the run", tt.key)
				}
				if cmd != nil {
					t.Errorf("%q produced a command, want none", tt.key)
				}
				return
			}
			if cancels != 1 {
				t.Errorf("%q called cancel %d times, want exactly 1", tt.key, cancels)
			}
			if cmd == nil {
				t.Fatalf("%q returned no command, want tea.Quit", tt.key)
			}
			if _, ok := cmd().(tea.QuitMsg); !ok {
				t.Errorf("%q returned %T, want tea.QuitMsg", tt.key, cmd())
			}
		})
	}
}

// New tolerates a nil cancel func rather than panicking on the quit key; a caller that
// wants a read-only dashboard has no run to cancel.
func TestQuitWithNilCancel(t *testing.T) {
	m := New(newStubSource(sampleSnapshot()), nil)
	_, cmd := m.Update(keyPress("q"))
	if cmd == nil {
		t.Fatal("want tea.Quit even without a cancel func")
	}
	if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Fatalf("got %T, want tea.QuitMsg", cmd())
	}
}

// The single most important property of the whole package: the snapshot handed over is
// read-only. If the TUI wrote to it, it would be writing into memory the engine handed out
// while producers and consumers keep running.
func TestModelNeverMutatesTheSnapshot(t *testing.T) {
	original := sampleSnapshot()
	handed := sampleSnapshot() // an identical, independent copy for the stub to hand over

	src := newStubSource(handed)
	m := New(src, func() {})

	m.Init()
	for range 5 {
		m, _ = m.Update(tickMsg(time.Now()))
		m.View()
		m, _ = m.Update(tea.WindowSizeMsg{Width: 40, Height: 10})
		m.View()
	}
	m, _ = m.Update(keyPress("a"))
	m.View()

	if !reflect.DeepEqual(handed, original) {
		t.Fatalf("the model mutated the snapshot it was given:\ngot  %+v\nwant %+v", handed, original)
	}
}

// Update must be a pure function of state: the same message against the same model twice
// has to produce the same view, so a repaint can never disagree with the frame before it.
func TestUpdateIsPure(t *testing.T) {
	snap := sampleSnapshot()
	base := New(newStubSource(snap, snap, snap), func() {})

	a, _ := base.Update(tickMsg(time.Now()))
	b, _ := base.Update(tickMsg(time.Now()))

	if plain(a.View()) != plain(b.View()) {
		t.Fatal("the same message against the same model produced two different views")
	}
	// ...and the receiver was not modified in place.
	if got := plain(base.View()); strings.Contains(got, "black-friday") {
		t.Fatal("Update mutated its receiver instead of returning a new model")
	}
}

func TestRunReturnsNilWhenTheContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- runHeadless(ctx, newStubSource(sampleSnapshot()), func() {}, &bytes.Buffer{}) }()

	// Give the program a moment to start before pulling the rug out.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil: every khaos run ends by cancelling this context", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

// Pressing q cancels the run context from inside Update, which means Run sees exactly the
// same "killed" error as the timed path. A user quitting cleanly must not produce an error
// on stderr either.
func TestRunReturnsNilWhenTheUserQuits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var cancelled bool
	var mu sync.Mutex
	src := newStubSource(sampleSnapshot())

	in := bytes.NewBufferString("q")
	err := Run(ctx, src, func() {
		mu.Lock()
		cancelled = true
		mu.Unlock()
		cancel()
	}, nil, tea.WithInput(in), tea.WithOutput(io.Discard), tea.WithoutSignalHandler())

	if err != nil {
		t.Fatalf("Run = %v, want nil after the user pressed q", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !cancelled {
		t.Fatal("q did not cancel the run context")
	}
}

// runHeadless is Run wired to a dead input and a throwaway output, so tests never touch
// the real terminal.
func runHeadless(ctx context.Context, src Source, cancel context.CancelFunc, out io.Writer) error {
	return Run(ctx, src, cancel, nil,
		tea.WithInput(&bytes.Buffer{}),
		tea.WithOutput(out),
		tea.WithoutSignalHandler(),
	)
}

// killed reproduces exactly how Bubble Tea wraps a cause into ErrProgramKilled
// (bubbletea/v2 tea.go:1150-1163). The double-%w form matters: errors.Unwrap returns nil
// for it, so any classification that leaned on Unwrap would misread every one of these.
func killed(cause error) error {
	return fmt.Errorf("%w: %w", tea.ErrProgramKilled, cause)
}

func TestIsCleanExit(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"no error", nil, true},
		{"bare kill", tea.ErrProgramKilled, true},
		{"kill wrapping our cancellation", killed(context.Canceled), true},
		{"kill wrapping a deadline", killed(context.DeadlineExceeded), true},
		{"sigint", tea.ErrInterrupted, true},
		{"kill wrapping sigint", killed(tea.ErrInterrupted), true},
		{"kill wrapping a panic", killed(tea.ErrProgramPanic), false},
		{"bare panic", tea.ErrProgramPanic, false},
		{"a real failure", errors.New("could not open tty"), false},
		{"kill wrapping a real failure", killed(errors.New("could not open tty")), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isCleanExit(tt.err); got != tt.want {
				t.Fatalf("isCleanExit(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
