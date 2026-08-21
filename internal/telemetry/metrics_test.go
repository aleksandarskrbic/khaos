package telemetry

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// gather renders a registry as a sorted set of "name{label=value,...} value" lines,
// optionally restricted to the named metric families.
//
// The filter matters because a plain (non-Vec) Counter exports a zero-valued series from
// the moment it is registered, so khaos_scenario_transitions_total shows up in every
// gather whether or not the test is about it.
//
// prometheus/client_golang ships a testutil package that would do this, but it pulls in a
// module that is not in go.mod, so the few lines it saves are not worth the dependency.
func gather(t *testing.T, reg *prometheus.Registry, only ...string) []string {
	t.Helper()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	wanted := make(map[string]bool, len(only))
	for _, n := range only {
		wanted[n] = true
	}

	var lines []string
	for _, f := range families {
		if len(wanted) > 0 && !wanted[f.GetName()] {
			continue
		}
		for _, m := range f.GetMetric() {
			var labels []string
			for _, l := range m.GetLabel() {
				labels = append(labels, fmt.Sprintf("%s=%q", l.GetName(), l.GetValue()))
			}
			var value float64
			switch {
			case m.GetCounter() != nil:
				value = m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				value = m.GetGauge().GetValue()
			default:
				t.Fatalf("metric %s has an unexpected type", f.GetName())
			}
			lines = append(lines, fmt.Sprintf("%s{%s} %g", f.GetName(), strings.Join(labels, ","), value))
		}
	}
	sort.Strings(lines)
	return lines
}

func familyNames(t *testing.T, reg *prometheus.Registry) []string {
	t.Helper()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	sort.Strings(names)
	return names
}

func TestNewMetricsRegistersTheWholeSet(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	// Touch every collector so each one produces at least one series.
	m.GeneratedMessages.WithLabelValues("orders").Inc()
	m.GeneratedBytes.WithLabelValues("orders").Add(512)
	m.ProduceErrors.WithLabelValues("orders").Inc()
	m.ConsumedMessages.WithLabelValues("orders", "billing").Inc()
	m.ConsumeErrors.WithLabelValues("orders", "billing").Inc()
	m.Reconnects.WithLabelValues("kafka-1").Inc()
	m.Rebalances.WithLabelValues("billing").Inc()
	m.ScenarioTransitions.Inc()
	m.SetPhase("payment-spike", "ramp-up")

	want := []string{
		"khaos_bytes_generated_total",
		"khaos_consume_errors_total",
		"khaos_current_phase",
		"khaos_messages_consumed_total",
		"khaos_messages_generated_total",
		"khaos_produce_errors_total",
		"khaos_rebalances_total",
		"khaos_reconnects_total",
		"khaos_scenario_transitions_total",
	}
	got := familyNames(t, reg)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("metric families =\n  %v\nwant\n  %v", got, want)
	}
}

func TestCounterValues(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.GeneratedMessages.WithLabelValues("orders").Add(3)
	m.GeneratedMessages.WithLabelValues("payments").Add(2)
	m.GeneratedBytes.WithLabelValues("orders").Add(1024)
	m.ConsumedMessages.WithLabelValues("orders", "billing").Add(5)
	m.ScenarioTransitions.Add(2)

	want := []string{
		`khaos_bytes_generated_total{topic="orders"} 1024`,
		`khaos_messages_consumed_total{group="billing",topic="orders"} 5`,
		`khaos_messages_generated_total{topic="orders"} 3`,
		`khaos_messages_generated_total{topic="payments"} 2`,
		`khaos_scenario_transitions_total{} 2`,
	}
	got := gather(t, reg)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("series =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// SetPhase must leave exactly one series behind, otherwise every phase a run passed
// through would still report 1 and the "current" gauge would describe the whole history.
func TestSetPhaseKeepsOnlyTheCurrentSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.SetPhase("payment-spike", "ramp-up")
	m.SetPhase("payment-spike", "steady")

	want := []string{`khaos_current_phase{phase="steady",scenario="payment-spike"} 1`}
	got := gather(t, reg, "khaos_current_phase")
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("series =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// Two Metrics on two registries must not collide, which is what taking an explicit
// registry instead of the default one buys.
func TestNewMetricsIsIndependentPerRegistry(t *testing.T) {
	regA, regB := prometheus.NewRegistry(), prometheus.NewRegistry()
	a := NewMetrics(regA)
	NewMetrics(regB)

	a.GeneratedMessages.WithLabelValues("orders").Inc()

	const series = "khaos_messages_generated_total"
	if got := gather(t, regA, series); len(got) != 1 || got[0] != `khaos_messages_generated_total{topic="orders"} 1` {
		t.Errorf("registry a = %v", got)
	}
	if got := gather(t, regB, series); len(got) != 0 {
		t.Errorf("registry b = %v, want no series", got)
	}
}
