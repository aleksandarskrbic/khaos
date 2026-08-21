package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

// The CLI surface is a compatibility contract: these flags and shorthands are what the
// README documents, so scripts in the wild depend on them.
func TestDurationAcceptsSecondsAndDurations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"60", 60 * time.Second, false},  // bare seconds
		{"0", 0, false},                  // 0 = run until Ctrl+C
		{"90s", 90 * time.Second, false}, // Go duration
		{"10m", 10 * time.Minute, false},
		{"1h30m", 90 * time.Minute, false},
		{"-5", 0, true},
		{"soon", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			var d durationValue
			err := d.Set(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Set(%q) succeeded, want an error", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("Set(%q): %v", tt.in, err)
			}
			if time.Duration(d) != tt.want {
				t.Errorf("Set(%q) = %v, want %v", tt.in, time.Duration(d), tt.want)
			}
		})
	}
}

// --duration defaults to 0, meaning run until interrupted. A non-zero default would
// silently stop long-running daemon workloads after a minute.
func TestDurationDefaultsToZero(t *testing.T) {
	t.Parallel()
	cmd := newRunCmd()
	if got := cmd.Flags().Lookup("duration").DefValue; got != "0" {
		t.Errorf("--duration default = %q, want %q", got, "0")
	}
}

// Every documented flag, shorthand and default must still exist on every command it
// belongs to. A missing shorthand breaks a script silently at the worst moment.
func TestFlagContractPreserved(t *testing.T) {
	t.Parallel()

	type flagSpec struct {
		short   string
		defVal  string // "" means "not asserted"
		command string
	}

	cases := map[string]map[string]flagSpec{
		"run": {
			"duration":          {short: "d", defVal: "0"},
			"keep-cluster":      {short: "k", defVal: "false"},
			"bootstrap-servers": {short: "b"},
			"mode":              {short: "m", defVal: "kraft"},
			"no-consumers":      {defVal: "false"},
		},
		"simulate": {
			"bootstrap-servers":   {short: "b"},
			"duration":            {short: "d", defVal: "0"},
			"security-protocol":   {defVal: "PLAINTEXT"},
			"sasl-mechanism":      {},
			"sasl-username":       {},
			"sasl-password":       {},
			"ssl-ca-location":     {},
			"ssl-cert-location":   {},
			"ssl-key-location":    {},
			"ssl-key-password":    {},
			"skip-topic-creation": {defVal: "false"},
			"no-consumers":        {defVal: "false"},
			"schema-registry-url": {},
		},
		// cluster-up takes --mode/-m, not a --kraft boolean.
		"cluster-up": {
			"mode": {short: "m", defVal: "kraft"},
		},
		// cluster-down takes --volumes/-v.
		"cluster-down": {
			"volumes": {short: "v", defVal: "false"},
		},
	}

	byName := map[string]*cobra.Command{}
	for _, c := range newRootCmd().Commands() {
		byName[c.Name()] = c
	}

	for cmdName, flags := range cases {
		cmd := byName[cmdName]
		if cmd == nil {
			t.Errorf("missing command %q", cmdName)
			continue
		}
		for name, spec := range flags {
			fl := cmd.Flags().Lookup(name)
			if fl == nil {
				t.Errorf("%s: missing --%s", cmdName, name)
				continue
			}
			if fl.Shorthand != spec.short {
				t.Errorf("%s: --%s shorthand = %q, want %q",
					cmdName, name, fl.Shorthand, spec.short)
			}
			if spec.defVal != "" && fl.DefValue != spec.defVal {
				t.Errorf("%s: --%s default = %q, want %q",
					cmdName, name, fl.DefValue, spec.defVal)
			}
		}
	}
}

// Every command must exist under a stable name; scripts depend on the name never changing.
func TestCommandSetPreserved(t *testing.T) {
	t.Parallel()
	have := map[string]bool{}
	for _, c := range newRootCmd().Commands() {
		have[c.Name()] = true
	}
	for _, name := range []string{
		"cluster-up", "cluster-down", "cluster-status", "list", "validate", "run", "simulate",
	} {
		if !have[name] {
			t.Errorf("missing command %q", name)
		}
	}
}

// A LIST of scenarios runs together in one invocation.
func TestMultipleScenariosRunTogether(t *testing.T) {
	t.Parallel()
	brokers := fakeBrokers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	out, err := runCLI(t, ctx, "simulate",
		"traffic/high-throughput", "traffic/consumer-lag",
		"--bootstrap-servers", brokers,
		"-d", runDuration,
		"--tui", "off",
		"--log-level", "error",
	)
	if err != nil {
		t.Fatalf("two scenarios together: %v\n%s", err, out)
	}
}

// Both commands must reject an empty scenario list, exiting 1: a command-level failure,
// not a usage error.
func TestScenarioRequired(t *testing.T) {
	t.Parallel()
	_, err := runCLI(t, context.Background(), "run")
	if err == nil {
		t.Fatal("run with no scenario should fail")
	}
	if got := exitCode(err); got != 1 {
		t.Errorf("run with no scenario exited %d, want 1", got)
	}

	_, err = runCLI(t, context.Background(), "simulate", "--bootstrap-servers", "x:9092")
	if err == nil {
		t.Fatal("simulate with no scenario should fail")
	}
	if got := exitCode(err); got != 1 {
		t.Errorf("simulate with no scenario exited %d, want 1", got)
	}
}

// --bootstrap-servers is checked BEFORE the scenario list, so `khaos simulate` with
// neither reports the missing broker list first. Reporting the scenario instead would
// send the user to fix the wrong thing.
func TestSimulateReportsMissingBootstrapFirst(t *testing.T) {
	t.Parallel()
	_, err := runCLI(t, context.Background(), "simulate")
	if err == nil {
		t.Fatal("simulate with no arguments should fail")
	}
	if !strings.Contains(err.Error(), "bootstrap-servers") {
		t.Errorf("error was %q, want it to name --bootstrap-servers", err)
	}
}

// Exit code 2 marks a bad invocation, 1 marks a failure inside a command that ran.
// Scripts branch on that, so both halves are pinned.
func TestExitCodes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
		want int
	}{
		{"no arguments at all", nil, 2},
		{"unknown command", []string{"bogus"}, 2},
		{"unknown flag", []string{"run", "--nope"}, 2},
		{"extra positional on list", []string{"list", "extra"}, 2},
		{"invalid duration", []string{"run", "x", "-d", "soon"}, 2},
		{"missing scenario", []string{"run"}, 1},
		{"unknown scenario", []string{"validate", "no/such/scenario"}, 1},
		{"invalid mode", []string{"run", "traffic/high-throughput", "-m", "zookeper"}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := runCLI(t, context.Background(), tt.args...)
			if err == nil {
				t.Fatalf("%v succeeded, want a failure", tt.args)
			}
			if got := exitCode(err); got != tt.want {
				t.Errorf("%v exited %d, want %d (%v)", tt.args, got, tt.want, err)
			}
		})
	}
}

// A successful invocation must never look like a usage error.
func TestSuccessIsNotAUsageError(t *testing.T) {
	t.Parallel()
	if _, err := runCLI(t, context.Background(), "list"); err != nil {
		t.Fatalf("list: %v", err)
	}
	if _, err := runCLI(t, context.Background(), "--help"); err != nil {
		t.Fatalf("--help: %v", err)
	}
}

// runCLISplit is runCLI with the two streams kept apart, for asserting which one a message
// lands on.
func runCLISplit(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	cmd := newRootCmd()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(context.Background())
	return out.String(), errOut.String(), err
}

// What goes on which stream is a contract for anyone piping khaos: stdout carries the
// answer, stderr carries everything else. Asking for help gets it on stdout; being told
// you asked wrongly gets it on stderr.
func TestStreamDiscipline(t *testing.T) {
	t.Parallel()

	t.Run("--help goes to stdout", func(t *testing.T) {
		t.Parallel()
		out, errOut, err := runCLISplit(t, "--help")
		if err != nil {
			t.Fatalf("--help: %v", err)
		}
		if !strings.Contains(out, "Available Commands") {
			t.Errorf("help did not reach stdout\n%s", out)
		}
		if errOut != "" {
			t.Errorf("help wrote to stderr as well: %q", errOut)
		}
	})

	t.Run("bare invocation goes to stderr", func(t *testing.T) {
		t.Parallel()
		out, errOut, err := runCLISplit(t)
		if err == nil {
			t.Fatal("a bare invocation should fail")
		}
		if out != "" {
			t.Errorf("a bare invocation wrote to stdout: %q", out)
		}
		if !strings.Contains(errOut, "Available Commands") {
			t.Errorf("no help on stderr\n%s", errOut)
		}
	})

	t.Run("list goes to stdout", func(t *testing.T) {
		t.Parallel()
		out, _, err := runCLISplit(t, "list")
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if !strings.Contains(out, "Available Scenarios") {
			t.Errorf("list did not reach stdout\n%s", out)
		}
	})

	t.Run("validation errors go to stderr", func(t *testing.T) {
		t.Parallel()
		bad := writeScenario(t, "name: broken\ntopics:\n  - name: orders\n    partitions: -3\n")
		out, errOut, err := runCLISplit(t, "validate", bad)
		if err == nil {
			t.Fatal("invalid scenario should fail")
		}
		if !strings.Contains(errOut, "partitions") {
			t.Errorf("the error did not reach stderr\n%s", errOut)
		}
		if strings.Contains(out, "partitions") {
			t.Errorf("the error leaked into stdout\n%s", out)
		}
	})
}

// `khaos --version` and `khaos -v` both work and exit 0; the homebrew formula's test block
// asserts the version string appears in that output.
func TestVersionFlag(t *testing.T) {
	t.Parallel()
	for _, flag := range []string{"--version", "-v"} {
		out, _, err := runCLISplit(t, flag)
		if err != nil {
			t.Fatalf("%s: %v", flag, err)
		}
		if !strings.Contains(out, version) {
			t.Errorf("%s printed %q, want it to contain %q", flag, out, version)
		}
	}
}

// --mode is validated against a fixed set of values, so a typo is rejected rather than
// silently falling back to KRaft.
func TestModeIsValidated(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in      string
		kraft   bool
		wantErr bool
	}{
		{"kraft", true, false},
		{"KRaft", true, false},
		{"zookeeper", false, false},
		{"ZooKeeper", false, false},
		{"zookeper", false, true},
		{"", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			kraft, err := parseMode(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseMode(%q) succeeded, want an error", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseMode(%q): %v", tt.in, err)
			}
			if kraft != tt.kraft {
				t.Errorf("parseMode(%q) = %v, want %v", tt.in, kraft, tt.kraft)
			}
		})
	}
}

// --tui and --log-level used to accept anything and silently mean "auto" and "info". A
// typo that changes what you see must say so.
func TestOutputFlagsAreValidated(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		f       runFlags
		wantErr bool
	}{
		{"defaults", runFlags{tuiMode: "auto", logLevel: "info"}, false},
		{"tui off, debug", runFlags{tuiMode: "off", logLevel: "debug"}, false},
		{"tui on, warn", runFlags{tuiMode: "on", logLevel: "warn"}, false},
		{"typo in tui", runFlags{tuiMode: "of", logLevel: "info"}, true},
		{"typo in level", runFlags{tuiMode: "off", logLevel: "trace"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := tt.f.resolve()
			if (err != nil) != tt.wantErr {
				t.Errorf("resolve() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// `khaos validate` with no arguments validates the whole bundled corpus, which is what
// makes it usable as a CI gate.
func TestValidateAllBundledScenarios(t *testing.T) {
	t.Parallel()
	out, err := runCLI(t, context.Background(), "validate")
	if err != nil {
		t.Fatalf("validate with no arguments: %v\n%s", err, out)
	}
	if !strings.Contains(out, "traffic/high-throughput: valid") {
		t.Errorf("validate did not cover the bundled corpus\n%s", out)
	}
	if !strings.Contains(out, "scenario(s) valid") {
		t.Errorf("validate printed no tally\n%s", out)
	}
}

// Several scenario names can be validated in one invocation.
func TestValidateAcceptsSeveralScenarios(t *testing.T) {
	t.Parallel()
	out, err := runCLI(t, context.Background(), "validate",
		"traffic/high-throughput", "traffic/consumer-lag", "traffic/hot-partition")
	if err != nil {
		t.Fatalf("validate with three scenarios: %v\n%s", err, out)
	}
	if got := strings.Count(out, ": valid"); got != 3 {
		t.Errorf("reported %d scenarios, want 3\n%s", got, out)
	}
}

// One bad scenario in a batch fails the whole batch, but the good ones are still reported
// so a single run tells you everything.
func TestValidateReportsEveryScenarioInABatch(t *testing.T) {
	t.Parallel()
	bad := writeScenario(t, "name: broken\ntopics:\n  - name: orders\n    partitions: -3\n")

	out, err := runCLI(t, context.Background(), "validate", bad, "traffic/high-throughput")
	if err == nil {
		t.Fatalf("a batch containing an invalid scenario should fail\n%s", out)
	}
	if got := exitCode(err); got != 1 {
		t.Errorf("exited %d, want 1", got)
	}
	if !strings.Contains(out, "traffic/high-throughput: valid") {
		t.Errorf("the valid scenario was not reported\n%s", out)
	}
	if !strings.Contains(out, "partitions") {
		t.Errorf("the invalid scenario's error was not reported\n%s", out)
	}
}

// errSilent means "already reported"; it must never reach the user as a second line.
func TestSilentErrorsAreNotPrintedTwice(t *testing.T) {
	t.Parallel()
	bad := writeScenario(t, "name: broken\ntopics:\n  - name: orders\n    partitions: -3\n")

	_, err := runCLI(t, context.Background(), "validate", bad)
	if err == nil {
		t.Fatal("invalid scenario should fail")
	}
	if !errors.Is(err, errSilent) {
		t.Errorf("error %v does not wrap errSilent, so main would print it again", err)
	}
}

// writeScenario drops a scenario file in the test's own temp directory.
func writeScenario(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scenario.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write scenario: %v", err)
	}
	return path
}
