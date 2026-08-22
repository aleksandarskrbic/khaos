// Command khaos generates Kafka traffic and simulates failure scenarios.
//
// The CLI surface is seven commands with stable flags, shorthands, defaults and exit
// codes (2 for a bad invocation, 1 for a command that ran and failed).
// cmd/khaos/cli_contract_test.go pins all of that, because a missing shorthand breaks a
// user's script silently. Errors and progress go to stderr rather than stdout, and
// whether a terminal UI runs is decided once, up front, from whether stdout is a TTY.
//
// Everything printed here goes through the cobra command's own writers rather than
// os.Stdout directly. In the binary those ARE os.Stdout and os.Stderr, so the output is
// identical; in tests it means nothing has to swap the process-wide streams, which is a
// data race as soon as any test runs in parallel.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

// version is stamped at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	// signal.NotifyContext gives graceful shutdown on the first signal and -- because the
	// stop function restores default handling -- a hard kill on the second.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := newRootCmd().ExecuteContext(ctx); err != nil {
		// Cobra has already printed usage errors; only report the rest, and only once.
		if !errors.Is(err, errSilent) {
			fmt.Fprintf(os.Stderr, "khaos: %v\n", err)
		}
		os.Exit(exitCode(err))
	}
}

// errSilent marks a failure whose message has already been reported, so main exits
// non-zero without printing a second, redundant line.
var errSilent = errors.New("khaos: already reported")

// usageError marks a bad invocation -- an unknown flag, an unknown command, the wrong
// number of positional arguments -- as opposed to a run that started and then failed.
// Scripts branch on the exit code, so the split between the two is deliberate and
// preserved.
type usageError struct{ err error }

func (e usageError) Error() string { return e.err.Error() }
func (e usageError) Unwrap() error { return e.err }

// exitCode maps an error to the process exit status: 2 for a bad invocation, 1 for
// everything else.
func exitCode(err error) int {
	var ue usageError
	if errors.As(err, &ue) {
		return 2
	}
	return 1
}

// usageArgs wraps a positional-argument validator so its failures exit 2 rather than 1.
func usageArgs(fn cobra.PositionalArgs) cobra.PositionalArgs {
	return func(c *cobra.Command, args []string) error {
		if err := fn(c, args); err != nil {
			return usageError{err}
		}
		return nil
	}
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "khaos",
		Short: "Kafka data generator and chaos-scenario simulator",
		Long: "khaos generates realistic Kafka traffic and simulates failure scenarios:\n" +
			"consumer lag, hot partitions, broker outages, rebalance storms and more.",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          usageArgs(cobra.NoArgs),
		// A bare `khaos` is a mistake, not a request for help: the help text goes to
		// stderr and the process exits 2, so stdout stays clean and `khaos > out.txt` in
		// a script fails loudly instead of writing help into the output.
		RunE: func(c *cobra.Command, _ []string) error {
			c.SetOut(c.ErrOrStderr())
			if err := c.Help(); err != nil {
				return err
			}
			return usageError{errSilent}
		},
	}

	// Unknown or malformed flags are Click UsageErrors too. FlagErrorFunc is inherited by
	// every subcommand, so setting it once on the root covers the whole tree.
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return usageError{err}
	})

	root.AddCommand(
		newRunCmd(),
		newSimulateCmd(),
		newListCmd(),
		newValidateCmd(),
		newClusterUpCmd(),
		newClusterDownCmd(),
		newClusterStatusCmd(),
	)
	setHelpStyle(root)
	return root
}
