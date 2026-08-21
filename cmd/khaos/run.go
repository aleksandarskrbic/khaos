package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aleksandarskrbic/khaos/internal/codec"
	"github.com/aleksandarskrbic/khaos/internal/engine"
	"github.com/aleksandarskrbic/khaos/internal/kafka"
	"github.com/aleksandarskrbic/khaos/internal/localcluster"
	"github.com/aleksandarskrbic/khaos/internal/scenario"
	"github.com/aleksandarskrbic/khaos/internal/telemetry"
	"github.com/aleksandarskrbic/khaos/internal/tui"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// runFlags are shared by `run` and `simulate`.
//
// `run` targets the bundled Docker cluster and `simulate` targets an external one, so
// only `simulate` carries the security flags. That split is the documented interface.
type runFlags struct {
	duration          durationValue
	noConsumers       bool
	recreateTopics    bool
	skipTopicCreation bool
	schemaRegistryURL string
	srUsername        string
	srPassword        string
	srBearerToken     string
	srCALocation      string
	srCertLocation    string
	srKeyLocation     string
	lagPoll           time.Duration
	seed              uint64

	tuiMode  string // auto | on | off
	logJSON  bool
	logLevel string

	metricsAddr string
}

func (f *runFlags) bind(c *cobra.Command) {
	// --duration/-d accepts a bare integer of seconds (DEFAULT 0, meaning "run until
	// Ctrl-C") or a Go duration string like "10m", so `-d 60` and `-d 90s` both work; see
	// durationValue below.
	c.Flags().VarP(&f.duration, "duration", "d",
		"duration in seconds, or a Go duration like 90s/10m (0 = run until Ctrl+C)")
	c.Flags().BoolVar(&f.noConsumers, "no-consumers", false, "produce only, start no consumers")
	// Unlike --recreate-topics=false, this skips topic administration entirely -- nothing
	// is created and nothing is deleted -- so a user without CreateTopics permission on a
	// managed cluster can still run.
	c.Flags().BoolVar(&f.skipTopicCreation, "skip-topic-creation", false,
		"do not create or delete any topics; assume they already exist")
	// Deletes and recreates every topic on every run by default. Destructive against a
	// cluster khaos does not own, so it can be turned off.
	c.Flags().BoolVar(&f.recreateTopics, "recreate-topics", kafka.RecreateTopicsByDefault,
		"delete and recreate topics before running, discarding their data (destructive)")
	c.Flags().StringVar(&f.schemaRegistryURL, "schema-registry-url", "", "Schema Registry URL")
	// Schema Registry credentials, additive: omit them all and a plain URL still works,
	// but they let a secured registry (Confluent Cloud, Aiven) be reached too.
	c.Flags().StringVar(&f.srUsername, "schema-registry-username", "",
		"Schema Registry basic-auth user (on Confluent Cloud, the SR API key)")
	c.Flags().StringVar(&f.srPassword, "schema-registry-password", "",
		"Schema Registry basic-auth password (on Confluent Cloud, the SR API secret)")
	c.Flags().StringVar(&f.srBearerToken, "schema-registry-token", "",
		"Schema Registry bearer token, instead of basic auth")
	c.Flags().StringVar(&f.srCALocation, "schema-registry-ca-location", "",
		"path to a CA certificate for the Schema Registry")
	c.Flags().StringVar(&f.srCertLocation, "schema-registry-cert-location", "",
		"path to a client certificate for the Schema Registry (mTLS)")
	c.Flags().StringVar(&f.srKeyLocation, "schema-registry-key-location", "",
		"path to a client key for the Schema Registry (mTLS)")
	// Real broker lag, off by default; the counters otherwise only ever report
	// produced-minus-consumed, which is not consumer lag at all.
	c.Flags().DurationVar(&f.lagPoll, "lag-poll", 0,
		"poll brokers for real consumer-group lag at this interval (0 disables)")
	c.Flags().Uint64Var(&f.seed, "seed", 0, "seed for generated data; 0 picks one and reports it")

	c.Flags().StringVar(&f.tuiMode, "tui", "auto", "terminal UI: auto, on or off")
	c.Flags().BoolVar(&f.logJSON, "log-json", false, "emit structured JSON logs")
	c.Flags().StringVar(&f.logLevel, "log-level", "info", "log level: debug, info, warn or error")
	c.Flags().StringVar(&f.metricsAddr, "metrics-addr", "",
		"serve /healthz and /metrics on this address, e.g. :9090")
}

// localCluster is the slice of *localcluster.Cluster that `run` uses.
//
// It exists for one reason: the teardown-on-failure path is the part of `run` most worth
// testing and the part hardest to reach, because the real implementation needs a Docker
// daemon. A test supplies a fake and asserts the cluster is stopped even when the scenario
// blows up, and left alone under --keep-cluster. engine.BrokerController is embedded
// because the same value is handed to the engine for stop_broker/start_broker.
type localCluster interface {
	engine.BrokerController
	Up(context.Context) error
	Down(context.Context, bool) error
	BootstrapServers(context.Context) (string, error)

	// Running is the is-it-up test. BootstrapServers cannot serve that purpose: it
	// falls back to a default address rather than failing when nothing is running.
	Running(context.Context) bool
}

func newRunCmd() *cobra.Command { return runCmd(nil) }

// runCmd builds the run command. newCluster is nil everywhere except in tests, where it
// substitutes a cluster that needs no Docker.
func runCmd(newCluster func(kraft bool) (localCluster, error)) *cobra.Command {
	if newCluster == nil {
		newCluster = func(kraft bool) (localCluster, error) {
			return localcluster.New(localcluster.WithKRaft(kraft))
		}
	}
	var (
		f           runFlags
		keepCluster bool
		bootstrap   string
		mode        string
	)
	cmd := &cobra.Command{
		Use:   "run <scenario>...",
		Short: "Run one or more scenarios against the local Docker cluster",
		Long: "Run scenarios against the bundled local Kafka cluster.\n\n" +
			"Each scenario may be a bundled name (e.g. traffic/high-throughput) or a path to a\n" +
			"YAML file; several can run together. Use `khaos list` to see bundled scenarios.\n\n" +
			"The cluster is started automatically if it is not already running, and stopped\n" +
			"again when the run ends unless --keep-cluster is given.",
		// The scenario list is optional at the cobra level and checked by hand below,
		// exiting 1 with a hint rather than 2 with a cobra usage message.
		Args: cobra.ArbitraryArgs,
		RunE: func(c *cobra.Command, args []string) error {
			if len(args) == 0 {
				return errNoScenario
			}
			kraft, err := parseMode(mode)
			if err != nil {
				return err
			}
			cluster, err := newCluster(kraft)
			if err != nil {
				return fmt.Errorf("local cluster: %w", err)
			}
			stderr := c.ErrOrStderr()

			// The cluster is started automatically when it is not already up, rather than
			// telling the user to do it. The test must be Running(), NOT whether
			// BootstrapServers errors: BootstrapServers falls back to 127.0.0.1:9092 when
			// nothing is published, so it returns an address and no error even when there
			// is no cluster at all. Using its error as the is-it-up check made this
			// auto-start dead code, and `khaos run` on a stopped cluster failed with a
			// bare "connection refused" instead of starting it.
			up := cluster.Running(c.Context())
			started := false
			if !up {
				err := spin(stderr, "Starting Kafka cluster",
					"(a first run pulls ~1GB of images and can take minutes)",
					func() error { return cluster.Up(c.Context()) })
				if err != nil {
					return err
				}
				spinDone(stderr, "Kafka cluster ready", "")
				up, started = true, true
			}

			// ...and tears it down afterwards unless --keep-cluster. Registered here,
			// BEFORE anything else can fail, and running on a context that is still live,
			// so a scenario that panics or errors still stops the cluster: a run that
			// leaks a three-broker cluster on every failure is how a laptop ends up with
			// no memory left.
			defer func() {
				// Nothing came up, so there is nothing to stop and nothing truthful to
				// say about it.
				if !up {
					return
				}
				if keepCluster {
					fmt.Fprintln(stderr, styDim.Render("Kafka cluster left running · khaos cluster-down to stop"))
					return
				}
				// Printed unconditionally: the cluster is stopped whether or not this
				// process started it, and a silent teardown of a cluster the user started
				// by hand is the surprising case, not the quiet one.
				if !started {
					fmt.Fprintln(stderr, styDim.Render("(the cluster was already running; pass -k to keep it)"))
				}
				stopCtx, cancel := context.WithTimeout(context.WithoutCancel(c.Context()), 2*time.Minute)
				defer cancel()
				// false: `run` never asked to remove volumes. Only `cluster-down --volumes`
				// does.
				if err := spin(stderr, "Stopping Kafka cluster", "", func() error {
					return cluster.Down(stopCtx, false)
				}); err != nil {
					fmt.Fprintf(stderr, "khaos: could not stop cluster: %v\n", err)
					return
				}
				spinDone(stderr, "Kafka cluster stopped", "")
			}()

			servers := bootstrap
			if servers == "" {
				servers, err = cluster.BootstrapServers(c.Context())
				if err != nil {
					return err
				}
			}

			return execute(c.Context(), c.OutOrStdout(), stderr, args, kafka.Config{
				BootstrapServers: splitServers(servers),
			}, cluster, &f)
		},
	}

	cmd.Flags().BoolVarP(&keepCluster, "keep-cluster", "k", false,
		"keep the Kafka cluster running after the scenario ends")
	cmd.Flags().StringVarP(&bootstrap, "bootstrap-servers", "b", "",
		"override the local cluster's bootstrap servers")
	cmd.Flags().StringVarP(&mode, "mode", "m", "kraft", "cluster mode: kraft or zookeeper")
	f.bind(cmd)
	return cmd
}

func newSimulateCmd() *cobra.Command {
	var (
		f    runFlags
		sec  kafka.Security
		boot string
	)
	cmd := &cobra.Command{
		Use:   "simulate <scenario>...",
		Short: "Run one or more scenarios against an external Kafka cluster",
		Long: "Run scenarios against a cluster khaos does not manage.\n\n" +
			"Each scenario may be a bundled name (e.g. traffic/high-throughput) or a path to a\n" +
			"YAML file; several can run together. Use `khaos list` to see bundled scenarios.\n\n" +
			"Broker fault incidents (stop_broker, start_broker) are unavailable here and are\n" +
			"reported as skipped rather than silently ignored.",
		Args: cobra.ArbitraryArgs,
		RunE: func(c *cobra.Command, args []string) error {
			// Order matters: bootstrap-servers is checked BEFORE the scenario list, so
			// `khaos simulate` with neither reports the missing broker list first.
			if boot == "" {
				return fmt.Errorf("--bootstrap-servers is required")
			}
			if len(args) == 0 {
				return errNoScenario
			}
			if err := sec.Validate(); err != nil {
				return err
			}
			// Brokers is nil: an external cluster gets the no-op controller, so broker
			// incidents surface as events instead of doing nothing at all.
			return execute(c.Context(), c.OutOrStdout(), c.ErrOrStderr(), args, kafka.Config{
				BootstrapServers: splitServers(boot),
				Security:         sec,
				External:         true,
			}, nil, &f)
		},
	}

	cmd.Flags().StringVarP(&boot, "bootstrap-servers", "b", "", "comma-separated broker list (required)")
	cmd.Flags().StringVar(&sec.Protocol, "security-protocol", "PLAINTEXT",
		"PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL")
	cmd.Flags().StringVar(&sec.SASLMechanism, "sasl-mechanism", "",
		"PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512")
	cmd.Flags().StringVar(&sec.SASLUsername, "sasl-username", "", "SASL username")
	cmd.Flags().StringVar(&sec.SASLPassword, "sasl-password", "", "SASL password")
	cmd.Flags().StringVar(&sec.CALocation, "ssl-ca-location", "", "path to the CA certificate")
	cmd.Flags().StringVar(&sec.CertLocation, "ssl-cert-location", "", "path to the client certificate")
	cmd.Flags().StringVar(&sec.KeyLocation, "ssl-key-location", "", "path to the client key")
	// Accepted so the flag is rejected by name rather than as "unknown flag": encrypted
	// keys are not supported (internal/kafka/security.go:69).
	cmd.Flags().StringVar(&sec.KeyPassword, "ssl-key-password", "",
		"unsupported: decrypt the key first with 'openssl pkcs8 -nocrypt'")
	f.bind(cmd)
	return cmd
}

// durationValue accepts either a bare number of seconds or any Go duration string, so
// `-d 60` and `-d 90s` both work.
type durationValue time.Duration

func (d *durationValue) String() string {
	if *d == 0 {
		return "0"
	}
	return time.Duration(*d).String()
}

func (d *durationValue) Set(v string) error {
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return fmt.Errorf("duration cannot be negative")
		}
		*d = durationValue(time.Duration(secs) * time.Second)
		return nil
	}
	parsed, err := time.ParseDuration(v)
	if err != nil {
		return fmt.Errorf("invalid duration %q: want seconds (60) or a duration (90s, 10m)", v)
	}
	if parsed < 0 {
		return fmt.Errorf("duration cannot be negative")
	}
	*d = durationValue(parsed)
	return nil
}

func (d *durationValue) Type() string { return "duration" }

// registry assembles the Schema Registry configuration from the flags.
func (f *runFlags) registry() codec.RegistryConfig {
	return codec.RegistryConfig{
		URL:          f.schemaRegistryURL,
		Username:     f.srUsername,
		Password:     f.srPassword,
		BearerToken:  f.srBearerToken,
		CALocation:   f.srCALocation,
		CertLocation: f.srCertLocation,
		KeyLocation:  f.srKeyLocation,
	}
}

// execute is the shared body of run and simulate.
//
// Several scenarios can run together: their topics, incidents and flows are concatenated
// into one run.
func execute(ctx context.Context, stdout, stderr io.Writer, names []string, kcfg kafka.Config, cluster localCluster, f *runFlags) error {
	useTUI, level, err := f.resolve()
	if err != nil {
		return err
	}

	scenarios := make([]*scenario.Scenario, 0, len(names))
	for _, name := range names {
		sc, err := loadScenario(stderr, name)
		if err != nil {
			return err
		}
		scenarios = append(scenarios, sc)
	}

	logger := telemetry.NewLogger(stderr, f.logJSON, level)
	if useTUI {
		// Under a TUI, logs must not reach the terminal at all or they will corrupt the
		// rendered frame.
		logger = telemetry.NewLogger(io.Discard, f.logJSON, level)
	}

	var brokers engine.BrokerController
	if cluster != nil {
		brokers = cluster
	}

	ecfg := engine.Config{
		Kafka:             kcfg,
		Scenarios:         scenarios,
		Duration:          time.Duration(f.duration),
		NoConsumers:       f.noConsumers,
		RecreateTopics:    f.recreateTopics,
		SkipTopicCreation: f.skipTopicCreation,
		Seed:              f.seed,
		Brokers:           brokers,
		Registry:          f.registry(),
		LagPoll:           f.lagPoll,
		Logger:            logger,
	}

	eng, err := engine.New(ctx, ecfg)
	if err != nil {
		return err
	}

	names_ := make([]string, len(scenarios))
	topics, flows := 0, 0
	for i, s := range scenarios {
		names_[i] = s.Name
		topics += len(s.Topics)
		flows += len(s.Flows)
	}
	logger.Info("starting run",
		"scenarios", strings.Join(names_, ", "),
		"topics", topics,
		"flows", flows,
		"duration", time.Duration(f.duration),
		"seed", eng.Seed())

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The output layer runs OUTSIDE the engine's errgroup, deliberately. If it were a
	// member, a panic in rendering would cancel the group and kill the run.
	outDone := make(chan struct{})
	go func() {
		defer close(outDone)
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(stderr, "khaos: output layer panicked, continuing headless: %v\n", r)
			}
		}()
		if useTUI {
			// Column visibility is decided from config here, once, so the table keeps one
			// shape for the whole run. Deciding from live counters instead made the
			// FAILED/DLQ columns pop in on the first simulated failure.
			uiOpts := []tui.Option{
				tui.ShowFailureColumns(anyFailureSimulation(scenarios)),
				tui.ShowBrokerLag(f.lagPoll > 0),
			}
			if err := tui.Run(runCtx, eng, cancel, uiOpts); err != nil {
				fmt.Fprintf(stderr, "khaos: terminal UI stopped: %v\n", err)
			}
			return
		}
		logLoop(runCtx, eng, logger)
	}()

	var srvDone chan struct{}
	if f.metricsAddr != "" {
		srv := telemetry.NewServer(f.metricsAddr, nil, eng.Healthy)
		srvDone = make(chan struct{})
		go func() {
			defer close(srvDone)
			if err := srv.Run(runCtx); err != nil {
				logger.Error("metrics server stopped", "err", err)
			}
		}()
	}

	runErr := eng.Run(runCtx)

	cancel()
	<-outDone
	if srvDone != nil {
		<-srvDone
	}

	summary(stdout, eng.Snapshot(), useTUI)
	return runErr
}

// anyFailureSimulation reports whether any topic's consumers run failure simulation.
func anyFailureSimulation(scenarios []*scenario.Scenario) bool {
	for _, sc := range scenarios {
		for _, t := range sc.Topics {
			if t.ConsumerConfig.FailureSimulationEnabled() {
				return true
			}
		}
	}
	return false
}

// totalBrokerLag sums real consumer-group lag across every topic that reported it.
//
// The bool is the point: nil BrokerLag means "not measured" -- polling is off, or the
// cluster refused DESCRIBE -- which must stay distinguishable from a genuine zero.
func totalBrokerLag(s engine.Snapshot) (int64, bool) {
	var total int64
	var measured bool
	for _, t := range s.Topics {
		for _, lag := range t.BrokerLag {
			total += lag
			measured = true
		}
	}
	return total, measured
}

// logLoop is the headless output path: a periodic structured summary.
//
// This is a first-class mode, not a degraded TUI. A 24/7 container run uses this and it
// must be complete enough to operate from.
func logLoop(ctx context.Context, eng *engine.Engine, logger *slog.Logger) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()

	var lastEvent time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s := eng.Snapshot()
			attrs := []any{
				"elapsed", s.Elapsed.Round(time.Second).String(),
				"produced", s.TotalProduced,
				"consumed", s.TotalConsumed,
				"lag", s.TotalProduced - s.TotalConsumed,
				"errors", s.TotalErrors,
				"rebalances", s.Rebalances,
			}
			// "lag" above is khaos's own produced-minus-consumed count. With
			// --lag-poll the brokers are asked for the real thing, and a headless daemon is
			// precisely the case that cannot see the TUI's second column -- so surface it
			// here too. Absent when nothing could be measured, never zero: a lag of 0 and a
			// lag we could not read must not look the same in a log a monitoring system is
			// being graded against.
			if broker, ok := totalBrokerLag(s); ok {
				attrs = append(attrs, "broker_lag", broker)
			}
			logger.Info("progress", attrs...)

			// Events are emitted as they appear rather than only at the end, because for
			// a monitoring-system ground truth the timestamp of an incident is the point.
			for _, e := range s.Events {
				if e.At.After(lastEvent) {
					logger.Info("event", "at", e.At, "level", int(e.Level), "message", e.Message)
					lastEvent = e.At
				}
			}
		}
	}
}

// resolve validates the output flags and answers the two questions the run needs: whether
// a terminal UI runs, and at what level to log.
//
// Both flags used to fall back silently on an unrecognised value, so `--tui of` quietly
// meant "auto" and `--log-level trace` quietly meant "info" -- a typo that changes what
// you see and never says so. Rejecting the typo is strictly better.
func (f *runFlags) resolve() (useTUI bool, level slog.Level, err error) {
	switch strings.ToLower(f.tuiMode) {
	case "on", "true", "yes":
		useTUI = true
	case "off", "false", "no":
		useTUI = false
	case "auto", "":
		useTUI = autoTUI()
	default:
		return false, 0, fmt.Errorf("invalid --tui %q: want auto, on or off", f.tuiMode)
	}

	switch strings.ToLower(f.logLevel) {
	case "debug":
		level = slog.LevelDebug
	case "info", "":
		level = slog.LevelInfo
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return false, 0, fmt.Errorf("invalid --log-level %q: want debug, info, warn or error", f.logLevel)
	}
	return useTUI, level, nil
}

// autoTUI is the `--tui auto` decision: a terminal UI only when stdout really is a
// terminal and the environment is not a CI log.
func autoTUI() bool {
	if os.Getenv("CI") != "" || os.Getenv("TERM") == "dumb" {
		return false
	}
	return term.IsTerminal(int(os.Stdout.Fd()))
}

// parseMode maps the --mode string onto the boolean localcluster wants, rejecting
// anything else; a typo like `-m zookeper` would otherwise silently run KRaft.
func parseMode(mode string) (kraft bool, err error) {
	switch strings.ToLower(mode) {
	case "kraft":
		return true, nil
	case "zookeeper":
		return false, nil
	default:
		return false, fmt.Errorf("invalid --mode %q: want kraft or zookeeper", mode)
	}
}

// errNoScenario is the message and hint for an empty scenario list on both `run` and
// `simulate`.
var errNoScenario = errors.New("please specify at least one scenario (use `khaos list` to see them)")

func splitServers(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
