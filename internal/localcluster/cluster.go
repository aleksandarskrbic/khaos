// Package localcluster manages the bundled single-host Kafka cluster that khaos uses for
// local runs, by driving the docker CLI against embedded compose files.
//
// Every compose invocation carries an explicit `-p khaos` (see ProjectName), stop/start
// capture their output instead of letting docker's cursor escapes hit a live TUI (see
// execDocker), and the bootstrap port is chosen by matching each broker's client listener
// rather than trusting publisher order (see selectPublishedPort). Readiness is probed with
// a hand-rolled Kafka ApiVersions request per broker instead of an AdminClient call, both
// to avoid depending on internal/kafka and because a bare TCP dial succeeds against
// docker's userland proxy before the broker inside is actually listening (see probeAll).
//
// Nothing here writes to stdout or stderr; all output is returned.
package localcluster

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// Cluster drives one bundled compose project.
//
// It is not safe for concurrent use: the lifecycle operations are inherently serial and a
// caller interleaving Up and Down would confuse compose regardless of any locking here.
type Cluster struct {
	kraft          bool
	schemaRegistry bool

	// dir is the stable directory the embedded compose files are materialized into.
	dir string

	// run executes the docker CLI. Swapped in tests so no daemon is required.
	run runner
}

// Option configures a Cluster.
type Option func(*Cluster)

// WithKRaft selects the KRaft compose files when true and the ZooKeeper ones when false.
// KRaft is the default.
func WithKRaft(enabled bool) Option {
	return func(c *Cluster) { c.kraft = enabled }
}

// WithSchemaRegistry makes Up also start the Schema Registry stack and wait for it, and
// makes Down tear it down first.
func WithSchemaRegistry(enabled bool) Option {
	return func(c *Cluster) { c.schemaRegistry = enabled }
}

// New materializes the embedded compose files and returns a Cluster ready to drive them.
//
// Materialization happens here rather than lazily in Up so a bad cache directory is
// reported before anything is started.
func New(opts ...Option) (*Cluster, error) {
	dir, err := composeDir()
	if err != nil {
		return nil, err
	}

	c := &Cluster{kraft: true, dir: dir, run: execDocker}
	for _, opt := range opts {
		opt(c)
	}

	if _, err := materializeAssets(c.dir); err != nil {
		return nil, err
	}
	return c, nil
}

// clusterFile is the broker stack's compose file.
func (c *Cluster) clusterFile() string {
	if c.kraft {
		return filepath.Join(c.dir, composeKRaft)
	}
	return filepath.Join(c.dir, composeZooKeeper)
}

// registryFile is the Schema Registry stack's compose file.
func (c *Cluster) registryFile() string {
	if c.kraft {
		return filepath.Join(c.dir, composeSRKRaft)
	}
	return filepath.Join(c.dir, composeSRZK)
}

// Up starts the cluster and blocks until it is reachable.
//
// It brings the broker stack up and waits for it, then, when configured, brings Schema
// Registry up and waits for it too. An already-running Schema Registry is left alone.
func (c *Cluster) Up(ctx context.Context) error {
	file := c.clusterFile()
	if _, stderr, err := c.run(ctx, upArgs(file)...); err != nil {
		return classifyComposeError("start containers", file, stderr, err)
	}

	bootstrap, err := c.BootstrapServers(ctx)
	if err != nil {
		return err
	}
	if err := c.waitForKafka(ctx, bootstrap); err != nil {
		return err
	}

	if !c.schemaRegistry {
		return nil
	}
	if !c.isContainerRunning(ctx, "schema-registry") {
		srFile := c.registryFile()
		if _, stderr, err := c.run(ctx, upArgs(srFile)...); err != nil {
			return classifyComposeError("start schema registry", srFile, stderr, err)
		}
	}
	return c.waitForSchemaRegistry(ctx)
}

// Down stops the cluster, optionally removing its volumes.
//
// removeVolumes is the caller's `--volumes`/`-v`, defaulting to false. For the bundled
// stack this currently changes nothing observable -- none of the four compose files
// declares a volume, so `compose down` discards the brokers' log dirs either way -- but
// the flag is honoured rather than hardcoded so it stays correct if a volume is ever added.
//
// Schema Registry is torn down first and its failures are ignored, since it is a dependent
// stack and refusing to stop the brokers over an already-absent registry would be worse
// than the lost detail. Teardown is attempted whenever its container is running, not only
// when WithSchemaRegistry was set, so a cluster started by an earlier process still gets
// cleaned up.
func (c *Cluster) Down(ctx context.Context, removeVolumes bool) error {
	if c.schemaRegistry || c.isContainerRunning(ctx, "schema-registry") {
		_, _, _ = c.run(ctx, downArgs(c.registryFile(), removeVolumes)...)
	}

	file := c.clusterFile()
	if _, stderr, err := c.run(ctx, downArgs(file, removeVolumes)...); err != nil {
		return classifyComposeError("stop containers", file, stderr, err)
	}
	return nil
}

// Status returns one row per compose service, sorted by service name.
//
// A non-zero exit from `compose ps` is not fatal: a project that was never started exits
// non-zero with empty output, and the honest answer is "no services", not an error.
func (c *Cluster) Status(ctx context.Context) ([]ServiceStatus, error) {
	stdout, stderr, err := c.run(ctx, psArgs(c.clusterFile())...)
	if err != nil {
		// Only surface the failure when docker itself is unusable; otherwise an
		// un-started project would look like a hard error.
		if classified := classifyComposeError("list services", c.clusterFile(), stderr, err); isDockerUnusable(classified) {
			return nil, classified
		}
		return nil, nil
	}
	return parsePS(stdout), nil
}

// StopBroker stops one broker container, leaving the rest of the cluster up.
func (c *Cluster) StopBroker(ctx context.Context, name string) error {
	return c.serviceAction(ctx, "stop", name)
}

// StartBroker restarts a broker previously stopped by StopBroker.
func (c *Cluster) StartBroker(ctx context.Context, name string) error {
	return c.serviceAction(ctx, "start", name)
}

// serviceAction runs `compose stop|start <service>` after confirming there is a cluster
// to act on.
//
// The check asks the compose project's own service list rather than probing for a
// specific container name: once StopBroker has taken kafka-1 down, a name-based probe for
// "kafka-1" would find nothing and StartBroker("kafka-1") could never bring it back, while
// the service list still reports kafka-2, kafka-3 and kafka-ui as present.
func (c *Cluster) serviceAction(ctx context.Context, action, name string) error {
	file := c.clusterFile()

	statuses, err := c.Status(ctx)
	if err != nil {
		return fmt.Errorf("%s broker %s: %w", action, name, err)
	}
	if len(statuses) == 0 {
		return fmt.Errorf("%s broker %s: %w", action, name, ErrNoActiveCluster)
	}

	var args []string
	if action == "stop" {
		args = stopArgs(file, name)
	} else {
		args = startArgs(file, name)
	}

	if _, stderr, err := c.run(ctx, args...); err != nil {
		return classifyComposeError(fmt.Sprintf("%s broker %s", action, name), file, stderr, err)
	}
	return nil
}

// BootstrapServers returns the comma-separated broker list clients should connect to.
//
// It falls back to 127.0.0.1:9092 when the project publishes nothing; callers treat that
// as "try the conventional port" rather than as an error.
func (c *Cluster) BootstrapServers(ctx context.Context) (string, error) {
	statuses, err := c.Status(ctx)
	if err != nil {
		return "", fmt.Errorf("determine bootstrap servers: %w", err)
	}
	return brokerAddresses(statuses), nil
}

// isDockerUnusable reports whether an error means docker cannot be talked to at all, as
// opposed to a command that merely had nothing to do.
func isDockerUnusable(err error) bool {
	return errors.Is(err, ErrDockerNotRunning) || errors.Is(err, ErrComposeFileNotFound)
}

// Running reports whether every service of the cluster is up: empty status means not
// running, otherwise every service's state must contain "running".
//
// This exists because BootstrapServers cannot answer the question -- it falls back to
// 127.0.0.1:9092 when nothing is published, so it returns an address and no error even
// when there is no cluster at all, which would make an auto-start check based on it dead
// code.
func (c *Cluster) Running(ctx context.Context) bool {
	statuses, err := c.Status(ctx)
	if err != nil || len(statuses) == 0 {
		return false
	}
	for _, s := range statuses {
		if !strings.Contains(strings.ToLower(s.State), "running") {
			return false
		}
	}
	return true
}
