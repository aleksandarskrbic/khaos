package localcluster

import (
	"bytes"
	"context"
	"os/exec"
	"strings"
)

// ProjectName is the compose project every khaos invocation runs under.
//
// Compose derives the project name from the compose file's parent directory when -p is
// absent, and the embedded files materialize into a cache directory whose name varies by
// machine. Passing -p explicitly on every invocation pins the project so the Schema
// Registry stack's external network reference always resolves to it.
const ProjectName = "khaos"

// NetworkName is the bridge network the cluster stack creates and the Schema Registry
// stack joins as an external network. It must equal "<ProjectName>_kafka-net"; assets_test
// asserts the embedded YAML agrees.
const NetworkName = ProjectName + "_kafka-net"

// dockerBinary is the CLI khaos shells out to.
//
// Everything this package does is compose-level (up/down/ps/stop/start against a compose
// project). The Docker Engine API has no notion of a compose project at all, so the Go
// SDK cannot express these operations -- the CLI is not a shortcut here, it is the only
// interface that has the concept.
const dockerBinary = "docker"

// runner executes the docker CLI. It is a field on Cluster rather than an interface
// because there is exactly one real implementation; tests substitute a closure.
type runner func(ctx context.Context, args ...string) (stdout, stderr string, err error)

// execDocker runs the docker CLI, always capturing both streams so docker's progress
// rendering -- including cursor movement escapes -- never reaches the terminal and shreds
// a live TUI. Output is surfaced only through returned errors.
func execDocker(ctx context.Context, args ...string) (string, string, error) {
	cmd := exec.CommandContext(ctx, dockerBinary, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// composeArgs builds a compose invocation for composeFile with the explicit project flag.
// Every command in this package goes through it, so -p can never be forgotten.
func composeArgs(composeFile string, sub ...string) []string {
	args := make([]string, 0, 5+len(sub))
	args = append(args, "compose", "-p", ProjectName, "-f", composeFile)
	return append(args, sub...)
}

func upArgs(composeFile string) []string {
	return composeArgs(composeFile, "up", "-d")
}

// downArgs appends -v only when volumes are removed.
func downArgs(composeFile string, removeVolumes bool) []string {
	if removeVolumes {
		return composeArgs(composeFile, "down", "-v")
	}
	return composeArgs(composeFile, "down")
}

func psArgs(composeFile string) []string {
	return composeArgs(composeFile, "ps", "--format", "json")
}

func stopArgs(composeFile, service string) []string {
	return composeArgs(composeFile, "stop", service)
}

func startArgs(composeFile, service string) []string {
	return composeArgs(composeFile, "start", service)
}

// containerRunningArgs is a plain `docker ps` filter, not a compose command -- it is used
// to detect containers started by an earlier process.
func containerRunningArgs(nameFilter string) []string {
	return []string{"ps", "--filter", "name=" + nameFilter, "--format", "{{.Names}}"}
}

// isContainerRunning reports whether a container whose name contains nameFilter is up.
// The match is by substring, not exact name.
func (c *Cluster) isContainerRunning(ctx context.Context, nameFilter string) bool {
	stdout, _, err := c.run(ctx, containerRunningArgs(nameFilter)...)
	if err != nil {
		return false
	}
	return strings.Contains(stdout, nameFilter)
}
