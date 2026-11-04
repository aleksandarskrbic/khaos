package localcluster

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Sentinel errors for the docker failures khaos can say something useful about.
//
// These are recognized by substring-matching docker's stderr; classification happens once,
// here, and callers branch with errors.Is instead of re-matching strings.
var (
	// ErrDockerNotRunning means the docker daemon could not be reached.
	ErrDockerNotRunning = errors.New("docker is not running")

	// ErrPortAllocated means a published port is already bound by another process.
	ErrPortAllocated = errors.New("port already allocated")

	// ErrComposeFileNotFound means docker could not read the compose file it was given.
	ErrComposeFileNotFound = errors.New("compose file not found")

	// ErrNoActiveCluster is returned by operations that need a running cluster to act on.
	ErrNoActiveCluster = errors.New("no active kafka cluster found")

	// ErrNotReady means a readiness poll hit its deadline.
	ErrNotReady = errors.New("did not become ready in time")
)

// Friendly messages surfaced to the user alongside the typed sentinel errors.
const (
	msgDockerNotRunning = "Docker is not running. Please start Docker Desktop and try again."
	msgPortAllocated    = "Ports 9092-9094 already in use. Stop other Kafka instances or free the ports."
)

// portBindRe pulls the port out of docker's bind failure.
//
// Two host forms must be handled: IPv4 and bare hostnames come through as
// "Bind for 0.0.0.0:8080 failed", but IPv6 addresses come through bracketed, as
// "Bind for [::]:8080 failed", with colons of their own inside the brackets that a naive
// [^:]* host pattern would fail to skip over.
var portBindRe = regexp.MustCompile(`Bind for (?:\[[^\]]*\]|[^:\s]*):(\d+) failed`)

// conflictingPort returns the port docker actually failed to bind, or "".
func conflictingPort(stderr string) string {
	if m := portBindRe.FindStringSubmatch(stderr); len(m) == 2 {
		return m[1]
	}
	return ""
}

// composeFailure recognises one docker failure mode by substring-matching stderr.
type composeFailure struct {
	patterns []string // any one of these in stderr identifies the failure
	fold     bool     // match against lowercased stderr
	sentinel error
	// detail renders what follows the sentinel in the returned error.
	detail func(composeFile, stderr string) string
}

// composeFailures is walked in order and the first match wins, so a stderr that mentions
// both the daemon and a missing file is reported as the daemon problem, the actionable one.
var composeFailures = []composeFailure{
	{
		patterns: []string{"Cannot connect to the Docker daemon", "Is the docker daemon running"},
		sentinel: ErrDockerNotRunning,
		detail:   func(string, string) string { return msgDockerNotRunning },
	},
	{
		patterns: []string{"port is already allocated"},
		sentinel: ErrPortAllocated,
		detail:   portAllocatedDetail,
	},
	{
		patterns: []string{"no such file or directory", "not found"},
		fold:     true,
		sentinel: ErrComposeFileNotFound,
		detail: func(composeFile, stderr string) string {
			return composeFile + ": " + strings.TrimSpace(stderr)
		},
	},
}

func (f composeFailure) matches(stderr string) bool {
	if f.fold {
		stderr = strings.ToLower(stderr)
	}
	for _, p := range f.patterns {
		if strings.Contains(stderr, p) {
			return true
		}
	}
	return false
}

// portAllocatedDetail names the port docker actually failed to bind.
//
// The bundled stack also publishes 8080 (kafka-ui) and 8081 (schema-registry), so a
// generic "port already allocated" message would send you hunting for a stray Kafka when
// the actual clash is elsewhere. Name the real port when it can be extracted, and always
// append what docker actually said.
func portAllocatedDetail(_, stderr string) string {
	if port := conflictingPort(stderr); port != "" {
		return fmt.Sprintf("port %s is already in use: %s", port, strings.TrimSpace(stderr))
	}
	return msgPortAllocated + ": " + strings.TrimSpace(stderr)
}

// classifyComposeError turns a failed docker invocation into a typed error.
//
// The last entry of composeFailures treats any stderr containing "not found" (case
// insensitive) as a missing compose file, so e.g. `manifest ... not found` from a failed
// image pull is also reported as ErrComposeFileNotFound rather than narrowed to the exact
// case. The wrapped stderr is always included so the real cause is never lost.
func classifyComposeError(action, composeFile, stderr string, err error) error {
	for _, f := range composeFailures {
		if f.matches(stderr) {
			return fmt.Errorf("%s: %w: %s", action, f.sentinel, f.detail(composeFile, stderr))
		}
	}

	if stderr = strings.TrimSpace(stderr); stderr != "" {
		return fmt.Errorf("%s: %w: %s", action, err, stderr)
	}
	return fmt.Errorf("%s: %w", action, err)
}
