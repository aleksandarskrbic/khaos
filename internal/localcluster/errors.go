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

// classifyComposeError turns a failed docker invocation into a typed error.
//
// The last branch treats any stderr containing "not found" (case insensitive) as a missing
// compose file, so e.g. `manifest ... not found` from a failed image pull is also reported
// as ErrComposeFileNotFound rather than narrowed to the exact case. The wrapped stderr is
// always included so the real cause is never lost.
func classifyComposeError(action, composeFile, stderr string, err error) error {
	low := strings.ToLower(stderr)

	switch {
	case strings.Contains(stderr, "Cannot connect to the Docker daemon"),
		strings.Contains(stderr, "Is the docker daemon running"):
		return fmt.Errorf("%s: %w: %s", action, ErrDockerNotRunning, msgDockerNotRunning)

	case strings.Contains(stderr, "port is already allocated"):
		// The bundled stack also publishes 8080 (kafka-ui) and 8081 (schema-registry), so
		// a generic "port already allocated" message would send you hunting for a stray
		// Kafka when the actual clash is elsewhere. Name the real port when it can be
		// extracted, and always append what docker actually said.
		if port := conflictingPort(stderr); port != "" {
			return fmt.Errorf("%s: %w: port %s is already in use: %s",
				action, ErrPortAllocated, port, strings.TrimSpace(stderr))
		}
		return fmt.Errorf("%s: %w: %s: %s", action, ErrPortAllocated, msgPortAllocated, strings.TrimSpace(stderr))

	case strings.Contains(low, "no such file or directory"), strings.Contains(low, "not found"):
		return fmt.Errorf("%s: %w: %s: %s", action, ErrComposeFileNotFound, composeFile, strings.TrimSpace(stderr))
	}

	if stderr = strings.TrimSpace(stderr); stderr != "" {
		return fmt.Errorf("%s: %w: %s", action, err, stderr)
	}
	return fmt.Errorf("%s: %w", action, err)
}
