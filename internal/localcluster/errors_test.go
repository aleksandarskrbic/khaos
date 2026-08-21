package localcluster

import (
	"errors"
	"strings"
	"testing"
)

// TestClassifyComposeError feeds real docker stderr samples through the classifier and
// pins the precedence among the substring matches.
func TestClassifyComposeError(t *testing.T) {
	tests := []struct {
		name        string
		stderr      string
		wantErr     error
		wantMessage string
	}{
		{
			name:        "daemon down",
			stderr:      "Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?",
			wantErr:     ErrDockerNotRunning,
			wantMessage: msgDockerNotRunning,
		},
		{
			name:        "daemon down, second phrasing only",
			stderr:      "error during connect: Is the docker daemon running?",
			wantErr:     ErrDockerNotRunning,
			wantMessage: msgDockerNotRunning,
		},
		{
			// The message names the port docker actually failed to bind rather than a
			// fixed range, since the clash may be on 8080 (kafka-ui) or 8081
			// (schema-registry). See errors_port_test.go.
			name:        "port taken",
			stderr:      `Error response from daemon: driver failed programming external connectivity on endpoint kafka-1: Bind for 0.0.0.0:9092 failed: port is already allocated`,
			wantErr:     ErrPortAllocated,
			wantMessage: "port 9092 is already in use",
		},
		{
			name:        "missing compose file",
			stderr:      "stat /nope/docker-compose.kraft.yml: no such file or directory",
			wantErr:     ErrComposeFileNotFound,
			wantMessage: "/cache/docker-compose.kraft.yml",
		},
		{
			// Any "not found" is reported as a missing compose file, even when the real
			// cause is an image pull.
			name:    "image pull failure is misreported as a missing file",
			stderr:  "Error response from daemon: manifest for confluentinc/cp-kafka:9.9.9 not found",
			wantErr: ErrComposeFileNotFound,
		},
		{
			name:    "unrecognised stderr keeps the underlying error",
			stderr:  "something else went wrong",
			wantErr: errFakeExit,
		},
		{
			name:    "empty stderr keeps the underlying error",
			stderr:  "",
			wantErr: errFakeExit,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := classifyComposeError("start containers", "/cache/docker-compose.kraft.yml", tt.stderr, errFakeExit)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want errors.Is(..., %v)", err, tt.wantErr)
			}
			if !strings.Contains(err.Error(), "start containers") {
				t.Errorf("error lost the action context: %v", err)
			}
			if tt.wantMessage != "" && !strings.Contains(err.Error(), tt.wantMessage) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantMessage)
			}
		})
	}
}

// Precedence matters: a stderr that mentions both the daemon and a missing file must be
// reported as the daemon problem, because that is the actionable one.
func TestClassifyComposeErrorPrecedence(t *testing.T) {
	stderr := "Cannot connect to the Docker daemon: no such file or directory"
	err := classifyComposeError("stop containers", "/f.yml", stderr, errFakeExit)
	if !errors.Is(err, ErrDockerNotRunning) {
		t.Fatalf("err = %v, want ErrDockerNotRunning", err)
	}
	if errors.Is(err, ErrComposeFileNotFound) {
		t.Fatal("must not also classify as ErrComposeFileNotFound")
	}
}
