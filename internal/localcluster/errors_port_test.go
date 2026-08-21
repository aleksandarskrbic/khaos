package localcluster

import (
	"errors"
	"strings"
	"testing"
)

// A real docker failure: the bundled stack publishes 8080 for kafka-ui, which collides
// with anything else already on that port. The error must name the actual port rather
// than a generic guess, so a hunt for the wrong culprit doesn't start.
func TestPortConflictNamesTheRealPort(t *testing.T) {
	stderr := `Error response from daemon: failed to set up container networking: ` +
		`driver failed programming external connectivity on endpoint kafka-ui ` +
		`(d2fc0d9dab0c): Bind for 0.0.0.0:8080 failed: port is already allocated`

	err := classifyComposeError("start containers", "compose.yml", stderr, errors.New("exit 1"))

	if !errors.Is(err, ErrPortAllocated) {
		t.Fatalf("want ErrPortAllocated, got %v", err)
	}
	if !strings.Contains(err.Error(), "8080") {
		t.Errorf("error should name the real port 8080, got: %v", err)
	}
	if strings.Contains(err.Error(), "9092-9094") {
		t.Errorf("error should not claim 9092-9094 when 8080 was the conflict: %v", err)
	}
	if !strings.Contains(err.Error(), "kafka-ui") {
		t.Errorf("error should retain docker's stderr naming the container: %v", err)
	}
}

// The host part of docker's bind message takes several shapes, and the IPv6 one carries
// colons of its own. Every shape must still yield the real port.
func TestConflictingPortAcrossDockerHostForms(t *testing.T) {
	tests := []struct {
		name   string
		stderr string
		want   string
	}{
		{
			name:   "ipv4 wildcard",
			stderr: "Bind for 0.0.0.0:9092 failed: port is already allocated",
			want:   "9092",
		},
		{
			name:   "ipv4 loopback",
			stderr: "Bind for 127.0.0.1:8081 failed: port is already allocated",
			want:   "8081",
		},
		{
			// docker with ipv6 enabled, or a compose file publishing on "[::]".
			name:   "ipv6 wildcard",
			stderr: "Bind for [::]:8080 failed: port is already allocated",
			want:   "8080",
		},
		{
			name:   "ipv6 loopback",
			stderr: "Bind for [::1]:9094 failed: port is already allocated",
			want:   "9094",
		},
		{
			name:   "no bind clause",
			stderr: "port is already allocated",
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := conflictingPort(tt.stderr); got != tt.want {
				t.Fatalf("conflictingPort = %q, want %q", got, tt.want)
			}

			err := classifyComposeError("start containers", "compose.yml", tt.stderr, errFakeExit)
			if !errors.Is(err, ErrPortAllocated) {
				t.Fatalf("err = %v, want ErrPortAllocated", err)
			}
			if tt.want == "" {
				return
			}
			if !strings.Contains(err.Error(), "port "+tt.want+" is already in use") {
				t.Errorf("error should name port %s: %v", tt.want, err)
			}
			if strings.Contains(err.Error(), "9092-9094") && tt.want != "9092" {
				t.Errorf("error fell back to the hardcoded range despite knowing port %s: %v", tt.want, err)
			}
		})
	}
}

// When docker's wording changes and no port can be extracted, fall back to the generic
// message rather than losing the classification.
func TestPortConflictFallsBackToGenericWording(t *testing.T) {
	err := classifyComposeError("start containers", "compose.yml",
		"something odd: port is already allocated", errors.New("exit 1"))

	if !errors.Is(err, ErrPortAllocated) {
		t.Fatalf("want ErrPortAllocated, got %v", err)
	}
	if !strings.Contains(err.Error(), msgPortAllocated) {
		t.Errorf("want the fallback message, got: %v", err)
	}
}
