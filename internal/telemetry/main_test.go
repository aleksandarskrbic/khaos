package telemetry

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if any test leaves a goroutine behind.
//
// Server.Run owns a listener and a Serve goroutine, and its whole contract is that
// returning means both are gone -- khaos must not still be holding a port after Ctrl-C.
// Without this the only symptom of a botched shutdown would be a confusing "address
// already in use" on the next run.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
