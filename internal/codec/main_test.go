package codec

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if any test leaves a goroutine behind.
//
// Nothing here is meant to run in the background: protocompile fans compilation out across
// goroutines and joins them, and the registry client is plain HTTP. A leak would therefore
// mean a compile or a registry call that was abandoned rather than cancelled, which in a
// run with many topics multiplies by the topic count.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
