package kafka

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if any test leaves a goroutine behind. Every constructor here
// hands back a *kgo.Client backed by several background goroutines, and forgetting to Close
// one leaks silently -- exactly the defect a load generator cannot afford.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
