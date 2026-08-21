package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// Consumer group timings. scenario.ConsumerConf carries no timing fields -- these are fixed
// constants that no scenario file can override.
const (
	// sessionTimeout is session.timeout.ms, how long the coordinator waits for a
	// heartbeat before evicting a member.
	sessionTimeout = 45 * time.Second

	// rebalanceTimeout is max.poll.interval.ms. librdkafka's max.poll.interval.ms and
	// kgo's RebalanceTimeout are the same budget seen from two sides: the longest a
	// member may take between polls, which is also the longest the group will wait for
	// it to rejoin a rebalance.
	rebalanceTimeout = 5 * time.Minute

	// autoCommitInterval is auto.commit.interval.ms.
	autoCommitInterval = 5 * time.Second
)

// RebalanceHooks are optional callbacks invoked when the group reassigns partitions. They
// exist so the engine can observe and report what the rebalance-storm and
// uneven-assignment scenarios do, and are entirely optional so plain consumption is
// unaffected.
//
// The callbacks run on the client's internal goroutine and block the rebalance while they
// do, so they must be quick and must not call back into the client.
type RebalanceHooks struct {
	// OnAssigned fires after partitions have been assigned to this member.
	OnAssigned func(ctx context.Context, cl *kgo.Client, assigned map[string][]int32)

	// OnRevoked fires before partitions are taken away, while they are still owned.
	OnRevoked func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32)
}

// ConsumerOption customises a consumer beyond what the scenario configuration specifies.
type ConsumerOption func(*consumerSettings)

type consumerSettings struct {
	hooks RebalanceHooks
	extra []kgo.Opt
}

// WithRebalanceHooks attaches partition assignment/revocation callbacks.
func WithRebalanceHooks(h RebalanceHooks) ConsumerOption {
	return func(s *consumerSettings) { s.hooks = h }
}

// WithConsumerOpts appends raw kgo options, applied last so they override everything else.
//
// This is the seam for cross-cutting concerns the engine owns -- metrics hooks, a logger,
// an instance id -- without this package taking a dependency on them.
func WithConsumerOpts(opts ...kgo.Opt) ConsumerOption {
	return func(s *consumerSettings) { s.extra = append(s.extra, opts...) }
}

// NewConsumer builds a group-consuming client. The balancers deliberately keep the eager
// range/round-robin pair rather than franz-go's cooperative-sticky default; see Balancers
// in policy.go.
//
// Auto-commit is disabled exactly when failure simulation is on, so a simulated processing
// failure can withhold the offset; committing then becomes the engine's job, and this
// client will never commit on its own, including on close.
func NewConsumer(cfg Config, groupID string, topics []string, cc scenario.ConsumerConf, options ...ConsumerOption) (*kgo.Client, error) {
	if groupID == "" {
		return nil, errors.New("consumer group id must not be empty")
	}
	if len(topics) == 0 {
		return nil, errors.New("consumer must subscribe to at least one topic")
	}

	base, err := cfg.baseOptions()
	if err != nil {
		return nil, fmt.Errorf("consumer client config: %w", err)
	}

	var settings consumerSettings
	for _, o := range options {
		o(&settings)
	}

	opts := append(base,
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topics...),
		kgo.Balancers(Balancers...),
		kgo.SessionTimeout(sessionTimeout),
		kgo.RebalanceTimeout(rebalanceTimeout),

		// auto.offset.reset is always "latest": khaos' YAML schema has no
		// auto_offset_reset key, so every scenario-loaded consumer starts at the end of
		// the log. That is also what a load generator wants -- starting at the beginning
		// would spend the run draining whatever a previous run left behind instead of
		// measuring this run's traffic.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)

	if cc.FailureSimulationEnabled() {
		opts = append(opts, kgo.DisableAutoCommit())
	} else {
		opts = append(opts, kgo.AutoCommitInterval(autoCommitInterval))
	}

	if settings.hooks.OnAssigned != nil {
		opts = append(opts, kgo.OnPartitionsAssigned(settings.hooks.OnAssigned))
	}
	if settings.hooks.OnRevoked != nil {
		opts = append(opts, kgo.OnPartitionsRevoked(settings.hooks.OnRevoked))
	}

	cl, err := kgo.NewClient(append(opts, settings.extra...)...)
	if err != nil {
		return nil, fmt.Errorf("create consumer %q for %s: %w",
			groupID, strings.Join(cfg.BootstrapServers, ","), err)
	}
	return cl, nil
}
