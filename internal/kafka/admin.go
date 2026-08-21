package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// defaultRetentionMS is the seven-day retention applied to topics created on local
// clusters; see EnsureTopics.
const defaultRetentionMS = 604800000

// Admin performs the cluster operations khaos needs outside the data path: creating and
// deleting the topics a scenario uses, checking reachability, and reading consumer-group
// lag.
type Admin struct {
	client *kgo.Client
	adm    *kadm.Client

	// external mirrors Config.External and gates retention.ms; see EnsureTopics.
	external bool
}

// NewAdmin builds an admin client. It does not probe the cluster: a constructor has no
// context.Context to bound a network call, so reachability is exposed separately as Ping
// for callers that want fail-fast behaviour. NewAdmin only validates configuration and
// allocates.
func NewAdmin(cfg Config) (*Admin, error) {
	opts, err := cfg.baseOptions()
	if err != nil {
		return nil, fmt.Errorf("admin client config: %w", err)
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create admin client for %s: %w",
			strings.Join(cfg.BootstrapServers, ","), err)
	}

	return &Admin{
		client:   cl,
		adm:      kadm.NewClient(cl),
		external: cfg.External,
	}, nil
}

// Close releases the underlying connections. Safe to call more than once.
func (a *Admin) Close() {
	if a.client != nil {
		a.client.Close()
	}
}

// Ping verifies the cluster is reachable and answering. It requests broker metadata rather
// than topic metadata: the same round trip without the per-topic payload, which is
// substantial on a large cluster.
func (a *Admin) Ping(ctx context.Context) error {
	if _, err := a.adm.BrokerMetadata(ctx); err != nil {
		// The hint is added only when it says something the raw error does not.
		if hint := Explain(err); hint != err.Error() {
			return fmt.Errorf("cannot reach Kafka: %s: %w", hint, err)
		}
		return fmt.Errorf("cannot reach Kafka: %w", err)
	}
	return nil
}

// EnsureTopics makes the scenario's topics exist with the configured partition count and
// replication factor.
//
// With recreate false, an existing topic is left as-is: the create request comes back
// TOPIC_ALREADY_EXISTS and is tolerated, so its partition count may not match the
// scenario's. With recreate true, every topic is deleted and recreated first; since
// deletion is asynchronous on the broker, this waits for the deletions to actually be
// visible before recreating, and again for the new topics to become visible, rather than
// sleeping a fixed duration and hoping.
//
// retention.ms is set on created topics only when the cluster is not external, since
// managed clusters commonly enforce retention policies that reject the override. See
// Config.External.
//
// Every topic is attempted even if one fails, and all errors are joined into the returned
// error -- partial failure is never reported as success.
func (a *Admin) EnsureTopics(ctx context.Context, topics []scenario.Topic, recreate bool) error {
	if len(topics) == 0 {
		return nil
	}

	names := make([]string, 0, len(topics))
	for _, t := range topics {
		if t.Name == "" {
			return errors.New("cannot create a topic with an empty name")
		}
		names = append(names, t.Name)
	}

	if recreate {
		if err := a.DeleteTopics(ctx, names...); err != nil {
			return fmt.Errorf("recreate: %w", err)
		}
		// Deletion is asynchronous on the broker: the response acknowledges the
		// request, not the removal. Creating immediately can lose the race and come
		// back TOPIC_ALREADY_EXISTS against the topic being deleted, leaving the old
		// partition count in place.
		if err := a.waitForTopics(ctx, names, false); err != nil {
			return fmt.Errorf("recreate: waiting for topics to be deleted: %w", err)
		}
	}

	var errs []error
	for _, t := range topics {
		if err := a.createTopic(ctx, t); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("creating topics: %w", errors.Join(errs...))
	}

	if err := a.waitForTopics(ctx, names, true); err != nil {
		return fmt.Errorf("waiting for topics to become available: %w", err)
	}
	return nil
}

// createTopic creates one topic, tolerating one that already exists.
func (a *Admin) createTopic(ctx context.Context, t scenario.Topic) error {
	partitions := t.Partitions
	if partitions < 1 {
		return fmt.Errorf("topic %q: partitions must be at least 1, got %d", t.Name, partitions)
	}
	rf := t.ReplicationFactor
	if rf < 1 {
		return fmt.Errorf("topic %q: replication_factor must be at least 1, got %d", t.Name, rf)
	}

	// See the retention note on EnsureTopics.
	var configs map[string]*string
	if !a.external {
		configs = map[string]*string{
			"retention.ms": kadm.StringPtr(strconv.Itoa(defaultRetentionMS)),
		}
	}

	resp, err := a.adm.CreateTopics(ctx, int32(partitions), int16(rf), configs, t.Name)
	if err != nil {
		return fmt.Errorf("topic %q: %w", t.Name, describeShardErrors(err))
	}

	r, ok := resp[t.Name]
	if !ok {
		return fmt.Errorf("topic %q: broker returned no result for the create request", t.Name)
	}
	if r.Err != nil {
		// An already-existing topic is not an error.
		if errors.Is(r.Err, kerr.TopicAlreadyExists) {
			return nil
		}
		if r.ErrMessage != "" {
			return fmt.Errorf("topic %q: %w (%s)", t.Name, r.Err, r.ErrMessage)
		}
		return fmt.Errorf("topic %q: %w", t.Name, r.Err)
	}
	return nil
}

// DeleteTopics deletes the named topics, tolerating ones that do not exist. Every name is
// attempted even if an earlier one fails, and all failures are returned together.
func (a *Admin) DeleteTopics(ctx context.Context, names ...string) error {
	if len(names) == 0 {
		return nil
	}

	resp, err := a.adm.DeleteTopics(ctx, names...)
	if err != nil {
		return fmt.Errorf("deleting topics: %w", describeShardErrors(err))
	}

	var errs []error
	for _, name := range names {
		r, ok := resp[name]
		if !ok {
			errs = append(errs, fmt.Errorf("topic %q: broker returned no result for the delete request", name))
			continue
		}
		if r.Err == nil || errors.Is(r.Err, kerr.UnknownTopicOrPartition) {
			continue
		}
		if r.ErrMessage != "" {
			errs = append(errs, fmt.Errorf("topic %q: %w (%s)", name, r.Err, r.ErrMessage))
			continue
		}
		errs = append(errs, fmt.Errorf("topic %q: %w", name, r.Err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("deleting topics: %w", errors.Join(errs...))
	}
	return nil
}

// waitForTopics polls cluster metadata until every named topic is present (want true) or
// gone (want false), or TopicReadyTimeout elapses.
func (a *Admin) waitForTopics(ctx context.Context, names []string, want bool) error {
	deadline := time.Now().Add(TopicReadyTimeout)

	for {
		present, err := a.topicsPresent(ctx, names)
		if err != nil {
			// Metadata is fanned out, so this can be a partial failure. Treat it as
			// "not settled yet" and retry until the deadline rather than failing the
			// run on one transient shard error.
			if time.Now().After(deadline) {
				return fmt.Errorf("listing topics: %w", describeShardErrors(err))
			}
		} else {
			pending := make([]string, 0, len(names))
			for _, n := range names {
				if present[n] != want {
					pending = append(pending, n)
				}
			}
			if len(pending) == 0 {
				return nil
			}
			if time.Now().After(deadline) {
				state := "appear"
				if !want {
					state = "disappear"
				}
				return fmt.Errorf("timed out after %s waiting for %s to %s",
					TopicReadyTimeout, strings.Join(pending, ", "), state)
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(TopicReadyPollInterval):
		}
	}
}

// topicsPresent asks the brokers, right now, which of names exist.
//
// It deliberately bypasses kadm.ListTopics, which answers from franz-go's metadata cache --
// stale by up to 5 seconds by default -- so a change might not be observed for 5 seconds
// however fast the broker applied it. A raw MetadataRequest is not cached: franz-go issues
// a real broker round trip and refreshes the cache as a side effect, which producers then
// benefit from too.
func (a *Admin) topicsPresent(ctx context.Context, names []string) (map[string]bool, error) {
	req := kmsg.NewPtrMetadataRequest()
	// Explicit rather than relying on the zero value. A metadata request that names
	// topics is exactly what triggers broker-side auto-creation, which on a cluster with
	// auto.create.topics.enable would resurrect the very topics the recreate path is
	// waiting to see deleted -- and then never observe them disappear.
	req.AllowAutoTopicCreation = false
	for _, n := range names {
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(n)
		req.Topics = append(req.Topics, rt)
	}

	resp, err := req.RequestWith(ctx, a.client)
	if err != nil {
		return nil, err
	}

	present := make(map[string]bool, len(names))
	for _, t := range resp.Topics {
		if t.Topic == nil {
			continue
		}
		// Same rule as kadm.TopicDetails.Has (kadm/metadata.go:129-132): only
		// UNKNOWN_TOPIC_OR_PARTITION means "not there". Any other error means the topic
		// exists but could not be described, which must not read as deleted.
		present[*t.Topic] = !errors.Is(kerr.ErrorForCode(t.ErrorCode), kerr.UnknownTopicOrPartition)
	}
	return present, nil
}

// GroupLag returns the group's lag as topic -> partition -> records behind the log end. It
// is admin-protocol surface (DescribeGroups + OffsetFetch + ListOffsets) that the engine
// should not have to assemble itself.
//
// Partial results are possible: a per-partition error omits that partition from the map and
// returns a non-nil error alongside whatever did resolve, so callers must check the error
// before trusting the map.
//
// An empty map with a nil error is a normal answer meaning "nothing to be behind on yet":
// the group does not exist, it exists but has no members and has committed nothing, or its
// members have joined but have no assignment yet mid-rebalance. A group that HAS members
// but has committed nothing is different -- the full log-end offset is reported as lag,
// correctly, since it has everything still to read.
func (a *Admin) GroupLag(ctx context.Context, group string) (map[string]map[int32]int64, error) {
	if group == "" {
		return nil, errors.New("consumer group id must not be empty")
	}

	lags, err := a.adm.Lag(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("fetching lag for group %q: %w", group, describeShardErrors(err))
	}

	described, ok := lags[group]
	if !ok {
		return nil, fmt.Errorf("fetching lag for group %q: broker returned no result", group)
	}
	if err := described.Error(); err != nil {
		// "Group does not exist" is an answer, not a failure -- see the doc comment above.
		if errors.Is(err, kerr.GroupIDNotFound) {
			return map[string]map[int32]int64{}, nil
		}
		return nil, fmt.Errorf("fetching lag for group %q: %w", group, err)
	}

	out := make(map[string]map[int32]int64, len(described.Lag))
	var errs []error
	for topic, parts := range described.Lag {
		for partition, l := range parts {
			if l.Err != nil {
				errs = append(errs, fmt.Errorf("%s[%d]: %w", topic, partition, l.Err))
				continue
			}
			if out[topic] == nil {
				out[topic] = make(map[int32]int64, len(parts))
			}
			out[topic][partition] = l.Lag
		}
	}

	if len(errs) > 0 {
		return out, fmt.Errorf("lag for group %q is incomplete: %w", group, errors.Join(errs...))
	}
	return out, nil
}

// describeShardErrors turns kadm's ShardErrors into something a user can act on.
//
// Requests that kadm fans out to several brokers can half-succeed; kadm signals that by
// returning a *kadm.ShardErrors whose AllFailed field says whether anything got through.
// Its own Error() string reports only the count and the first error, which hides both the
// partial-success fact and the other brokers. Since a partially applied topic operation
// must never look like a clean one, the detail is unpacked here.
//
// Errors that are not ShardErrors pass through untouched.
func describeShardErrors(err error) error {
	var se *kadm.ShardErrors
	if !errors.As(err, &se) {
		return err
	}

	parts := make([]string, 0, len(se.Errs))
	for _, e := range se.Errs {
		parts = append(parts, fmt.Sprintf("broker %d: %v", e.Broker.NodeID, e.Err))
	}

	scope := "partially failed (some brokers succeeded, the operation is half applied)"
	if se.AllFailed {
		scope = "failed on every broker"
	}
	return fmt.Errorf("%s %s: %s", se.Name, scope, strings.Join(parts, "; "))
}
