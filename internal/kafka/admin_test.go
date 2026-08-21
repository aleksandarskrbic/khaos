package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// newAdmin starts an admin against the given config and closes it with the test.
func newAdmin(t *testing.T, cfg Config) *Admin {
	t.Helper()
	a, err := NewAdmin(cfg)
	if err != nil {
		t.Fatalf("NewAdmin() = %v", err)
	}
	t.Cleanup(a.Close)
	return a
}

// testCtx bounds every broker interaction so a hung request fails the test rather than the
// whole package.
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// listTopicConfigs reads back a created topic's configuration. The Source field is what
// makes the retention assertion meaningful: DYNAMIC_TOPIC_CONFIG means the create request
// carried a value, DEFAULT_CONFIG means the broker's own default is showing through.
func listTopicConfigs(t *testing.T, cfg Config, topic string) map[string]kadm.Config {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.BootstrapServers...))
	if err != nil {
		t.Fatalf("client for config read: %v", err)
	}
	defer cl.Close()

	resps, err := kadm.NewClient(cl).DescribeTopicConfigs(testCtx(t), topic)
	if err != nil {
		t.Fatalf("DescribeTopicConfigs(%q) = %v", topic, err)
	}
	rc, err := resps.On(topic, nil)
	if err != nil {
		t.Fatalf("DescribeTopicConfigs(%q) response: %v", topic, err)
	}

	out := make(map[string]kadm.Config, len(rc.Configs))
	for _, c := range rc.Configs {
		out[c.Key] = c
	}
	return out
}

func TestAdminPing(t *testing.T) {
	t.Parallel()
	_, cfg := newFakeCluster(t)

	t.Run("reachable cluster", func(t *testing.T) {
		if err := newAdmin(t, cfg).Ping(testCtx(t)); err != nil {
			t.Fatalf("Ping() = %v", err)
		}
	})

	t.Run("unreachable cluster", func(t *testing.T) {
		// Port 1 is reserved and never listening: exercises the unreachable-cluster failure path.
		a := newAdmin(t, Config{BootstrapServers: []string{"127.0.0.1:1"}})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := a.Ping(ctx); err == nil {
			t.Fatal("Ping() = nil, want a connection error")
		} else if !strings.Contains(err.Error(), "cannot reach Kafka") {
			t.Fatalf("Ping() = %v, want it to mention the cluster is unreachable", err)
		}
	})
}

// TestAdminEnsureTopics covers topic creation, including tolerance for an existing topic.
func TestAdminEnsureTopics(t *testing.T) {
	t.Parallel()
	_, cfg := newFakeCluster(t, kfake.NumBrokers(3))
	a := newAdmin(t, cfg)
	ctx := testCtx(t)

	topics := []scenario.Topic{
		{Name: "orders", Partitions: 6, ReplicationFactor: 3},
		{Name: "payments", Partitions: 2, ReplicationFactor: 1},
	}

	if err := a.EnsureTopics(ctx, topics, false); err != nil {
		t.Fatalf("EnsureTopics() = %v", err)
	}

	details, err := a.adm.ListTopics(ctx, "orders", "payments")
	if err != nil {
		t.Fatalf("ListTopics() = %v", err)
	}
	for _, want := range topics {
		d, ok := details[want.Name]
		if !ok {
			t.Fatalf("topic %q was not created", want.Name)
		}
		if got := len(d.Partitions); got != want.Partitions {
			t.Errorf("topic %q has %d partitions, want %d", want.Name, got, want.Partitions)
		}
	}

	// A second pass must be a no-op, not an error -- this is what makes create-if-absent
	// usable.
	if err := a.EnsureTopics(ctx, topics, false); err != nil {
		t.Fatalf("EnsureTopics() second call = %v, want nil (already-exists must be tolerated)", err)
	}
}

// TestAdminEnsureTopicsRetention pins retention.ms being applied to local clusters only.
func TestAdminEnsureTopicsRetention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		external      bool
		wantRetention bool
	}{
		{name: "local cluster sets retention", external: false, wantRetention: true},
		{name: "external cluster leaves retention to the operator", external: true, wantRetention: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, cfg := newFakeCluster(t)
			cfg.External = tt.external

			a := newAdmin(t, cfg)
			const topic = "retention-probe"
			if err := a.EnsureTopics(testCtx(t), []scenario.Topic{
				{Name: topic, Partitions: 1, ReplicationFactor: 1},
			}, false); err != nil {
				t.Fatalf("EnsureTopics() = %v", err)
			}

			got, ok := listTopicConfigs(t, cfg, topic)["retention.ms"]
			if !ok {
				t.Fatal("the broker reported no retention.ms at all")
			}
			setByKhaos := got.Source == kmsg.ConfigSourceDynamicTopicConfig

			if setByKhaos != tt.wantRetention {
				t.Fatalf("retention.ms source = %v (khaos set it: %v), want khaos to set it: %v",
					got.Source, setByKhaos, tt.wantRetention)
			}
			if tt.wantRetention && got.MaybeValue() != "604800000" {
				t.Fatalf("retention.ms = %q, want 604800000 (7 days)",
					got.MaybeValue())
			}
		})
	}
}

// TestAdminEnsureTopicsRecreate pins that recreate actually replaces an existing topic,
// observable through the partition count.
func TestAdminEnsureTopicsRecreate(t *testing.T) {
	t.Parallel()
	_, cfg := newFakeCluster(t)
	a := newAdmin(t, cfg)
	ctx := testCtx(t)

	const topic = "resized"

	if err := a.EnsureTopics(ctx, []scenario.Topic{
		{Name: topic, Partitions: 1, ReplicationFactor: 1},
	}, false); err != nil {
		t.Fatalf("EnsureTopics() initial = %v", err)
	}

	// Without recreate the existing 1-partition topic survives: the create comes back
	// TOPIC_ALREADY_EXISTS and is tolerated.
	if err := a.EnsureTopics(ctx, []scenario.Topic{
		{Name: topic, Partitions: 4, ReplicationFactor: 1},
	}, false); err != nil {
		t.Fatalf("EnsureTopics() without recreate = %v", err)
	}
	if got := partitionCount(t, ctx, a, topic); got != 1 {
		t.Fatalf("partitions = %d without recreate, want the existing 1 to be left alone", got)
	}

	if err := a.EnsureTopics(ctx, []scenario.Topic{
		{Name: topic, Partitions: 4, ReplicationFactor: 1},
	}, true); err != nil {
		t.Fatalf("EnsureTopics() with recreate = %v", err)
	}
	if got := partitionCount(t, ctx, a, topic); got != 4 {
		t.Fatalf("partitions = %d after recreate, want 4", got)
	}
}

// TestAdminWaitForTopicsSeesFreshMetadata pins that topic presence is checked against fresh
// broker metadata, not franz-go's 5-second metadata cache.
func TestAdminWaitForTopicsSeesFreshMetadata(t *testing.T) {
	t.Parallel()
	_, cfg := newFakeCluster(t)
	a := newAdmin(t, cfg)
	ctx := testCtx(t)

	const topic = "cache-probe"
	spec := []scenario.Topic{{Name: topic, Partitions: 1, ReplicationFactor: 1}}

	if err := a.EnsureTopics(ctx, spec, false); err != nil {
		t.Fatalf("EnsureTopics() = %v", err)
	}
	if present, err := a.topicsPresent(ctx, []string{topic}); err != nil {
		t.Fatalf("topicsPresent() = %v", err)
	} else if !present[topic] {
		t.Fatal("topicsPresent() reported a topic that was just created as absent")
	}

	if err := a.DeleteTopics(ctx, topic); err != nil {
		t.Fatalf("DeleteTopics() = %v", err)
	}
	present, err := a.topicsPresent(ctx, []string{topic})
	if err != nil {
		t.Fatalf("topicsPresent() = %v", err)
	}
	if present[topic] {
		t.Fatal("topicsPresent() reported a deleted topic as present: the answer came from the metadata cache")
	}

	start := time.Now()
	if err := a.EnsureTopics(ctx, spec, true); err != nil {
		t.Fatalf("EnsureTopics(recreate) = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("EnsureTopics(recreate) took %s against an in-process broker; "+
			"anything near 5s per phase means the metadata answers are cached", elapsed)
	}
}

func partitionCount(t *testing.T, ctx context.Context, a *Admin, topic string) int {
	t.Helper()
	details, err := a.adm.ListTopics(ctx, topic)
	if err != nil {
		t.Fatalf("ListTopics(%q) = %v", topic, err)
	}
	d, ok := details[topic]
	if !ok {
		t.Fatalf("topic %q is missing", topic)
	}
	return len(d.Partitions)
}

func TestAdminEnsureTopicsValidation(t *testing.T) {
	t.Parallel()
	_, cfg := newFakeCluster(t)
	a := newAdmin(t, cfg)

	tests := []struct {
		name    string
		topics  []scenario.Topic
		wantErr string
	}{
		{name: "nothing to do", topics: nil},
		{
			name:    "empty name",
			topics:  []scenario.Topic{{Name: "", Partitions: 1, ReplicationFactor: 1}},
			wantErr: "empty name",
		},
		{
			name:    "zero partitions",
			topics:  []scenario.Topic{{Name: "bad", Partitions: 0, ReplicationFactor: 1}},
			wantErr: "partitions must be at least 1",
		},
		{
			name:    "zero replication factor",
			topics:  []scenario.Topic{{Name: "bad", Partitions: 1, ReplicationFactor: 0}},
			wantErr: "replication_factor must be at least 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := a.EnsureTopics(testCtx(t), tt.topics, false)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("EnsureTopics() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("EnsureTopics() = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

// TestAdminDeleteTopics pins the UNKNOWN_TOPIC_OR_PART tolerance.
func TestAdminDeleteTopics(t *testing.T) {
	t.Parallel()
	_, cfg := newFakeCluster(t)
	a := newAdmin(t, cfg)
	ctx := testCtx(t)

	if err := a.EnsureTopics(ctx, []scenario.Topic{
		{Name: "doomed", Partitions: 1, ReplicationFactor: 1},
	}, false); err != nil {
		t.Fatalf("EnsureTopics() = %v", err)
	}

	if err := a.DeleteTopics(ctx, "doomed"); err != nil {
		t.Fatalf("DeleteTopics() = %v", err)
	}

	// Deleting again, and deleting something that never existed, must both be silent.
	if err := a.DeleteTopics(ctx, "doomed", "never-existed"); err != nil {
		t.Fatalf("DeleteTopics() on missing topics = %v, want nil", err)
	}

	if err := a.DeleteTopics(ctx); err != nil {
		t.Fatalf("DeleteTopics() with no names = %v, want nil", err)
	}
}

// TestAdminGroupLag drives a real produce/consume cycle so lag numbers come from the
// broker, not a mock.
func TestAdminGroupLag(t *testing.T) {
	t.Parallel()

	const (
		topic = "lagging"
		group = "lag-group"
	)
	_, cfg := newFakeCluster(t)
	a := newAdmin(t, cfg)
	ctx := testCtx(t)

	if err := a.EnsureTopics(ctx, []scenario.Topic{
		{Name: topic, Partitions: 1, ReplicationFactor: 1},
	}, false); err != nil {
		t.Fatalf("EnsureTopics() = %v", err)
	}

	prod, err := NewProducer(cfg, scenario.ProducerConf{Acks: "1", BatchSize: 16384})
	if err != nil {
		t.Fatalf("NewProducer() = %v", err)
	}
	defer prod.Close()

	const records = 5
	for i := 0; i < records; i++ {
		if err := prod.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("x")}).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	// A group that joined but committed nothing is behind by every record produced.
	cons, err := NewConsumer(cfg, group, []string{topic}, scenario.ConsumerConf{},
		WithConsumerOpts(kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())))
	if err != nil {
		t.Fatalf("NewConsumer() = %v", err)
	}
	defer cons.Close()

	fetches := cons.PollRecords(ctx, records)
	if err := fetches.Err(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if err := cons.CommitRecords(ctx, fetches.Records()...); err != nil {
		t.Fatalf("commit: %v", err)
	}

	lag, err := a.GroupLag(ctx, group)
	if err != nil {
		t.Fatalf("GroupLag() = %v", err)
	}
	parts, ok := lag[topic]
	if !ok {
		t.Fatalf("GroupLag() = %v, want an entry for %q", lag, topic)
	}
	if got := parts[0]; got != 0 {
		t.Fatalf("lag for %s[0] = %d, want 0 after committing everything", topic, got)
	}
}

// TestAdminGroupLagBehindGroup pins that a group with an assignment but no commits reports
// lag as the full log, not zero -- the normal state of a khaos consumer for the first five
// seconds of every run (auto.commit.interval.ms is 5s).
func TestAdminGroupLagBehindGroup(t *testing.T) {
	t.Parallel()

	const (
		topic   = "behind"
		group   = "behind-group"
		records = 5
	)
	_, cfg := newFakeCluster(t)
	a := newAdmin(t, cfg)
	ctx := testCtx(t)

	if err := a.EnsureTopics(ctx, []scenario.Topic{
		{Name: topic, Partitions: 1, ReplicationFactor: 1},
	}, false); err != nil {
		t.Fatalf("EnsureTopics() = %v", err)
	}

	prod, err := NewProducer(cfg, scenario.ProducerConf{Acks: "1", BatchSize: 16384})
	if err != nil {
		t.Fatalf("NewProducer() = %v", err)
	}
	defer prod.Close()

	for i := 0; i < records; i++ {
		if err := prod.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("x")}).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	// Auto-commit off: the group joins and is assigned the partition but never commits.
	cons, err := NewConsumer(cfg, group, []string{topic}, scenario.ConsumerConf{},
		WithConsumerOpts(
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.DisableAutoCommit(),
		))
	if err != nil {
		t.Fatalf("NewConsumer() = %v", err)
	}
	defer cons.Close()

	// Polling forces the join to complete; without it there are no members to be behind.
	if err := cons.PollRecords(ctx, records).Err(); err != nil {
		t.Fatalf("poll: %v", err)
	}

	lag, err := a.GroupLag(ctx, group)
	if err != nil {
		t.Fatalf("GroupLag() = %v", err)
	}
	if got := lag[topic][0]; got != records {
		t.Fatalf("lag for %s[0] = %d, want %d for a group that has committed nothing",
			topic, got, records)
	}
}

// TestAdminGroupLagUnknownGroup pins that an unknown group is an empty answer, not an
// error, since the lag poller asks on a timer before any consumer has finished joining.
func TestAdminGroupLagUnknownGroup(t *testing.T) {
	t.Parallel()
	_, cfg := newFakeCluster(t)

	lag, err := newAdmin(t, cfg).GroupLag(testCtx(t), "never-created-this-group")
	if err != nil {
		t.Fatalf("GroupLag() on an unknown group = %v, want no error", err)
	}
	if len(lag) != 0 {
		t.Fatalf("GroupLag() on an unknown group = %v, want an empty result", lag)
	}
}

// TestAdminGroupLagEmptyGroup pins that a group with committed offsets but no members --
// every consumer left, as a rebalance incident does deliberately -- still answers with lag.
func TestAdminGroupLagEmptyGroup(t *testing.T) {
	t.Parallel()

	const (
		topic   = "abandoned"
		group   = "abandoned-group"
		records = 3
	)
	_, cfg := newFakeCluster(t)
	a := newAdmin(t, cfg)
	ctx := testCtx(t)

	if err := a.EnsureTopics(ctx, []scenario.Topic{
		{Name: topic, Partitions: 1, ReplicationFactor: 1},
	}, false); err != nil {
		t.Fatalf("EnsureTopics() = %v", err)
	}

	prod, err := NewProducer(cfg, scenario.ProducerConf{Acks: "1", BatchSize: 16384})
	if err != nil {
		t.Fatalf("NewProducer() = %v", err)
	}
	defer prod.Close()

	for i := 0; i < records; i++ {
		if err := prod.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("x")}).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	cons, err := NewConsumer(cfg, group, []string{topic}, scenario.ConsumerConf{},
		WithConsumerOpts(kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())))
	if err != nil {
		t.Fatalf("NewConsumer() = %v", err)
	}
	fetches := cons.PollRecords(ctx, 1)
	if err := fetches.Err(); err != nil {
		cons.Close()
		t.Fatalf("poll: %v", err)
	}
	if err := cons.CommitRecords(ctx, fetches.Records()...); err != nil {
		cons.Close()
		t.Fatalf("commit: %v", err)
	}
	// Leaves the group, so it is Empty with commits still stored.
	cons.Close()

	lag, err := a.GroupLag(ctx, group)
	if err != nil {
		t.Fatalf("GroupLag() on an empty group = %v, want no error", err)
	}
	// Commit visibility is broker-dependent; what must never happen is an error.
	if parts, ok := lag[topic]; ok && parts[0] < 0 {
		t.Fatalf("lag for %s[0] = %d, want a non-negative value", topic, parts[0])
	}
}

func TestAdminGroupLagRejectsEmptyGroup(t *testing.T) {
	t.Parallel()
	_, cfg := newFakeCluster(t)

	if _, err := newAdmin(t, cfg).GroupLag(testCtx(t), ""); err == nil {
		t.Fatal("GroupLag(\"\") = nil error, want a validation failure")
	}
}

// TestDescribeShardErrors pins that a half-applied fanned-out request is reported as
// partial failure, not read as a clean one.
func TestDescribeShardErrors(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")

	tests := []struct {
		name     string
		err      error
		wantSubs []string
	}{
		{
			name:     "plain errors pass through",
			err:      boom,
			wantSubs: []string{"boom"},
		},
		{
			name: "partial failure is called out",
			err: &kadm.ShardErrors{
				Name:      "CreateTopics",
				AllFailed: false,
				Errs:      []kadm.ShardError{{Broker: kadm.BrokerDetail{NodeID: 2}, Err: boom}},
			},
			wantSubs: []string{"CreateTopics", "partially failed", "half applied", "broker 2", "boom"},
		},
		{
			name: "total failure is distinguished",
			err: &kadm.ShardErrors{
				Name:      "DeleteTopics",
				AllFailed: true,
				Errs: []kadm.ShardError{
					{Broker: kadm.BrokerDetail{NodeID: 1}, Err: boom},
					{Broker: kadm.BrokerDetail{NodeID: 2}, Err: boom},
				},
			},
			wantSubs: []string{"DeleteTopics", "failed on every broker", "broker 1", "broker 2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := describeShardErrors(tt.err).Error()
			for _, sub := range tt.wantSubs {
				if !strings.Contains(got, sub) {
					t.Errorf("describeShardErrors() = %q, want it to contain %q", got, sub)
				}
			}
		})
	}

	if got := describeShardErrors(boom); !errors.Is(got, boom) {
		t.Fatal("a non-shard error must be returned unwrapped so errors.Is still works")
	}
}

// TestExplain pins the substring-match table and its ordering: "timed out" is checked
// before "ssl", so a TLS handshake that merely timed out reports as a timeout.
func TestExplain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "nil", err: nil, want: ""},
		{
			name: "brokers down",
			err:  errors.New("BrokerTransportFailure: all brokers are down"),
			want: "Cannot connect to broker. Check Kafka is running and bootstrap servers are correct.",
		},
		{
			name: "dns",
			err:  errors.New("dial tcp: lookup kafka: nodename nor servname provided"),
			want: "Cannot resolve broker hostname. Check bootstrap server addresses.",
		},
		{
			name: "refused",
			err:  errors.New("dial tcp 127.0.0.1:9092: connect: connection refused"),
			want: "Connection refused. Check that Kafka broker is running on the specified port.",
		},
		{
			name: "timeout is checked before ssl",
			err:  errors.New("ssl handshake timed out"),
			want: "Connection timed out. Check network connectivity and firewall rules.",
		},
		{
			name: "sasl",
			err:  errors.New("SASL authentication failed"),
			want: "Authentication failed. Check SASL username and password.",
		},
		{
			name: "acls",
			err:  errors.New("client is not authorized to access this group"),
			want: "Not authorized. Check user permissions and ACLs.",
		},
		{
			name: "tls",
			err:  errors.New("x509: certificate signed by unknown authority"),
			want: "SSL/TLS error. Check certificate paths and permissions.",
		},
		{
			name: "unknown topic",
			err:  errors.New("UNKNOWN TOPIC or partition"),
			want: "Topic does not exist. Check topic name or create the topic first.",
		},
		{
			name: "replication factor",
			err:  errors.New("replication factor 3 larger than available brokers"),
			want: "Invalid replication factor. Cannot exceed the number of available brokers.",
		},
		{
			name: "no match passes through",
			err:  errors.New("something else entirely"),
			want: "something else entirely",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Explain(tt.err); got != tt.want {
				t.Fatalf("Explain() = %q, want %q", got, tt.want)
			}
		})
	}
}
