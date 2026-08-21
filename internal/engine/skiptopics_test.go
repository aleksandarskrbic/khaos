package engine

import (
	"context"
	"testing"
	"time"

	"github.com/aleksandarskrbic/khaos/internal/kafka"
	"github.com/aleksandarskrbic/khaos/internal/scenario"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// topicExists asks the broker directly rather than going through kafka.Admin, so the test
// cannot be fooled by a bug in the same wrapper it is checking.
func topicExists(t *testing.T, addrs []string, name string) bool {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer cl.Close()

	topics, err := kadm.NewClient(cl).ListTopics(context.Background())
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	return topics.Has(name)
}

// SkipTopicCreation must make the engine issue NO topic admin calls at all.
//
// The flag exists for clusters where khaos has no CreateTopics permission -- the normal
// case on Confluent Cloud, Aiven or MSK. An earlier version conflated it with
// RecreateTopics and only skipped the DELETE half, so EnsureTopics still called
// CreateTopics and exactly the user the flag exists for still failed at startup.
//
// The proof is a cluster with auto-creation OFF and no topic pre-created: with the flag
// set, New must succeed, because nothing tried to create anything.
func TestSkipTopicCreationIssuesNoAdminCalls(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("start fake cluster: %v", err)
	}
	t.Cleanup(c.Close)

	sc := &scenario.Scenario{
		Name:   "skip",
		Topics: []scenario.Topic{jsonTopic("never-created")},
	}

	eng, err := New(context.Background(), Config{
		Kafka:             kafka.Config{BootstrapServers: c.ListenAddrs()},
		Scenarios:         []*scenario.Scenario{sc},
		Duration:          time.Second,
		Seed:              1,
		SkipTopicCreation: true,
		NoConsumers:       true,
	})
	if err != nil {
		t.Fatalf("New with SkipTopicCreation must not touch topics: %v", err)
	}
	t.Cleanup(func() { eng.closeAll(); eng.admin.Close() })

	if topicExists(t, c.ListenAddrs(), "never-created") {
		t.Error("the topic was created despite SkipTopicCreation")
	}
}

// Without the flag the topic IS created, so the test above is proving the flag rather
// than an unrelated failure to create anything.
func TestTopicsAreCreatedWithoutTheFlag(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("start fake cluster: %v", err)
	}
	t.Cleanup(c.Close)

	tp := jsonTopic("is-created")
	tp.ReplicationFactor = 1
	sc := &scenario.Scenario{Name: "create", Topics: []scenario.Topic{tp}}

	eng, err := New(context.Background(), Config{
		Kafka:       kafka.Config{BootstrapServers: c.ListenAddrs()},
		Scenarios:   []*scenario.Scenario{sc},
		Duration:    time.Second,
		Seed:        1,
		NoConsumers: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { eng.closeAll(); eng.admin.Close() })

	if !topicExists(t, c.ListenAddrs(), "is-created") {
		t.Error("topic was not created; the skip test above proves nothing")
	}
}
