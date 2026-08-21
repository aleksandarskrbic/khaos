package kafka

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// This file collects every client-behaviour choice where franz-go's default differs from
// librdkafka's, as package-level vars: changing khaos' behaviour is one edit here, not a
// hunt through producer.go/consumer.go/admin.go. Each entry names both options and what
// flipping it costs.

// Partitioner decides which partition a keyed record lands on: CRC32 of the key modulo the
// partition count, random when there is no key -- librdkafka's `consistent_random`. See
// partitioner.go.
//
// Set kgo.StickyKeyPartitioner(nil) instead for franz-go's default -- murmur2, the same
// hash the Java client uses, which is verifiable with standard Kafka tooling but sends
// every key to a different partition than this default does.
var Partitioner kgo.Partitioner = consistentRandomPartitioner{}

// Balancers is the consumer-group partition assignment strategy: range and round-robin,
// both EAGER protocols where every rebalance revokes every partition from every member
// before reassigning ("stop the world"). This is deliberate: khaos ships scenarios
// (rebalance-storm, uneven-assignment) whose entire purpose is to exercise rebalance pain,
// and franz-go's cooperative-sticky default would make those rebalances measurably gentler
// and no longer representative of the failure they demonstrate.
//
//   - kgo.RangeBalancer(), kgo.RoundRobinBalancer() (current): eager. Range is first, so
//     it is the one negotiated when all members agree.
//   - kgo.CooperativeStickyBalancer(): fewer stalls and a smoother ride, but the
//     rebalance-storm and uneven-assignment scenarios stop being representative. Eager and
//     cooperative balancers must NOT be mixed within one group.
var Balancers = []kgo.GroupBalancer{kgo.RangeBalancer(), kgo.RoundRobinBalancer()}

// RecreateTopicsByDefault controls whether a run destroys the topics it uses.
//
// Deleting and recreating every topic on every run is harmless against the bundled local
// Docker cluster, but pointed at a shared or staging cluster via
// `khaos simulate --bootstrap-servers ...` it silently deletes other people's data in
// topics that merely share a name.
//
//   - true (current): every run starts from empty topics, which is what makes lag numbers
//     readable from the first message. Opt out per run with --no-recreate-topics.
//   - false: EnsureTopics only creates what is missing and leaves existing data alone --
//     safer against shared clusters.
var RecreateTopicsByDefault = true

// BlockOnBufferFull is what a producer does when its memory buffer is full. franz-go's
// default is to block the producing goroutine until buffer space frees up: real
// backpressure, where the achieved rate drops to what the broker can absorb and the load
// generator keeps generating load.
//
//   - true (current): kgo's default. No option is passed; this var documents the choice.
//   - false: would require kgo.MaxBufferedRecords plus handling of kgo.ErrMaxBuffered to
//     fail fast instead. Only worth doing if a full buffer must be a hard, visible failure
//     rather than a slowdown.
var BlockOnBufferFull = true

// TopicReadyTimeout bounds the metadata poll that EnsureTopics runs after creating topics,
// rather than sleeping a fixed duration and hoping metadata has propagated -- too long on
// a small cluster, and too short on a loaded multi-broker one, where producers then start
// against unpropagated metadata and eat UNKNOWN_TOPIC_OR_PARTITION.
var TopicReadyTimeout = 30 * time.Second

// TopicReadyPollInterval is how often the post-create metadata poll re-checks.
var TopicReadyPollInterval = 250 * time.Millisecond
