package kafka

import (
	"hash/crc32"
	"math/rand/v2"

	"github.com/twmb/franz-go/pkg/kgo"
)

// consistentRandomPartitioner reproduces librdkafka's consistent_random algorithm: CRC32 of
// the key modulo the partition count, and a uniformly random partition when the key is null
// or empty. This keeps khaos byte-compatible with librdkafka-based producers, so identical
// keys land on identical partitions across any client talking to the same cluster.
//
// franz-go's default, StickyKeyPartitioner, uses murmur2 instead -- the same hash the Java
// client uses -- which sends every key to a different partition than this one does. Switch
// Partitioner in policy.go to kgo.StickyKeyPartitioner(nil) for that placement instead.
type consistentRandomPartitioner struct{}

func (consistentRandomPartitioner) ForTopic(string) kgo.TopicPartitioner {
	return &consistentRandomTopicPartitioner{
		// Each topic partitioner gets its own generator; only the distribution of the
		// keyless draw matters, not the exact sequence.
		//
		// rand.Uint64 here is the top-level math/rand/v2 function, safe to call from
		// several goroutines; the *rand.Rand it seeds is not -- see
		// consistentRandomTopicPartitioner.
		rnd: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
	}
}

// consistentRandomTopicPartitioner places records within one topic.
//
// The *rand.Rand is unsynchronised on purpose: kgo holds the topic's partsMu across
// ForTopic, RequiresConsistency and Partition, and ForTopic hands back a fresh
// partitioner per topic, so this generator is only ever touched by one goroutine at a
// time. It must not be shared across topics or clients -- the guarantee is per
// topic-partitioner instance, and two topics (or two clients) do partition concurrently.
type consistentRandomTopicPartitioner struct {
	rnd *rand.Rand
}

// RequiresConsistency reports whether a record must be placed deterministically.
//
// Keyed records hash; keyless records go anywhere, which is the "random" half of
// consistent_random.
func (p *consistentRandomTopicPartitioner) RequiresConsistency(r *kgo.Record) bool {
	return len(r.Key) > 0
}

func (p *consistentRandomTopicPartitioner) Partition(r *kgo.Record, n int) int {
	if n <= 0 {
		return 0
	}
	if len(r.Key) == 0 {
		return p.rnd.IntN(n)
	}
	// librdkafka: rd_crc32(key) % partition_cnt, IEEE polynomial, treated as unsigned
	// before the modulo -- as signed int32 first, roughly half of all keys would get
	// negative partitions.
	return int(crc32.ChecksumIEEE(r.Key) % uint32(n))
}
