package kafka

import (
	"context"
	"hash/crc32"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// TestConsistentRandomMatchesLibrdkafka pins CRC32 key placement; changing it silently
// moves every key to a different partition.
func TestConsistentRandomMatchesLibrdkafka(t *testing.T) {
	tp := consistentRandomPartitioner{}.ForTopic("orders")

	for _, key := range []string{"key-0", "key-7", "hot-key", "user-12345", ""} {
		rec := &kgo.Record{Key: []byte(key)}
		if key == "" {
			if tp.RequiresConsistency(rec) {
				t.Error("empty key should not require consistency")
			}
			continue
		}
		if !tp.RequiresConsistency(rec) {
			t.Errorf("key %q should require consistency", key)
		}
		want := int(crc32.ChecksumIEEE([]byte(key)) % 12)
		for i := 0; i < 5; i++ {
			if got := tp.Partition(rec, 12); got != want {
				t.Fatalf("key %q -> partition %d, want %d (not stable or not CRC32)", key, got, want)
			}
		}
	}
}

// TestConsistentRandomNeverNegative pins the unsigned modulo: a signed CRC32 would yield
// negative partitions for about half of all keys.
func TestConsistentRandomNeverNegative(t *testing.T) {
	tp := consistentRandomPartitioner{}.ForTopic("t")
	for i := 0; i < 5000; i++ {
		key := []byte(string(rune('a'+i%26)) + string(rune('A'+i%26)) + string(rune(i)))
		if p := tp.Partition(&kgo.Record{Key: key}, 12); p < 0 || p >= 12 {
			t.Fatalf("partition %d out of range for key %q", p, key)
		}
	}
}

// TestConsistentRandomSpreadsKeylessRecords asserts the "random" half.
func TestConsistentRandomSpreadsKeylessRecords(t *testing.T) {
	tp := consistentRandomPartitioner{}.ForTopic("t")
	seen := make(map[int]bool)
	for i := 0; i < 2000; i++ {
		seen[tp.Partition(&kgo.Record{}, 12)] = true
	}
	if len(seen) < 10 {
		t.Errorf("keyless records hit only %d of 12 partitions", len(seen))
	}
}

// TestConsistentRandomIsSafeUnderConcurrentProduce pins that the per-topic RNG is race-free
// under concurrent produce across topics and clients (meaningful only under -race).
func TestConsistentRandomIsSafeUnderConcurrentProduce(t *testing.T) {
	t.Parallel()

	topics := []string{"spread-a", "spread-b"}
	_, cfg := newFakeCluster(t, kfake.SeedTopics(4, topics...))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for c := 0; c < 2; c++ {
		cl, err := NewProducer(cfg, scenario.ProducerConf{Acks: "1", BatchSize: 16384})
		if err != nil {
			t.Fatalf("NewProducer() = %v", err)
		}
		defer cl.Close()

		for _, topic := range topics {
			for g := 0; g < 2; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 50; i++ {
						// No key: this is the branch that draws from the generator.
						r := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("x")})
						if err := r.FirstErr(); err != nil {
							select {
							case errs <- err:
							default:
							}
							return
						}
					}
				}()
			}
		}
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		t.Fatalf("concurrent produce: %v", err)
	}
}
