package kafka

import (
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// NewProducer builds a producing client for the given scenario producer configuration:
// batch size, linger, acks and compression, plus the options forced by the
// acks/idempotency interaction described on producerAcks, plus the partitioner from
// policy.go.
//
// Any extra options are applied last and therefore win, which is how the engine attaches
// metrics hooks or a logger without this package knowing about them.
func NewProducer(cfg Config, pc scenario.ProducerConf, extra ...kgo.Opt) (*kgo.Client, error) {
	base, err := cfg.baseOptions()
	if err != nil {
		return nil, fmt.Errorf("producer client config: %w", err)
	}

	acks, idempotent, err := producerAcks(pc.Acks)
	if err != nil {
		return nil, err
	}

	codec, err := compressionCodec(pc.CompressionType)
	if err != nil {
		return nil, err
	}

	opts := append(base,
		kgo.RequiredAcks(acks),
		kgo.ProducerLinger(time.Duration(pc.LingerMS)*time.Millisecond),
		kgo.ProducerBatchCompression(codec),
		kgo.RecordPartitioner(Partitioner),

		// batch_size has no exact franz-go equivalent: librdkafka's batch.size is a
		// per-partition byte budget, while kgo.ProducerBatchMaxBytes caps the whole
		// produce request. At the default 16384 across 12 partitions, that means smaller
		// and more numerous requests for the same byte throughput -- the YAML key keeps
		// its name and value (see scenario.ProducerConf) rather than being rescaled to
		// compensate, since that would silently change what every existing scenario file
		// means.
		kgo.ProducerBatchMaxBytes(batchMaxBytes(pc.BatchSize)),
	)

	if !idempotent {
		// franz-go turns on idempotent produce by default and hard-errors with
		// "idempotency requires acks=all" at client construction. khaos' YAML default
		// for acks is "1", so without this branch nearly every scenario would fail
		// before producing a single record.
		//
		// Disabling idempotency is not enough on its own: franz-go then caps in-flight
		// produce requests per broker at 1 instead of 5, a ~5x throughput collapse with
		// no error or warning. Restoring 5 keeps the idempotent and non-idempotent paths
		// comparable.
		opts = append(opts,
			kgo.DisableIdempotentWrite(),
			kgo.MaxProduceRequestsInflightPerBroker(5),
		)
	}

	// No option is passed, so kgo blocks the producing goroutine as backpressure. See
	// BlockOnBufferFull in policy.go.

	cl, err := kgo.NewClient(append(opts, extra...)...)
	if err != nil {
		return nil, fmt.Errorf("create producer for %s: %w",
			strings.Join(cfg.BootstrapServers, ","), err)
	}
	return cl, nil
}

// defaultBatchSize is the batch_size YAML default.
const defaultBatchSize = 16384

// batchMaxBytes converts batch_size into the ProducerBatchMaxBytes argument. Zero or
// negative means the caller built a ProducerConf by hand instead of decoding a scenario, so
// the YAML default applies; passing 0 through would instead fail inside kgo.NewClient with
// an error that says nothing about batch_size.
func batchMaxBytes(batchSize int) int32 {
	if batchSize <= 0 {
		return defaultBatchSize
	}
	return int32(batchSize)
}

// producerAcks maps a scenario `acks` value to the kgo equivalent and reports whether
// idempotent production is possible at that level. All four spellings -- 0, 1, -1, all --
// are honoured case-insensitively; idempotency requires the full ISR to acknowledge, which
// is true only for acks=all.
func producerAcks(acks string) (kgo.Acks, bool, error) {
	switch strings.ToLower(strings.TrimSpace(acks)) {
	case "0":
		return kgo.NoAck(), false, nil
	case "", "1":
		// Empty means the caller built a ProducerConf by hand rather than through
		// scenario decoding; "1" is the YAML default so it is the right thing to assume.
		return kgo.LeaderAck(), false, nil
	case "all", "-1":
		return kgo.AllISRAcks(), true, nil
	default:
		return kgo.Acks{}, false, fmt.Errorf("invalid acks %q: must be 0, 1, -1 or all", acks)
	}
}

// compressionCodec maps a scenario `compression_type` value to the kgo codec. The accepted
// set is librdkafka's, minus "inherit" (meaningless outside a broker config); the YAML
// default is lz4.
func compressionCodec(name string) (kgo.CompressionCodec, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "none", "uncompressed":
		return kgo.NoCompression(), nil
	case "gzip":
		return kgo.GzipCompression(), nil
	case "snappy":
		return kgo.SnappyCompression(), nil
	case "", "lz4":
		// Empty falls through to the YAML default rather than to kgo's default of
		// [snappy, none], so a hand-built ProducerConf behaves like a scenario-loaded one.
		return kgo.Lz4Compression(), nil
	case "zstd":
		return kgo.ZstdCompression(), nil
	default:
		return kgo.CompressionCodec{}, fmt.Errorf(
			"invalid compression_type %q: must be one of none, gzip, snappy, lz4, zstd", name)
	}
}
