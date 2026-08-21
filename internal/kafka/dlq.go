package kafka

import (
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// DLQTopicSuffix is appended to a topic name to form its dead-letter topic. khaos never
// creates these topics: they materialize through broker auto-creation the first time a
// message is routed to one, and on a cluster with auto.create.topics.enable=false the DLQ
// send fails and the scenario's dlq_sent counter stays at zero.
const DLQTopicSuffix = "-dlq"

// DLQTopic returns the dead-letter topic name for an original topic.
func DLQTopic(original string) string {
	return original + DLQTopicSuffix
}

// NewDLQProducer builds the client used to republish messages that failed simulated
// processing. It is a separate client from the scenario producer, configured at acks=all so
// a dead-letter record is not itself lost -- the one producer in khaos that does not need
// the DisableIdempotentWrite dance in NewProducer.
//
// Retries use franz-go's default RetryTimeout (30s) rather than a fixed attempt count: for
// a dead-letter path, "keep trying for 30 seconds" fits the intent better than any
// particular attempt count would.
func NewDLQProducer(cfg Config, extra ...kgo.Opt) (*kgo.Client, error) {
	base, err := cfg.baseOptions()
	if err != nil {
		return nil, fmt.Errorf("DLQ producer client config: %w", err)
	}

	opts := append(base,
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(Partitioner),
	)

	cl, err := kgo.NewClient(append(opts, extra...)...)
	if err != nil {
		return nil, fmt.Errorf("create DLQ producer for %s: %w",
			strings.Join(cfg.BootstrapServers, ","), err)
	}
	return cl, nil
}
