package kafka

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// newFakeCluster starts an in-process Kafka and returns a Config pointing at it.
func newFakeCluster(t *testing.T, opts ...kfake.Opt) (*kfake.Cluster, Config) {
	t.Helper()
	c, err := kfake.NewCluster(opts...)
	if err != nil {
		t.Fatalf("start fake cluster: %v", err)
	}
	t.Cleanup(c.Close)
	return c, Config{BootstrapServers: c.ListenAddrs()}
}

// TestNewProducerAcks pins that every acks level builds a working client, including the
// default "1" despite franz-go requiring acks=all for idempotency.
func TestNewProducerAcks(t *testing.T) {
	t.Parallel()
	_, cfg := newFakeCluster(t, kfake.SeedTopics(1, "acks-probe"))

	tests := []struct {
		name string
		acks string
	}{
		{name: "fire and forget", acks: "0"},
		{name: "leader only, the YAML default", acks: "1"},
		{name: "full ISR, idempotency stays on", acks: "all"},
		{name: "full ISR spelled -1", acks: "-1"},
		{name: "unset falls back to the YAML default", acks: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pc := scenario.ProducerConf{
				BatchSize:       16384,
				LingerMS:        5,
				Acks:            tt.acks,
				CompressionType: "lz4",
			}
			cl, err := NewProducer(cfg, pc)
			if err != nil {
				t.Fatalf("NewProducer(acks=%q) = %v", tt.acks, err)
			}
			defer cl.Close()

			// Construction succeeding isn't enough: prove it can actually produce at
			// this acks level.
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			res := cl.ProduceSync(ctx, &kgo.Record{Topic: "acks-probe", Value: []byte("hi")})
			if err := res.FirstErr(); err != nil {
				t.Fatalf("produce at acks=%q: %v", tt.acks, err)
			}
		})
	}
}

func TestNewProducerRejectsBadValues(t *testing.T) {
	t.Parallel()
	_, cfg := newFakeCluster(t)

	tests := []struct {
		name    string
		pc      scenario.ProducerConf
		cfg     *Config // nil means use the fake cluster's config
		wantErr string
	}{
		{
			name:    "unknown acks",
			pc:      scenario.ProducerConf{Acks: "quorum"},
			wantErr: "invalid acks",
		},
		{
			name:    "unknown compression",
			pc:      scenario.ProducerConf{Acks: "1", CompressionType: "brotli"},
			wantErr: "invalid compression_type",
		},
		{
			name:    "no bootstrap servers",
			pc:      scenario.ProducerConf{Acks: "1"},
			cfg:     &Config{},
			wantErr: "no bootstrap servers",
		},
		{
			name:    "incoherent security",
			pc:      scenario.ProducerConf{Acks: "1"},
			cfg:     &Config{BootstrapServers: []string{"localhost:9092"}, Security: Security{Protocol: ProtocolSASLSSL}},
			wantErr: "sasl_mechanism required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := cfg
			if tt.cfg != nil {
				c = *tt.cfg
			}
			cl, err := NewProducer(c, tt.pc)
			if err == nil {
				cl.Close()
				t.Fatalf("NewProducer() = nil error, want one containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("NewProducer() = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestCompressionCodecs(t *testing.T) {
	t.Parallel()
	_, cfg := newFakeCluster(t)

	for _, name := range []string{"none", "gzip", "snappy", "lz4", "zstd", "uncompressed", ""} {
		t.Run("codec_"+name, func(t *testing.T) {
			t.Parallel()
			cl, err := NewProducer(cfg, scenario.ProducerConf{Acks: "1", CompressionType: name, BatchSize: 16384})
			if err != nil {
				t.Fatalf("NewProducer(compression=%q) = %v", name, err)
			}
			cl.Close()
		})
	}
}

// TestBatchMaxBytes pins the batch_size fallback for a hand-built ProducerConf (zero
// value), which would otherwise fail client construction with an unhelpful kgo error.
func TestBatchMaxBytes(t *testing.T) {
	t.Parallel()
	_, cfg := newFakeCluster(t)

	tests := []struct {
		name string
		in   int
		want int32
	}{
		{name: "unset falls back to the YAML default", in: 0, want: defaultBatchSize},
		{name: "negative falls back to the YAML default", in: -1, want: defaultBatchSize},
		{name: "configured value passes through", in: 65536, want: 65536},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := batchMaxBytes(tt.in); got != tt.want {
				t.Fatalf("batchMaxBytes(%d) = %d, want %d", tt.in, got, tt.want)
			}
			cl, err := NewProducer(cfg, scenario.ProducerConf{Acks: "1", BatchSize: tt.in})
			if err != nil {
				t.Fatalf("NewProducer(batch_size=%d) = %v", tt.in, err)
			}
			cl.Close()
		})
	}
}

// TestNewConsumerCommitMode pins that auto-commit is on unless failure simulation is
// enabled, in which case the engine commits by hand.
func TestNewConsumerCommitMode(t *testing.T) {
	t.Parallel()
	_, cfg := newFakeCluster(t)

	tests := []struct {
		name           string
		cc             scenario.ConsumerConf
		wantAutoCommit bool
	}{
		{
			name:           "no simulation keeps auto-commit",
			cc:             scenario.ConsumerConf{},
			wantAutoCommit: true,
		},
		{
			name:           "failure rate switches to manual commit",
			cc:             scenario.ConsumerConf{FailureRate: 0.1},
			wantAutoCommit: false,
		},
		{
			name:           "commit failure rate switches to manual commit",
			cc:             scenario.ConsumerConf{CommitFailureRate: 0.1},
			wantAutoCommit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.cc.FailureSimulationEnabled(); got == tt.wantAutoCommit {
				t.Fatalf("FailureSimulationEnabled() = %v, want auto-commit %v", got, tt.wantAutoCommit)
			}
			cl, err := NewConsumer(cfg, "g", []string{"t"}, tt.cc)
			if err != nil {
				t.Fatalf("NewConsumer() = %v", err)
			}
			cl.Close()
		})
	}
}

func TestNewConsumerRejectsBadValues(t *testing.T) {
	t.Parallel()
	_, cfg := newFakeCluster(t)

	tests := []struct {
		name    string
		group   string
		topics  []string
		wantErr string
	}{
		{name: "empty group", group: "", topics: []string{"t"}, wantErr: "group id must not be empty"},
		{name: "no topics", group: "g", topics: nil, wantErr: "at least one topic"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cl, err := NewConsumer(cfg, tt.group, tt.topics, scenario.ConsumerConf{})
			if err == nil {
				cl.Close()
				t.Fatalf("NewConsumer() = nil error, want one containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("NewConsumer() = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

// TestNewConsumerRebalanceHooks pins that the optional rebalance hooks reach the group
// protocol.
func TestNewConsumerRebalanceHooks(t *testing.T) {
	t.Parallel()

	const topic = "hooked"
	_, cfg := newFakeCluster(t, kfake.SeedTopics(2, topic))

	var (
		mu       sync.Mutex
		assigned = map[string][]int32{}
	)
	got := make(chan struct{}, 1)

	cl, err := NewConsumer(cfg, "hook-group", []string{topic}, scenario.ConsumerConf{},
		WithRebalanceHooks(RebalanceHooks{
			OnAssigned: func(_ context.Context, _ *kgo.Client, a map[string][]int32) {
				mu.Lock()
				for tp, ps := range a {
					assigned[tp] = append(assigned[tp], ps...)
				}
				mu.Unlock()
				select {
				case got <- struct{}{}:
				default:
				}
			},
		}))
	if err != nil {
		t.Fatalf("NewConsumer() = %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Polling is what drives the join; without it the group never forms.
	go func() {
		for ctx.Err() == nil {
			cl.PollFetches(ctx)
		}
	}()

	select {
	case <-got:
	case <-ctx.Done():
		t.Fatal("OnAssigned was never called")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(assigned[topic]) != 2 {
		t.Fatalf("assigned partitions for %q = %v, want both partitions", topic, assigned[topic])
	}
}

func TestNewDLQProducer(t *testing.T) {
	t.Parallel()
	_, cfg := newFakeCluster(t, kfake.SeedTopics(1, DLQTopic("orders")))

	if got := DLQTopic("orders"); got != "orders-dlq" {
		t.Errorf("DLQTopic(orders) = %q, want orders-dlq", got)
	}

	// The DLQ producer is built at acks=all, so it must construct with idempotency left
	// enabled.
	cl, err := NewDLQProducer(cfg)
	if err != nil {
		t.Fatalf("NewDLQProducer() = %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: DLQTopic("orders"), Value: []byte("{}")}).FirstErr(); err != nil {
		t.Fatalf("produce to DLQ: %v", err)
	}
}
