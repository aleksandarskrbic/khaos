package engine

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// registry owns every live producer and consumer.
//
// There is exactly one map per kind and every "index" is a predicate over it, rather than
// a set of hand-maintained by-topic/by-group indexes that could silently diverge from the
// source map. The cost is a linear scan on incident targeting, which happens a handful of
// times per run against a few hundred components -- utterly irrelevant -- in exchange for
// making that class of divergence bug unrepresentable.
type registry struct {
	mu        sync.RWMutex
	producers map[scenario.ID]*Producer
	consumers map[scenario.ID]*Consumer

	// order preserves registration order so that scenario.ConsumerTarget.Indices
	// resolves deterministically. Positions are append-only, so a recreated consumer
	// never reuses a departed one's index.
	producerOrder []scenario.ID
	consumerOrder []scenario.ID

	nextID atomic.Int64
}

func newRegistry() *registry {
	return &registry{
		producers: make(map[scenario.ID]*Producer),
		consumers: make(map[scenario.ID]*Consumer),
	}
}

// mintID returns a process-unique handle. Prefixed by kind purely so that event messages
// and logs are readable.
func (r *registry) mintID(kind string) scenario.ID {
	return scenario.ID(fmt.Sprintf("%s-%d", kind, r.nextID.Add(1)))
}

func (r *registry) addProducer(p *Producer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.producers[p.ID] = p
	r.producerOrder = append(r.producerOrder, p.ID)
}

func (r *registry) addConsumer(c *Consumer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.consumers[c.ID] = c
	r.consumerOrder = append(r.consumerOrder, c.ID)
}

// removeConsumer drops a consumer from the registry.
//
// The ID is not reused and the ordering slot is left in place, so index-based targeting
// keeps referring to the same logical position while a departed consumer simply stops
// matching anything.
func (r *registry) removeConsumer(id scenario.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.consumers, id)
}

func (r *registry) producer(id scenario.ID) (*Producer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.producers[id]
	return p, ok
}

func (r *registry) consumer(id scenario.ID) (*Consumer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.consumers[id]
	return c, ok
}

// allProducers returns live producers in registration order.
func (r *registry) allProducers() []*Producer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Producer, 0, len(r.producers))
	for _, id := range r.producerOrder {
		if p, ok := r.producers[id]; ok {
			out = append(out, p)
		}
	}
	return out
}

// allConsumers returns live consumers in registration order.
func (r *registry) allConsumers() []*Consumer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Consumer, 0, len(r.consumers))
	for _, id := range r.consumerOrder {
		if c, ok := r.consumers[id]; ok {
			out = append(out, c)
		}
	}
	return out
}

// incidentContext builds the value view that incidents expand against.
//
// Incidents never see live components -- only these plain refs -- which is what keeps the
// whole incident layer unit-testable without a broker.
func (r *registry) incidentContext(rebalances int) *scenario.Context {
	producers := r.allProducers()
	consumers := r.allConsumers()

	pRefs := make([]scenario.ProducerRef, len(producers))
	for i, p := range producers {
		pRefs[i] = scenario.ProducerRef{ID: p.ID, Topic: p.Topic}
	}
	cRefs := make([]scenario.ConsumerRef, len(consumers))
	for i, c := range consumers {
		cRefs[i] = scenario.ConsumerRef{
			ID:                c.ID,
			Topic:             c.Topic,
			GroupID:           c.GroupID,
			Topics:            c.Topics,
			Conf:              c.Conf,
			ProcessingDelayMS: c.processingDelayMS(),
		}
	}

	return &scenario.Context{
		Consumers:      cRefs,
		Producers:      pRefs,
		RebalanceCount: rebalances,
	}
}
