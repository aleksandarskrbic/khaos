package main

import (
	"testing"

	"github.com/aleksandarskrbic/khaos/internal/engine"
)

// A headless daemon cannot see the TUI's broker-lag column, so the log loop has to carry
// it. The bool return is the contract: "not measured" and "measured as zero" must never
// render the same way, or a monitoring system being graded against khaos gets a confident
// 0 for a number nobody actually read.
func TestTotalBrokerLag(t *testing.T) {
	tests := []struct {
		name         string
		topics       []engine.TopicStat
		wantTotal    int64
		wantMeasured bool
	}{
		{
			name:         "polling off leaves every BrokerLag nil",
			topics:       []engine.TopicStat{{Topic: "a"}, {Topic: "b"}},
			wantMeasured: false,
		},
		{
			name:         "no topics at all",
			topics:       nil,
			wantMeasured: false,
		},
		{
			name: "summed across groups and topics",
			topics: []engine.TopicStat{
				{Topic: "a", BrokerLag: map[string]int64{"g1": 10, "g2": 5}},
				{Topic: "b", BrokerLag: map[string]int64{"g1": 7}},
			},
			wantTotal:    22,
			wantMeasured: true,
		},
		{
			name: "a real zero is measured, and must not look like unmeasured",
			topics: []engine.TopicStat{
				{Topic: "a", BrokerLag: map[string]int64{"g1": 0}},
			},
			wantTotal:    0,
			wantMeasured: true,
		},
		{
			name: "one topic measured, another not",
			topics: []engine.TopicStat{
				{Topic: "a", BrokerLag: map[string]int64{"g1": 3}},
				{Topic: "b"},
			},
			wantTotal:    3,
			wantMeasured: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			total, measured := totalBrokerLag(engine.Snapshot{Topics: tt.topics})
			if measured != tt.wantMeasured {
				t.Errorf("measured = %v, want %v", measured, tt.wantMeasured)
			}
			if measured && total != tt.wantTotal {
				t.Errorf("total = %d, want %d", total, tt.wantTotal)
			}
		})
	}
}
