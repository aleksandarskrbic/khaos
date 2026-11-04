package scenario

import (
	"strings"

	"go.yaml.in/yaml/v3"
)

// incidentKeys is the union of every key any incident type reads, plus `group`.
var incidentKeys = []string{
	"type", "group", "at_seconds", "every_seconds", "initial_delay_seconds",
	"target", "broker", "duration_seconds", "delay_ms", "rate",
}

// incident validates one entry of the top-level `incidents:` list, which is either a
// standalone incident or a group wrapper.
func (v *validator) incident(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "Incident must be an object/dict")
		return
	}
	// A `group` key makes the whole entry a group and nothing else on it is read.
	if group := mapGet(n, "group"); group != nil {
		v.incidentGroup(group, join(path, "group"))
		return
	}
	v.knownKeys(n, path, incidentKeys...)

	incidentType, ok := v.incidentType(n, path)
	if !ok {
		return
	}

	// Standalone incidents need a schedule; group members do not.
	at, every := mapGet(n, "at_seconds"), mapGet(n, "every_seconds")
	if at == nil && every == nil {
		v.errf(n, path, "Incident must have either 'at_seconds' or 'every_seconds'")
	}
	if at != nil && !isInt(at) {
		v.errf(at, join(path, "at_seconds"), "Field 'at_seconds' must be an integer")
	}
	if every != nil {
		if seconds, ok := intValue(every); !ok || seconds < 1 {
			v.errf(every, join(path, "every_seconds"),
				"Field 'every_seconds' must be a positive integer")
		}
	}

	if target := mapGet(n, "target"); target != nil {
		v.target(target, join(path, "target"))
	}
	v.incidentTypeFields(n, incidentType, path)
}

// incidentType reads and checks the `type` key shared by both incident paths.
func (v *validator) incidentType(n *yaml.Node, path string) (string, bool) {
	typeNode := mapGet(n, "type")
	if typeNode == nil {
		v.errf(n, join(path, "type"), "Missing required field 'type'")
		return "", false
	}
	incidentType := scalarText(typeNode)
	if !validIncidentTypeNames.has(incidentType) {
		v.errf(typeNode, join(path, "type"),
			"Unknown incident type '%s'. Valid types: %s", incidentType, validIncidentTypeNames)
		return "", false
	}
	return incidentType, true
}

// incidentTypeFields checks the parameters each incident type requires: that the key is
// present, and that its value is in range. Both checks apply wherever an incident
// appears, standalone or inside a group.
//
// The two used to be split by a `strict` parameter that standalone incidents passed true
// and group members passed false, so a group member could carry `delay_ms: -5` while the
// identical standalone incident was rejected. That was not merely untidy: `rate: -5`
// inside a group reached Producer.SetRate, which reads any value <= 0 as rate.Inf, so an
// out-of-range value in a group silently unlimited the producer it targeted.
func (v *validator) incidentTypeFields(n *yaml.Node, incidentType, path string) {
	switch incidentType {
	case IncidentStopBroker, IncidentStartBroker:
		broker := mapGet(n, "broker")
		if broker == nil {
			v.errf(n, join(path, "broker"),
				"Incident type '%s' requires 'broker' field", incidentType)
		} else if !Cluster.AllowsBroker(scalarText(broker)) {
			v.errf(broker, join(path, "broker"), "Invalid broker. Valid: %s", Cluster.brokerList())
		}

	case IncidentIncreaseConsumerDelay:
		delay := mapGet(n, "delay_ms")
		if delay == nil {
			v.errf(n, join(path, "delay_ms"),
				"Incident type 'increase_consumer_delay' requires 'delay_ms' field")
		} else {
			if ms, ok := intValue(delay); !ok || ms < 0 {
				v.errf(delay, join(path, "delay_ms"),
					"Field 'delay_ms' must be a non-negative integer")
			}
		}

	case IncidentChangeProducerRate:
		rate := mapGet(n, "rate")
		if rate == nil {
			v.errf(n, join(path, "rate"),
				"Incident type 'change_producer_rate' requires 'rate' field")
		} else {
			if r, ok := floatValue(rate); !ok || r < 0 {
				v.errf(rate, join(path, "rate"), "Field 'rate' must be a non-negative number")
			}
		}

	case IncidentPauseConsumer:
		duration := mapGet(n, "duration_seconds")
		if duration == nil {
			v.errf(n, join(path, "duration_seconds"),
				"Incident type 'pause_consumer' requires 'duration_seconds' field")
		} else {
			if seconds, ok := intValue(duration); !ok || seconds < 1 {
				v.errf(duration, join(path, "duration_seconds"),
					"Field 'duration_seconds' must be a positive integer")
			}
		}
	}
}

// target validates one `target:` block. Used for both consumer and producer targets;
// producers have no group, but this does not distinguish, so a `group` on a producer
// target is accepted here and then ignored at decode.
func (v *validator) target(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "Target must be an object/dict")
		return
	}
	v.knownKeys(n, path, "topic", "group", "group_id", "percentage", "count", "indices")

	if topic := mapGet(n, "topic"); topic != nil && !isString(topic) {
		v.errf(topic, join(path, "topic"), "Field 'topic' must be a string")
	}
	// `group_id` is validated alongside `group` because decode accepts both -- see
	// rawTarget.GroupID in decode.go.
	for _, key := range []string{"group", "group_id"} {
		if group := mapGet(n, key); group != nil && !isString(group) {
			v.errf(group, join(path, key), "Field '%s' must be a string", key)
		}
	}

	if pct := mapGet(n, "percentage"); pct != nil {
		if value, ok := intValue(pct); !ok || value < 1 || value > 100 {
			v.errf(pct, join(path, "percentage"),
				"Field 'percentage' must be an integer between 1 and 100")
		}
	}
	if count := mapGet(n, "count"); count != nil {
		if value, ok := intValue(count); !ok || value < 1 {
			v.errf(count, join(path, "count"), "Field 'count' must be a positive integer")
		}
	}
	if indices := mapGet(n, "indices"); indices != nil {
		if !isSequence(indices) {
			v.errf(indices, join(path, "indices"), "Field 'indices' must be a list")
		} else {
			for _, item := range seqItems(indices) {
				if value, ok := intValue(item); !ok || value < 0 {
					v.errf(indices, join(path, "indices"),
						"Field 'indices' must be a list of non-negative integers")
					break
				}
			}
		}
	}

	// The three selection keys are mutually exclusive.
	var set []string
	for _, key := range []string{"percentage", "count", "indices"} {
		if mapGet(n, key) != nil {
			set = append(set, key)
		}
	}
	if len(set) > 1 {
		v.errf(n, path, "Only one of percentage, count, indices can be set, got: %s",
			strings.Join(set, ", "))
	}
}

// incidentGroup validates a `group:` block.
//
// repeat and interval_seconds are REQUIRED here even though decode defaults them to 1
// and 60 when missing; validation is stricter than decode and that is preserved.
func (v *validator) incidentGroup(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "Incident group must be an object/dict")
		return
	}
	v.knownKeys(n, path, "repeat", "interval_seconds", "incidents")

	switch repeat := mapGet(n, "repeat"); {
	case repeat == nil:
		v.errf(n, join(path, "repeat"), "Missing required field 'repeat'")
	default:
		if value, ok := intValue(repeat); !ok || value < 1 {
			v.errf(repeat, join(path, "repeat"), "Field 'repeat' must be a positive integer")
		}
	}

	intervalNode := mapGet(n, "interval_seconds")
	switch {
	case intervalNode == nil:
		v.errf(n, join(path, "interval_seconds"), "Missing required field 'interval_seconds'")
	default:
		if value, ok := intValue(intervalNode); !ok || value < 1 {
			v.errf(intervalNode, join(path, "interval_seconds"),
				"Field 'interval_seconds' must be a positive integer")
		}
	}

	incidents := mapGet(n, "incidents")
	switch {
	case incidents == nil:
		v.errf(n, join(path, "incidents"), "Missing required field 'incidents'")
	case !isSequence(incidents):
		v.errf(incidents, join(path, "incidents"), "Field 'incidents' must be a list")
	case len(seqItems(incidents)) == 0:
		v.errf(incidents, join(path, "incidents"), "Incident group must have at least one incident")
	default:
		for i, inc := range seqItems(incidents) {
			v.groupIncident(inc, index(join(path, "incidents"), i))
		}
	}

	// A member scheduled at or past the cycle length overlaps the next repetition --
	// warning only.
	if interval, ok := intValue(intervalNode); ok && isSequence(incidents) {
		for i, inc := range seqItems(incidents) {
			atNode := mapGet(inc, "at_seconds")
			if at, isInt := intValue(atNode); isInt && at >= interval {
				v.warnf(atNode, join(index(join(path, "incidents"), i), "at_seconds"),
					"at_seconds (%d) >= interval (%d), may overlap", at, interval)
			}
		}
	}
}

// groupIncident validates one member of a `group:` block's `incidents:` list. Differs
// from a standalone incident in two ways: no schedule is required (a missing at_seconds
// is a warning), and every_seconds is not checked at all. Type-specific field values are
// checked exactly as they are outside a group.
func (v *validator) groupIncident(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "Incident must be an object/dict")
		return
	}
	v.knownKeys(n, path, incidentKeys...)

	incidentType, ok := v.incidentType(n, path)
	if !ok {
		return
	}

	at := mapGet(n, "at_seconds")
	if at == nil {
		v.warnf(n, path, "Group incidents should use 'at_seconds' (relative to cycle start)")
	} else if !isInt(at) {
		v.errf(at, join(path, "at_seconds"), "Field 'at_seconds' must be an integer")
	}

	if target := mapGet(n, "target"); target != nil {
		v.target(target, join(path, "target"))
	}
	v.incidentTypeFields(n, incidentType, path)
}
