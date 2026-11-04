package scenario

import (
	"go.yaml.in/yaml/v3"
)

func (v *validator) flows(n *yaml.Node, path string) {
	if !isSequence(n) {
		v.errf(n, path, "Field 'flows' must be a list")
		return
	}
	for i, f := range seqItems(n) {
		v.flow(f, index(path, i))
	}
}

// flow validates one entry of `flows:`.
func (v *validator) flow(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "Flow must be an object/dict")
		return
	}
	v.knownKeys(n, path, "name", "rate", "steps", "correlation")

	switch name := mapGet(n, "name"); {
	case name == nil:
		v.errf(n, join(path, "name"), "Missing required field 'name'")
	case !isString(name):
		v.errf(name, join(path, "name"), "Field 'name' must be a string")
	}

	if rate := mapGet(n, "rate"); rate != nil {
		if r, ok := floatValue(rate); !ok || r <= 0 {
			v.errf(rate, join(path, "rate"), "Field 'rate' must be a positive number")
		}
	}
	if corr := mapGet(n, "correlation"); corr != nil {
		v.correlation(corr, join(path, "correlation"))
	}

	steps := mapGet(n, "steps")
	if steps == nil {
		v.errf(n, join(path, "steps"), "Missing required field 'steps'")
		return
	}
	if !isSequence(steps) {
		v.errf(steps, join(path, "steps"), "Field 'steps' must be a list")
		return
	}
	items := seqItems(steps)
	// A one-step flow is legal but pointless -- warning, not error.
	if len(items) < 2 {
		v.warnf(steps, join(path, "steps"),
			"Flow has fewer than 2 steps - consider using regular topics instead")
	}
	for i, step := range items {
		v.flowStep(step, index(join(path, "steps"), i), i == 0)
	}
}

// correlation validates a `correlation:` block. field_ref without a field name is the
// one hard error here.
func (v *validator) correlation(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "correlation must be an object/dict")
		return
	}
	v.knownKeys(n, path, "type", "field")

	corrType := defaultCorrelationType
	typeNode := mapGet(n, "type")
	if typeNode != nil {
		corrType = scalarText(typeNode)
	}
	if !validCorrelationTypes.has(corrType) {
		v.errf(cmpNode(typeNode, n), join(path, "type"),
			"Invalid correlation type '%s'. Valid types: %s", corrType, validCorrelationTypes)
	}
	if corrType == CorrelationFieldRef {
		switch fieldNode := mapGet(n, "field"); {
		case fieldNode == nil:
			v.errf(n, join(path, "field"),
				"Correlation type 'field_ref' requires 'field' to be specified")
		case !isString(fieldNode):
			v.errf(fieldNode, join(path, "field"), "Field 'field' must be a string")
		}
	}
}

// flowStep validates one entry of a flow's `steps:` list.
func (v *validator) flowStep(n *yaml.Node, path string, isFirst bool) {
	if !isMapping(n) {
		v.errf(n, path, "Step must be an object/dict")
		return
	}
	v.knownKeys(n, path, "topic", "event_type", "delay_ms", "fields", "consumers")

	switch topic := mapGet(n, "topic"); {
	case topic == nil:
		v.errf(n, join(path, "topic"), "Missing required field 'topic'")
	case !isString(topic):
		v.errf(topic, join(path, "topic"), "Field 'topic' must be a string")
	}
	switch event := mapGet(n, "event_type"); {
	case event == nil:
		v.errf(n, join(path, "event_type"), "Missing required field 'event_type'")
	case !isString(event):
		v.errf(event, join(path, "event_type"), "Field 'event_type' must be a string")
	}

	if delay := mapGet(n, "delay_ms"); delay != nil {
		if ms, ok := intValue(delay); !ok || ms < 0 {
			v.errf(delay, join(path, "delay_ms"), "Field 'delay_ms' must be a non-negative integer")
		}
		// The range check above and the first-step warning below are independent
		// checks against the raw value, not chained as an else-branch: a first-step
		// `delay_ms: 1.5` produces BOTH the range error and this warning. Reading the
		// value as a float here (rather than only as an int) is what surfaces the
		// warning even when the range check already failed. A non-numeric delay_ms
		// fails the isNum check below, so it produces the range error alone.
		if value, isNum := floatValue(delay); isFirst && isNum && value > 0 {
			v.warnf(delay, join(path, "delay_ms"),
				"First step has delay_ms - delay applies before flow starts")
		}
	}

	if fields := mapGet(n, "fields"); fields != nil {
		v.fields(fields, join(path, "fields"))
	}
	if consumers := mapGet(n, "consumers"); consumers != nil {
		v.stepConsumers(consumers, join(path, "consumers"))
	}
}

// stepConsumers validates a step's `consumers:` block.
func (v *validator) stepConsumers(n *yaml.Node, path string) {
	if !isMapping(n) {
		v.errf(n, path, "consumers must be an object/dict")
		return
	}
	v.knownKeys(n, path, "groups", "per_group", "delay_ms")
	v.intAtLeast(n, path, "groups", 1, "Field 'groups' must be a positive integer")
	v.intAtLeast(n, path, "per_group", 1, "Field 'per_group' must be a positive integer")
	v.intAtLeast(n, path, "delay_ms", 0, "Field 'delay_ms' must be a non-negative integer")
}
