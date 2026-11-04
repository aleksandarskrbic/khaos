package scenario

import (
	"go.yaml.in/yaml/v3"
)

func (v *validator) fields(n *yaml.Node, path string) {
	if !isSequence(n) {
		v.errf(n, path, "fields must be a list")
		return
	}
	for i, f := range seqItems(n) {
		v.field(f, index(path, i), false)
	}
}

// field validates one field definition, recursing into nested objects and array items.
//
// asItem distinguishes a named field from an array's `items:` schema, which differ in
// two ways preserved verbatim: an array item needs no `name`, and an array item of type
// faker is NOT checked for a `provider`, because the dispatch below skips fakerField
// when asItem is set.
//
// A YAML anchor can make a field definition contain itself:
//
//	fields:
//	  - &f
//	    name: a
//	    type: array
//	    items: *f
//
// Without a guard that recurses forever, appending another ".items" to the diagnostic
// path on each pass until the process is killed. Nodes on the current recursion stack
// are therefore tracked and re-entry is reported as an error. A finite tree, however
// deeply nested, is unaffected: only an alias can revisit a node.
func (v *validator) field(n *yaml.Node, path string, asItem bool) {
	n = resolveNode(n)
	if v.visiting[n] {
		v.errf(n, path, "Field definition refers to itself through a YAML anchor")
		return
	}
	if v.visiting == nil {
		v.visiting = make(map[*yaml.Node]bool)
	}
	v.visiting[n] = true
	defer delete(v.visiting, n)

	if !isMapping(n) {
		if asItem {
			v.errf(n, path, "Array item must be an object/dict")
		} else {
			v.errf(n, path, "Field must be an object/dict")
		}
		return
	}
	v.knownKeys(n, path, "name", "type", "min", "max", "min_length", "max_length",
		"cardinality", "values", "fields", "items", "min_items", "max_items",
		"provider", "locale")

	if !asItem {
		switch name := mapGet(n, "name"); {
		case name == nil:
			v.errf(n, join(path, "name"), "Missing required field 'name'")
		case !isString(name):
			v.errf(name, join(path, "name"), "Field 'name' must be a string")
		}
	}

	typeNode := mapGet(n, "type")
	if typeNode == nil {
		v.errf(n, join(path, "type"), "Missing required field 'type'")
		return
	}
	if !isString(typeNode) {
		v.errf(typeNode, join(path, "type"), "Field 'type' must be a string")
		return
	}
	fieldType := typeNode.Value
	if !ValidFieldTypes[fieldType] {
		v.errf(typeNode, join(path, "type"),
			"Unknown field type '%s'. Valid types: %s", fieldType, sortedFieldTypes)
		return
	}

	switch fieldType {
	case FieldString:
		v.stringField(n, path)
	case FieldInt:
		v.numericRange(n, path)
		v.cardinality(n, path)
	case FieldFloat:
		// No cardinality check for floats.
		v.numericRange(n, path)
	case FieldEnum:
		v.enumField(n, path)
	case FieldObject:
		v.objectField(n, path)
	case FieldArray:
		v.arrayField(n, path)
	case FieldFaker:
		if !asItem {
			v.fakerField(n, path)
		}
	}
}

// stringField validates the string-only constraints.
func (v *validator) stringField(n *yaml.Node, path string) {
	v.intAtLeast(n, path, "min_length", 0, "Field 'min_length' must be a non-negative integer")
	v.intAtLeast(n, path, "max_length", 1, "Field 'max_length' must be a positive integer")
	// The defaults here (0 and 100) are the validator's own; the field model has no
	// length defaults at all.
	v.pairing(n, path, "min_length", "max_length", 0, 100,
		"min_length cannot be greater than max_length")
	v.cardinality(n, path)
}

// cardinality validates the shared `cardinality:` key.
func (v *validator) cardinality(n *yaml.Node, path string) {
	v.intAtLeast(n, path, "cardinality", 1, "Field 'cardinality' must be a positive integer")
}

// numericRange validates min/max: they may be int or float, and are only compared when
// both are present.
func (v *validator) numericRange(n *yaml.Node, path string) {
	minNode, maxNode := mapGet(n, "min"), mapGet(n, "max")
	minOK, maxOK := true, true
	if minNode != nil && !isNumber(minNode) {
		v.errf(minNode, join(path, "min"), "Field 'min' must be a number")
		minOK = false
	}
	if maxNode != nil && !isNumber(maxNode) {
		v.errf(maxNode, join(path, "max"), "Field 'max' must be a number")
		maxOK = false
	}
	if minNode == nil || maxNode == nil || !minOK || !maxOK {
		return
	}
	minVal, _ := floatValue(minNode)
	maxVal, _ := floatValue(maxNode)
	if minVal > maxVal {
		v.errf(minNode, path, "min cannot be greater than max")
	}
}

// enumField validates an enum field's `values:` list.
func (v *validator) enumField(n *yaml.Node, path string) {
	values := mapGet(n, "values")
	if values == nil {
		v.errf(n, join(path, "values"), "Enum field requires 'values' list")
		return
	}
	if !isSequence(values) {
		v.errf(values, join(path, "values"), "Field 'values' must be a list")
		return
	}
	items := seqItems(values)
	if len(items) == 0 {
		v.errf(values, join(path, "values"), "Enum must have at least one value")
	}
	for i, item := range items {
		if !isString(item) {
			v.errf(item, index(join(path, "values"), i), "Enum values must be strings")
		}
	}
}

// objectField validates an object field's nested `fields:` list.
func (v *validator) objectField(n *yaml.Node, path string) {
	fields := mapGet(n, "fields")
	if fields == nil {
		v.errf(n, join(path, "fields"), "Object field requires 'fields' list")
		return
	}
	if !isSequence(fields) {
		v.errf(fields, join(path, "fields"), "Field 'fields' must be a list")
		return
	}
	items := seqItems(fields)
	if len(items) == 0 {
		v.errf(fields, join(path, "fields"), "Object must have at least one field")
	}
	// Nested object fields are full fields, so they do need a name.
	for i, nested := range items {
		v.field(nested, index(join(path, "fields"), i), false)
	}
}

// arrayField validates an array field's `items:` schema and length bounds.
func (v *validator) arrayField(n *yaml.Node, path string) {
	items := mapGet(n, "items")
	if items == nil {
		v.errf(n, join(path, "items"), "Array field requires 'items' schema")
		return
	}
	if !isMapping(items) {
		v.errf(items, join(path, "items"), "Field 'items' must be an object/dict")
		return
	}
	v.intAtLeast(n, path, "min_items", 0, "Field 'min_items' must be a non-negative integer")
	v.intAtLeast(n, path, "max_items", 1, "Field 'max_items' must be a positive integer")
	v.pairing(n, path, "min_items", "max_items", defaultMinItems, defaultMaxItems,
		"min_items cannot be greater than max_items")
	v.field(items, join(path, "items"), true)
}

// fakerField validates a faker field's `provider:`/`locale:` keys.
func (v *validator) fakerField(n *yaml.Node, path string) {
	provider := mapGet(n, "provider")
	if provider == nil {
		v.errf(n, join(path, "provider"), "Faker field requires 'provider'")
		return
	}
	if !isString(provider) {
		v.errf(provider, join(path, "provider"), "Field 'provider' must be a string")
		return
	}
	if locale := mapGet(n, "locale"); locale != nil && !isString(locale) {
		v.errf(locale, join(path, "locale"), "Field 'locale' must be a string")
	}
}
