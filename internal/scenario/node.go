package scenario

import "go.yaml.in/yaml/v3"

// This file is the thin layer that lets validation read a raw yaml.Node tree with
// dict-like ergonomics. Validation runs against the node tree rather than the typed
// structs so that every problem in a file is reported at once (see validate.go), and so
// that each finding can carry the line it came from.

// mapEntry is one key/value pair of a mapping node.
type mapEntry struct {
	key     string
	keyNode *yaml.Node
	value   *yaml.Node
}

// resolveNode unwraps document and alias nodes so callers only ever see content.
func resolveNode(n *yaml.Node) *yaml.Node {
	for n != nil {
		switch {
		case n.Kind == yaml.DocumentNode && len(n.Content) > 0:
			n = n.Content[0]
		case n.Kind == yaml.AliasNode && n.Alias != nil:
			n = n.Alias
		default:
			return n
		}
	}
	return nil
}

// mappingEntries returns a mapping's pairs with YAML merge keys (`<<`) expanded.
//
// Expansion matters because yaml.Unmarshal in pass 2 honours merge keys, so pass 1
// has to see the same shape or the two passes would disagree about which keys exist.
// Explicit keys win over merged ones, standard merge-key semantics.
func mappingEntries(n *yaml.Node) []mapEntry {
	n = resolveNode(n)
	if n == nil || n.Kind != yaml.MappingNode {
		return nil
	}
	seen := make(map[string]bool, len(n.Content)/2)
	out := make([]mapEntry, 0, len(n.Content)/2)
	var merged []mapEntry
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		if k.Tag == "!!merge" {
			for _, src := range mergeSources(v) {
				merged = append(merged, mappingEntries(src)...)
			}
			continue
		}
		if k.Kind != yaml.ScalarNode {
			continue // non-scalar keys cannot be addressed by name; nothing reads them
		}
		seen[k.Value] = true
		out = append(out, mapEntry{key: k.Value, keyNode: k, value: v})
	}
	for _, e := range merged {
		if !seen[e.key] {
			seen[e.key] = true
			out = append(out, e)
		}
	}
	return out
}

// mergeSources normalises the value of a `<<` key, which is either one mapping or a
// sequence of mappings.
func mergeSources(n *yaml.Node) []*yaml.Node {
	n = resolveNode(n)
	if n == nil {
		return nil
	}
	if n.Kind == yaml.SequenceNode {
		out := make([]*yaml.Node, 0, len(n.Content))
		for _, c := range n.Content {
			out = append(out, resolveNode(c))
		}
		return out
	}
	return []*yaml.Node{n}
}

// mapGet returns the value node for key, or nil when the key is absent or n is not a
// mapping.
func mapGet(n *yaml.Node, key string) *yaml.Node {
	for _, e := range mappingEntries(n) {
		if e.key == key {
			return resolveNode(e.value)
		}
	}
	return nil
}

// seqItems returns the elements of a sequence node, each resolved.
func seqItems(n *yaml.Node) []*yaml.Node {
	n = resolveNode(n)
	if n == nil || n.Kind != yaml.SequenceNode {
		return nil
	}
	out := make([]*yaml.Node, 0, len(n.Content))
	for _, c := range n.Content {
		out = append(out, resolveNode(c))
	}
	return out
}

func isMapping(n *yaml.Node) bool {
	n = resolveNode(n)
	return n != nil && n.Kind == yaml.MappingNode
}

func isSequence(n *yaml.Node) bool {
	n = resolveNode(n)
	return n != nil && n.Kind == yaml.SequenceNode
}

// isString reports whether n is a string scalar.
func isString(n *yaml.Node) bool {
	n = resolveNode(n)
	return n != nil && n.Kind == yaml.ScalarNode && n.Tag == "!!str"
}

// isInt reports whether n is an integer scalar. A YAML bool carries a different tag
// than !!int, so `partitions: true` is rejected rather than silently read as 1.
func isInt(n *yaml.Node) bool {
	n = resolveNode(n)
	return n != nil && n.Kind == yaml.ScalarNode && n.Tag == "!!int"
}

// isNumber reports whether n is an int or float scalar.
func isNumber(n *yaml.Node) bool {
	n = resolveNode(n)
	return n != nil && n.Kind == yaml.ScalarNode && (n.Tag == "!!int" || n.Tag == "!!float")
}

// intValue reads an int scalar. The bool result is false when the node is absent or
// not an int, which lets callers skip the check rather than error.
func intValue(n *yaml.Node) (int, bool) {
	if !isInt(n) {
		return 0, false
	}
	var v int
	if err := resolveNode(n).Decode(&v); err != nil {
		return 0, false
	}
	return v, true
}

// floatValue reads an int or float scalar as a float64.
func floatValue(n *yaml.Node) (float64, bool) {
	if !isNumber(n) {
		return 0, false
	}
	var v float64
	if err := resolveNode(n).Decode(&v); err != nil {
		return 0, false
	}
	return v, true
}

// scalarText is the raw source text of a scalar, used to echo an offending value back
// in a diagnostic message.
func scalarText(n *yaml.Node) string {
	n = resolveNode(n)
	if n == nil || n.Kind != yaml.ScalarNode {
		return ""
	}
	return n.Value
}

// nodeLine is the 1-based source line of a node, or 0 when there is none.
func nodeLine(n *yaml.Node) int {
	if n == nil {
		return 0
	}
	return n.Line
}
