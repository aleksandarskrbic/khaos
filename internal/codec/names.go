package codec

import (
	"strings"
	"unicode"
)

// compatTitle title-cases the first letter of every run of letters and
// lowercases the rest, treating any non-letter (digits and punctuation
// included) as a word boundary: "user-events" -> "User-Events", "3d" -> "3D",
// "ORDERS" -> "Orders" -- unlike strings.Title or golang.org/x/text/cases.
// Generated Avro record names, Protobuf message names and enum names all flow
// through it and end up in schema text registered with Schema Registry, so
// the exact casing is kept for compatibility with subjects already registered
// by earlier khaos releases.
func compatTitle(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	prevIsLetter := false
	for _, r := range s {
		if prevIsLetter {
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune(unicode.ToTitle(r))
		}
		prevIsLetter = unicode.IsLetter(r)
	}
	return b.String()
}

// mangleProto produces a Protobuf type name from an arbitrary string:
// title-case, then drop '-' and '_'. Also used to derive the top-level
// message name from a topic name.
func mangleProto(name string) string {
	t := compatTitle(name)
	t = strings.ReplaceAll(t, "-", "")
	return strings.ReplaceAll(t, "_", "")
}

// mangleAvro is the Avro type-name mangling. Unlike mangleProto, it strips
// only '_', never '-', so a field named "order-id" yields the Avro name
// "Order-IdEnum" (which Avro itself rejects, since '-' is not legal in a
// name) while mangleProto would produce "OrderIdEnum". This discrepancy is
// deliberately preserved: changing it would break every already-registered
// Avro subject.
func mangleAvro(name string) string {
	return strings.ReplaceAll(compatTitle(name), "_", "")
}

// recordName is the top-level type name derived from a topic name.
func recordName(topic string) string {
	return mangleProto(topic) + "Record"
}

// subjectFor is the Confluent TopicNameStrategy subject for a topic's value.
func subjectFor(topic string) string {
	return topic + "-value"
}
