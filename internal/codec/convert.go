package codec

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// Array bounds for converted fields.
//
// scenario.Field's own defaults are unexported and only applied by YAML
// decoding, so a Field built directly in Go has to set them itself.
const (
	convertedMinItems = 1
	convertedMaxItems = 5
)

// avroFieldTypes maps Avro primitive type names to khaos field types. Avro
// "bytes" becomes a khaos string: the generator will produce text, which the
// Avro encoder writes as the byte sequence.
var avroFieldTypes = map[string]string{
	"string":  scenario.FieldString,
	"long":    scenario.FieldInt,
	"int":     scenario.FieldInt,
	"double":  scenario.FieldFloat,
	"float":   scenario.FieldFloat,
	"boolean": scenario.FieldBoolean,
	"bytes":   scenario.FieldString,
}

// convertedField builds a Field with khaos's standard array-size defaults.
func convertedField(name, typ string) scenario.Field {
	return scenario.Field{
		Name:     name,
		Type:     typ,
		MinItems: convertedMinItems,
		MaxItems: convertedMaxItems,
	}
}

// avroFieldsFromSchema converts a registry Avro schema into field definitions
// the generator can produce data for. Fallbacks: a union takes its first
// non-null branch, an Avro map degrades to an object with no fields, and
// anything unrecognised degrades to a string rather than failing.
func avroFieldsFromSchema(text string) ([]scenario.Field, error) {
	raw, err := avroSchemaJSON(text)
	if err != nil {
		return nil, err
	}
	if t, _ := raw["type"].(string); t != "record" {
		return nil, fmt.Errorf("expected an avro record schema, got %v", raw["type"])
	}
	return avroNestedFields(raw), nil
}

// avroNestedFields converts the `fields` array of an Avro record.
func avroNestedFields(record map[string]any) []scenario.Field {
	entries, _ := record["fields"].([]any)
	out := make([]scenario.Field, 0, len(entries))
	for _, e := range entries {
		m, ok := e.(map[string]any)
		if !ok {
			continue
		}
		if f, ok := avroFieldToField(m); ok {
			out = append(out, f)
		}
	}
	return out
}

// avroFieldToField converts one Avro record field, reporting false for a field
// that must be skipped: an all-null union has no representable type.
func avroFieldToField(entry map[string]any) (scenario.Field, bool) {
	name, _ := entry["name"].(string)
	typ := entry["type"]

	// Unions take the first non-null branch. A branch that is itself a
	// map (a logical type, a record) is never the string "null", so it wins.
	if union, ok := typ.([]any); ok {
		typ = nil
		for _, branch := range union {
			if s, isStr := branch.(string); isStr && s == "null" {
				continue
			}
			typ = branch
			break
		}
		if typ == nil {
			return scenario.Field{}, false
		}
	}

	if m, ok := typ.(map[string]any); ok {
		// Logical types are checked before the underlying type.
		switch lt, _ := m["logicalType"].(string); lt {
		case "uuid":
			return convertedField(name, scenario.FieldUUID), true
		case "timestamp-millis", "timestamp-micros":
			return convertedField(name, scenario.FieldTimestamp), true
		case "date", "time-millis":
			return convertedField(name, scenario.FieldInt), true
		}

		switch inner, _ := m["type"].(string); inner {
		case "record":
			f := convertedField(name, scenario.FieldObject)
			f.Fields = avroNestedFields(m)
			return f, true
		case "enum":
			f := convertedField(name, scenario.FieldEnum)
			f.Values = avroSymbols(m)
			return f, true
		case "array":
			f := convertedField(name, scenario.FieldArray)
			item := avroItemField("item", m["items"])
			f.Items = &item
			return f, true
		case "map":
			// An Avro map has no khaos field-type equivalent, so it degrades
			// to an object with no fields and the generator emits an empty object.
			return convertedField(name, scenario.FieldObject), true
		default:
			if mapped, ok := avroFieldTypes[inner]; ok {
				return convertedField(name, mapped), true
			}
		}
		return convertedField(name, scenario.FieldString), true
	}

	if s, ok := typ.(string); ok {
		if mapped, ok := avroFieldTypes[s]; ok {
			return convertedField(name, mapped), true
		}
	}
	return convertedField(name, scenario.FieldString), true
}

// avroItemField converts an array's item type. It deliberately handles less
// than avroFieldToField does: an array of logical types or an array of arrays
// degrades to the underlying primitive or to a string.
func avroItemField(name string, items any) scenario.Field {
	if s, ok := items.(string); ok {
		if mapped, ok := avroFieldTypes[s]; ok {
			return convertedField(name, mapped)
		}
		return convertedField(name, scenario.FieldString)
	}

	if m, ok := items.(map[string]any); ok {
		switch inner, _ := m["type"].(string); inner {
		case "record":
			f := convertedField(name, scenario.FieldObject)
			f.Fields = avroNestedFields(m)
			return f
		case "enum":
			f := convertedField(name, scenario.FieldEnum)
			f.Values = avroSymbols(m)
			return f
		default:
			if mapped, ok := avroFieldTypes[inner]; ok {
				return convertedField(name, mapped)
			}
		}
	}
	return convertedField(name, scenario.FieldString)
}

// avroSymbols reads an enum's symbol list.
func avroSymbols(m map[string]any) []string {
	raw, _ := m["symbols"].([]any)
	out := make([]string, 0, len(raw))
	for _, s := range raw {
		if str, ok := s.(string); ok {
			out = append(out, str)
		}
	}
	return out
}

// protoFieldTypes maps protobuf scalar kinds to khaos field types, keyed on
// the descriptor kind rather than a source keyword.
func protoFieldTypes(k protoreflect.Kind) string {
	switch k {
	case protoreflect.BoolKind:
		return scenario.FieldBoolean
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return scenario.FieldFloat
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return scenario.FieldInt
	default:
		// StringKind, BytesKind and anything unforeseen.
		return scenario.FieldString
	}
}

// protoFieldsFromSource converts a registry .proto schema into field
// definitions by compiling it properly, so nested messages, enums and
// repeated messages all convert correctly; a schema that fails to compile is
// reported as a real compile error rather than silently producing data that
// doesn't match it.
//
// The remaining limitation is imports of other registry subjects: only the
// well-known google/protobuf files resolve, because schema references are not
// fetched. Such a schema fails to compile rather than being half-parsed.
func protoFieldsFromSource(ctx context.Context, text string) ([]scenario.Field, error) {
	md, err := compileProto(ctx, map[string]string{protoRootFile: text}, protoRootFile)
	if err != nil {
		return nil, err
	}
	return protoMessageFields(md, map[protoreflect.FullName]bool{}), nil
}

// protoMessageFields converts every field of a message. seen carries the
// messages already being expanded so that a self-referential schema terminates
// with an empty object instead of recursing forever.
func protoMessageFields(md protoreflect.MessageDescriptor, seen map[protoreflect.FullName]bool) []scenario.Field {
	seen[md.FullName()] = true
	defer delete(seen, md.FullName())

	fds := md.Fields()
	out := make([]scenario.Field, 0, fds.Len())
	for i := 0; i < fds.Len(); i++ {
		out = append(out, protoFieldToField(fds.Get(i), seen))
	}
	return out
}

// protoFieldToField converts one protobuf field.
func protoFieldToField(fd protoreflect.FieldDescriptor, seen map[protoreflect.FullName]bool) scenario.Field {
	name := string(fd.Name())
	switch {
	case fd.IsMap():
		// khaos has no map field type; an object with no fields is what the
		// Avro converter does with an Avro map, so maps behave the same way.
		return convertedField(name, scenario.FieldObject)
	case fd.IsList():
		f := convertedField(name, scenario.FieldArray)
		item := protoScalarField("item", fd, seen)
		f.Items = &item
		return f
	default:
		return protoScalarField(name, fd, seen)
	}
}

// protoScalarField converts a field ignoring its repeated-ness, which is what
// an array's item schema needs.
func protoScalarField(name string, fd protoreflect.FieldDescriptor, seen map[protoreflect.FullName]bool) scenario.Field {
	switch fd.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		f := convertedField(name, scenario.FieldObject)
		if !seen[fd.Message().FullName()] {
			f.Fields = protoMessageFields(fd.Message(), seen)
		}
		return f
	case protoreflect.EnumKind:
		f := convertedField(name, scenario.FieldEnum)
		values := fd.Enum().Values()
		f.Values = make([]string, 0, values.Len())
		for i := 0; i < values.Len(); i++ {
			f.Values = append(f.Values, string(values.Get(i).Name()))
		}
		return f
	default:
		return convertedField(name, protoFieldTypes(fd.Kind()))
	}
}
