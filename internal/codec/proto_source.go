package codec

import (
	"fmt"
	"strings"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// protoPackage is the package every generated .proto lives in.
const protoPackage = "khaos.generated"

// protoTypeMap maps khaos field types to .proto source keywords.
var protoTypeMap = map[string]string{
	scenario.FieldString:    "string",
	scenario.FieldInt:       "int64",
	scenario.FieldFloat:     "double",
	scenario.FieldBoolean:   "bool",
	scenario.FieldUUID:      "string",
	scenario.FieldTimestamp: "int64",
	scenario.FieldFaker:     "string",
}

// ProtoSourceText renders fields as proto3 source text defining msgName.
// Source text is the single source of truth: it is compiled with protocompile
// for encoding and POSTed verbatim to Schema Registry.
//
// Fields are numbered from 1 in declaration order, and every non-repeated
// field is emitted without the `optional` keyword, i.e. LABEL_OPTIONAL with no
// proto3_optional.
func ProtoSourceText(fields []scenario.Field, msgName string) (string, error) {
	if msgName == "" {
		return "", fmt.Errorf("codec: protobuf message name must not be empty")
	}

	var b strings.Builder
	b.WriteString("syntax = \"proto3\";\n\n")
	b.WriteString("package " + protoPackage + ";\n\n")
	if err := writeProtoMessage(&b, msgName, fields, 0); err != nil {
		return "", err
	}
	return b.String(), nil
}

// writeProtoMessage emits one `message` block. Nested messages and enums are
// declared before the fields that reference them, inside the message that
// uses them, and referenced by their simple name -- which keeps the text
// readable.
func writeProtoMessage(b *strings.Builder, name string, fields []scenario.Field, depth int) error {
	pad := strings.Repeat("  ", depth)
	inner := strings.Repeat("  ", depth+1)

	var nested strings.Builder
	var lines []string

	for i, f := range fields {
		num := i + 1
		if f.Name == "" {
			return fmt.Errorf("codec: protobuf message %q: field %d has no name", name, num)
		}

		if f.Type == scenario.FieldArray {
			itemType, err := protoArrayItemType(&nested, f, depth)
			if err != nil {
				return err
			}
			lines = append(lines, fmt.Sprintf("%srepeated %s %s = %d;", inner, itemType, f.Name, num))
			continue
		}

		if mapped, ok := protoTypeMap[f.Type]; ok {
			lines = append(lines, fmt.Sprintf("%s%s %s = %d;", inner, mapped, f.Name, num))
			continue
		}

		switch f.Type {
		case scenario.FieldEnum:
			enumName := mangleProto(f.Name) + "Enum"
			writeProtoEnum(&nested, enumName, f.Values, depth+1)
			lines = append(lines, fmt.Sprintf("%s%s %s = %d;", inner, enumName, f.Name, num))

		case scenario.FieldObject:
			nestedName := mangleProto(f.Name) + "Type"
			if err := writeProtoMessage(&nested, nestedName, f.Fields, depth+1); err != nil {
				return err
			}
			lines = append(lines, fmt.Sprintf("%s%s %s = %d;", inner, nestedName, f.Name, num))

		default:
			// Unknown types fall back to string.
			lines = append(lines, fmt.Sprintf("%sstring %s = %d;", inner, f.Name, num))
		}
	}

	b.WriteString(pad + "message " + name + " {\n")
	b.WriteString(nested.String())
	for _, l := range lines {
		b.WriteString(l + "\n")
	}
	b.WriteString(pad + "}\n")
	return nil
}

// protoArrayItemType resolves the element type of a repeated field, declaring
// a nested message for arrays of objects.
//
// Note the fallthrough: an array whose items are an enum, or an array with no
// items at all, becomes `repeated string` -- only "object" gets special-cased,
// everything else falls back to string.
func protoArrayItemType(nested *strings.Builder, f scenario.Field, depth int) (string, error) {
	if f.Items == nil {
		return "string", nil
	}
	if mapped, ok := protoTypeMap[f.Items.Type]; ok {
		return mapped, nil
	}
	if f.Items.Type == scenario.FieldObject {
		nestedName := mangleProto(f.Name) + "Item"
		if err := writeProtoMessage(nested, nestedName, f.Items.Fields, depth+1); err != nil {
			return "", err
		}
		return nestedName, nil
	}
	return "string", nil
}

// writeProtoEnum emits an enum block. Values are numbered from 0 in order, and
// an enum with no values gets the single symbol UNKNOWN.
func writeProtoEnum(b *strings.Builder, name string, values []string, depth int) {
	pad := strings.Repeat("  ", depth)
	inner := strings.Repeat("  ", depth+1)

	symbols := values
	if len(symbols) == 0 {
		symbols = []string{"UNKNOWN"}
	}

	b.WriteString(pad + "enum " + name + " {\n")
	for i, v := range symbols {
		b.WriteString(fmt.Sprintf("%s%s = %d;\n", inner, v, i))
	}
	b.WriteString(pad + "}\n")
}
