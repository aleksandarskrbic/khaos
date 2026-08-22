package codec

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/linkedin/goavro/v2"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// avroNamespace is the namespace of every schema khaos generates.
const avroNamespace = "khaos.generated"

// avroPrimitive maps khaos field types to their Avro type name.
//
// Note that "int" maps to Avro "long" and "float" to Avro "double". That is not
// a mistake to be corrected: khaos has always written 64-bit values, and
// narrowing them now would break every consumer holding an older reader schema.
var avroPrimitive = map[string]string{
	scenario.FieldString:  "string",
	scenario.FieldInt:     "long",
	scenario.FieldFloat:   "double",
	scenario.FieldBoolean: "boolean",
	scenario.FieldFaker:   "string",
}

// AvroSchemaText renders the Avro schema khaos generates for a field list.
//
// It is both the schema used to encode and the exact text POSTed to Schema
// Registry, so any drift in key order, nesting shape, or generated type names
// is a compatibility break.
func AvroSchemaText(fields []scenario.Field, recordName string) (string, error) {
	schema := NewDoc()
	schema.Set("type", "record")
	schema.Set("name", recordName)
	schema.Set("namespace", avroNamespace)
	schema.Set("fields", avroFieldEntries(fields))

	b, err := schema.MarshalJSON()
	if err != nil {
		return "", fmt.Errorf("codec: render avro schema %q: %w", recordName, err)
	}
	return string(b), nil
}

// avroFieldEntries renders the `fields` array of a record.
func avroFieldEntries(fields []scenario.Field) []any {
	entries := make([]any, 0, len(fields))
	for _, f := range fields {
		entry := NewDoc()
		entry.Set("name", f.Name)
		entry.Set("type", avroTypeOf(f))
		entries = append(entries, entry)
	}
	return entries
}

// avroTypeOf maps a field schema to its Avro type, including its fallbacks: an
// enum without values degrades to a plain string, an object without fields to
// a map<string>, an array without items to an array of strings, and an
// unknown type to a string.
func avroTypeOf(f scenario.Field) *Doc {
	d := NewDoc()
	if prim, ok := avroPrimitive[f.Type]; ok {
		d.Set("type", prim)
		return d
	}

	switch f.Type {
	case scenario.FieldUUID:
		d.Set("type", "string")
		d.Set("logicalType", "uuid")
	case scenario.FieldTimestamp:
		d.Set("type", "long")
		d.Set("logicalType", "timestamp-millis")
	case scenario.FieldEnum:
		if len(f.Values) == 0 {
			d.Set("type", "string")
			return d
		}
		d.Set("type", "enum")
		d.Set("name", mangleAvro(f.Name)+"Enum")
		d.Set("symbols", stringsToAny(f.Values))
	case scenario.FieldObject:
		if len(f.Fields) == 0 {
			d.Set("type", "map")
			d.Set("values", "string")
			return d
		}
		d.Set("type", "record")
		d.Set("name", mangleAvro(f.Name)+"Record")
		d.Set("fields", avroFieldEntries(f.Fields))
	case scenario.FieldArray:
		d.Set("type", "array")
		if f.Items == nil {
			items := NewDoc()
			items.Set("type", "string")
			d.Set("items", items)
			return d
		}
		d.Set("items", avroTypeOf(*f.Items))
	default:
		d.Set("type", "string")
	}
	return d
}

func stringsToAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// avroSchemaNode is one node of a schema tree parsed from Avro schema JSON
// text. It stands in for the walkable schema object model goavro does not
// expose (see parseAvroSchemaNode): goavro.Codec only ever hands back the
// schema's original or canonical *text*, never a tree of Schema objects, so
// khaos re-parses the same text it built (or fetched) into this shape and
// walks it in parallel with values on encode (avro_coerce.go) and decode
// (avroDecodedValue below).
//
// A node's shape is one of:
//   - primitive ("null", "boolean", "int", "long", "float", "double",
//     "string", "bytes"): typeName set, nothing else.
//   - union: union set, to branch nodes in declaration order.
//   - record ("record"/"error"): fullName and fields set.
//   - array: items set.
//   - map: values set.
//   - enum: fullName set (no further structure needed: enum symbols
//     round-trip as plain strings, not union-wrapped).
//   - fixed: fullName set.
//
// Named types (record/enum/fixed) are built once and shared by pointer: a
// later bare-name reference to an already-defined type (legal Avro, and
// exercised by TestAvroParseDoesNotShareNamesBetweenSchemas's "reuses" case)
// resolves to the same *avroSchemaNode rather than a shallow stand-in, which
// is what makes a self-referential or reused named type still walk correctly
// deep into its fields instead of stopping at a name.
type avroSchemaNode struct {
	typeName string
	fullName string // record/enum/fixed: namespace-qualified name, also the union branch key
	fields   []avroFieldNode
	items    *avroSchemaNode
	values   *avroSchemaNode
	union    []*avroSchemaNode
}

type avroFieldNode struct {
	name string
	typ  *avroSchemaNode
}

// avroCodec encodes documents as Avro binary against a fixed writer schema.
//
// With frame == nil, Encode produces bare schemaless bytes with no header.
type avroCodec struct {
	codec *goavro.Codec
	root  avroSchemaNode // always a record node; walked for field order and union shapes
	frame *frame
}

// newAvroCodecFromSchema builds an avroCodec from schema text: it parses the
// text once with goavro (for encode/decode) and once into an avroSchemaNode
// tree (for field order and union wrapping), keeping both for the codec's
// lifetime. Shared by newAvroCodec in codec.go and by the tests, so the two
// never drift on how a codec is put together.
func newAvroCodecFromSchema(text string, fr *frame) (*avroCodec, error) {
	gc, err := goavro.NewCodec(text)
	if err != nil {
		return nil, fmt.Errorf("codec: parse avro schema: %w", err)
	}
	root, err := parseAvroSchemaNode(text)
	if err != nil {
		return nil, err
	}
	if root.typeName != "record" {
		return nil, fmt.Errorf("codec: avro schema is %s, want a record", root.typeName)
	}
	return &avroCodec{codec: gc, root: root, frame: fr}, nil
}

// Encode writes Avro binary, behind a Confluent header when framed.
func (c *avroCodec) Encode(d *Doc) ([]byte, error) {
	native := coerceAvro(c.root, docToMap(d))
	body, err := c.codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("codec: avro encode: %w", err)
	}
	if c.frame == nil {
		return body, nil
	}
	return c.frame.prepend(body)
}

// Decode reads Avro binary back into a document whose keys follow the schema's
// field order, which is also the order the encoder wrote them in.
func (c *avroCodec) Decode(b []byte) (*Doc, error) {
	body := b
	if c.frame != nil {
		_, rest, err := c.frame.strip(b)
		if err != nil {
			return nil, err
		}
		body = rest
	}
	native, _, err := c.codec.NativeFromBinary(body)
	if err != nil {
		return nil, fmt.Errorf("codec: avro decode: %w", err)
	}
	m, ok := native.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("codec: avro decode: expected a record, got %T", native)
	}
	return avroRecordDoc(c.root, m), nil
}

// avroRecordDoc rebuilds a Doc from a decoded record, using the schema for
// ordering.
func avroRecordDoc(rec avroSchemaNode, m map[string]any) *Doc {
	d := NewDoc()
	for _, f := range rec.fields {
		v, ok := m[f.name]
		if !ok {
			continue
		}
		d.Set(f.name, avroDecodedValue(*f.typ, v))
	}
	return d
}

// avroDecodedValue converts one decoded Avro native value into the Doc value
// model: it unwraps the goavro union representation
// (map[string]interface{}{"branch": value}) back to a bare value, recurses
// into records/arrays/maps in schema order, and undoes the one scalar
// conversion khaos still needs.
//
// goavro decodes bytes to []byte and khaos generates and encodes bytes-typed
// fields (from a registry schema; khaos never emits "bytes" itself) as plain
// Go strings, so decode converts back or a Doc would not survive a round
// trip. Unlike hamba, goavro decodes timestamp-millis, time-millis and uuid
// to plain Go numerics and a plain string respectively (verified against
// v2.15.0), so none of those need any conversion here.
func avroDecodedValue(s avroSchemaNode, v any) any {
	if len(s.union) > 0 {
		if v == nil {
			return nil
		}
		wrapped, ok := v.(map[string]any)
		if !ok || len(wrapped) != 1 {
			return v
		}
		for branchName, branchVal := range wrapped {
			for _, branch := range s.union {
				if branchMatchesName(*branch, branchName) {
					return avroDecodedValue(*branch, branchVal)
				}
			}
			// Unknown branch name (should not happen against our own parsed
			// schema): pass the raw value through rather than dropping it.
			return branchVal
		}
	}

	switch s.typeName {
	case "record":
		if m, ok := v.(map[string]any); ok {
			return avroRecordDoc(s, m)
		}
	case "array":
		if arr, ok := v.([]any); ok {
			out := make([]any, len(arr))
			for i, e := range arr {
				if s.items != nil {
					out[i] = avroDecodedValue(*s.items, e)
				} else {
					out[i] = e
				}
			}
			return out
		}
	case "map":
		if m, ok := v.(map[string]any); ok {
			d := NewDoc()
			for _, k := range sortedKeys(m) {
				if s.values != nil {
					d.Set(k, avroDecodedValue(*s.values, m[k]))
				} else {
					d.Set(k, m[k])
				}
			}
			return d
		}
	case "bytes":
		if bs, ok := v.([]byte); ok {
			return string(bs)
		}
	}
	return avroNormaliseScalar(v)
}

// avroNormaliseScalar undoes the one Go-type substitution goavro still makes
// for logical types that khaos generates and encodes as plain integers.
//
// Verified against goavro v2.15.0: timestamp-millis decodes to time.Time and
// time-millis decodes to time.Duration, exactly like hamba did, so khaos must
// still convert them back to epoch/day milliseconds or a Doc would not
// survive a round trip. uuid decodes to a plain Go string already (goavro has
// no special uuid type), so unlike hamba's decoder it needs no conversion
// here.
func avroNormaliseScalar(v any) any {
	switch t := v.(type) {
	case time.Time:
		return t.UnixMilli()
	case time.Duration:
		return t.Milliseconds()
	default:
		return v
	}
}

// branchMatchesName reports whether branch is the union member named
// branchName in goavro's wrapped-union representation: a named type
// (record/enum/fixed) is keyed by its full name, everything else by its bare
// type name.
func branchMatchesName(branch avroSchemaNode, branchName string) bool {
	if branch.fullName != "" {
		return branch.fullName == branchName
	}
	return branch.typeName == branchName
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// parseAvro validates that schema text parses as Avro, in isolation from
// every other schema in the process.
//
// goavro.NewCodec gives every call its own private name-resolution scope --
// unlike hamba, which published every named type it saw into a shared
// package-level cache -- so no fresh-cache trick is needed here to stop one
// topic's schema from resolving an unqualified reference against another
// topic's type by accident; that isolation is goavro's default behaviour
// (verified empirically against v2.15.0).
func parseAvro(text string) (*goavro.Codec, error) {
	return goavro.NewCodec(text)
}

// avroSchemaJSON parses schema text into the generic JSON form the registry
// converter walks.
func avroSchemaJSON(text string) (map[string]any, error) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		return nil, fmt.Errorf("codec: parse avro schema json: %w", err)
	}
	return raw, nil
}

// parseAvroSchemaNode parses schema text into an avroSchemaNode tree, the
// stand-in for the schema object model goavro does not expose. It is used
// only to recover field declaration order and union branch shapes; goavro's
// own NewCodec (called separately) remains the source of truth for whether
// the schema is valid Avro at all.
//
// Each call gets a fresh name registry, exactly like parseAvro/goavro.NewCodec,
// so this walk never leaks named types between schemas either.
func parseAvroSchemaNode(text string) (avroSchemaNode, error) {
	raw, err := avroSchemaJSON(text)
	if err != nil {
		return avroSchemaNode{}, err
	}
	ctx := &avroSchemaBuildCtx{names: make(map[string]*avroSchemaNode)}
	node, err := buildAvroSchemaNode(raw, "", ctx)
	if err != nil {
		return avroSchemaNode{}, fmt.Errorf("codec: walk avro schema: %w", err)
	}
	return *node, nil
}

// avroSchemaBuildCtx tracks named types (record/enum/fixed) seen so far
// during one buildAvroSchemaNode walk, so a later reference to an
// already-defined name resolves to the same node instead of a shallow stub.
// A record is registered under its own pointer before its fields are built,
// so a self-referential type (e.g. a linked-list-shaped record) resolves
// correctly too.
type avroSchemaBuildCtx struct {
	names map[string]*avroSchemaNode
}

// avroPrimitiveNames are the type names that are never namespace-qualified
// and never refer to a named type definition.
var avroPrimitiveNames = map[string]bool{
	"null": true, "boolean": true, "int": true, "long": true,
	"float": true, "double": true, "bytes": true, "string": true,
}

// avroCompoundLogicalTypeNames are the (baseType + "." + logicalType) keys
// goavro pre-compiles its own codec for (see the symbol table literal at the
// top of goavro's codec.go, v2.15.0) rather than falling back to the bare
// base type. For exactly these five, the wire-format identity of the type --
// and therefore the key a union branch must be wrapped/matched under, per
// union.go's makeCodecInfo (fullName := unionMemberCodec.typeName.fullName)
// -- is the compound form, not the base primitive name.
//
// Every other logicalType goavro doesn't recognize (uuid included -- goavro
// has no built-in uuid codec) is stripped by
// buildCodecForTypeDescribedByString's fallback and degrades to the bare
// base type, whose branch key is already the plain typeName the rest of this
// file computes.
var avroCompoundLogicalTypeNames = map[string]bool{
	"long.timestamp-millis": true,
	"long.timestamp-micros": true,
	"int.time-millis":       true,
	"long.time-micros":      true,
	"int.date":              true,
}

// buildAvroSchemaNode converts one already-json.Unmarshal'd schema value
// (string, []any, or map[string]any) into an avroSchemaNode. enclosingNS is
// the namespace inherited from the nearest enclosing named type, per the Avro
// spec's namespace inheritance rule.
func buildAvroSchemaNode(raw any, enclosingNS string, ctx *avroSchemaBuildCtx) (*avroSchemaNode, error) {
	switch t := raw.(type) {
	case string:
		if avroPrimitiveNames[t] {
			return &avroSchemaNode{typeName: t}, nil
		}
		// A bare reference to a previously-defined named type.
		full := qualifyAvroName(t, enclosingNS)
		if ref, ok := ctx.names[full]; ok {
			return ref, nil
		}
		if ref, ok := ctx.names[t]; ok {
			return ref, nil
		}
		// Unresolved reference: should not happen for text that already parsed
		// successfully via goavro.NewCodec. Fall back to a shallow stub rather
		// than failing outright, since the goavro codec itself is still correct
		// even if khaos's own field-order/union bookkeeping can't fully resolve
		// an exotic namespace pattern.
		return &avroSchemaNode{typeName: t, fullName: full}, nil

	case []any:
		branches := make([]*avroSchemaNode, 0, len(t))
		for _, b := range t {
			bn, err := buildAvroSchemaNode(b, enclosingNS, ctx)
			if err != nil {
				return nil, err
			}
			branches = append(branches, bn)
		}
		return &avroSchemaNode{union: branches}, nil

	case map[string]any:
		typeName, _ := t["type"].(string)
		ns := enclosingNS
		if v, ok := t["namespace"].(string); ok && v != "" {
			ns = v
		}
		node := &avroSchemaNode{typeName: typeName}

		switch typeName {
		case "record", "error":
			node.typeName = "record"
			name, _ := t["name"].(string)
			node.fullName = qualifyAvroName(name, ns)
			registerAvroName(ctx, node, name, node.fullName)
			rawFields, _ := t["fields"].([]any)
			for _, rf := range rawFields {
				fm, ok := rf.(map[string]any)
				if !ok {
					continue
				}
				fname, _ := fm["name"].(string)
				ft, err := buildAvroSchemaNode(fm["type"], ns, ctx)
				if err != nil {
					return nil, err
				}
				node.fields = append(node.fields, avroFieldNode{name: fname, typ: ft})
			}
		case "enum":
			name, _ := t["name"].(string)
			node.fullName = qualifyAvroName(name, ns)
			registerAvroName(ctx, node, name, node.fullName)
		case "fixed":
			name, _ := t["name"].(string)
			node.fullName = qualifyAvroName(name, ns)
			registerAvroName(ctx, node, name, node.fullName)
		case "array":
			items, err := buildAvroSchemaNode(t["items"], ns, ctx)
			if err != nil {
				return nil, err
			}
			node.items = items
		case "map":
			values, err := buildAvroSchemaNode(t["values"], ns, ctx)
			if err != nil {
				return nil, err
			}
			node.values = values
		default:
			// A primitive spelled out as {"type":"string",...} (e.g. carrying a
			// logicalType) -- typeName is already set correctly above, unless
			// goavro compiles this particular (type, logicalType) pair under a
			// compound key (see avroCompoundLogicalTypeNames), in which case the
			// node's typeName must match that compound key or union branch
			// wrapping (encode) and matching (decode) picks the wrong name.
			if lt, ok := t["logicalType"].(string); ok && lt != "" {
				compound := typeName + "." + lt
				if avroCompoundLogicalTypeNames[compound] {
					node.typeName = compound
				}
			}
		}
		return node, nil

	default:
		return nil, fmt.Errorf("unsupported schema node %T", raw)
	}
}

// registerAvroName records node under both its bare and namespace-qualified
// names, so a later reference resolves whichever form it uses.
func registerAvroName(ctx *avroSchemaBuildCtx, node *avroSchemaNode, bareName, fullName string) {
	if fullName != "" {
		ctx.names[fullName] = node
	}
	if bareName != "" && bareName != fullName {
		ctx.names[bareName] = node
	}
}

// qualifyAvroName returns name qualified with ns per the Avro spec: a name
// containing a dot is already fully qualified, and an unqualified name
// inherits its enclosing namespace, if any.
func qualifyAvroName(name, ns string) string {
	if name == "" || ns == "" {
		return name
	}
	for i := 0; i < len(name); i++ {
		if name[i] == '.' {
			return name
		}
	}
	return ns + "." + name
}
