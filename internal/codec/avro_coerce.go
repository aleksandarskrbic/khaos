package codec

// coerceAvro walks a value alongside its writer schema (recovered from the
// schema text as an avroSchemaNode tree, see parseAvroSchemaNode) and wraps
// every union branch into goavro's native representation before
// BinaryFromNative sees it.
//
// Unlike hamba, goavro never auto-wraps or auto-unwraps unions: a union value
// must be handed to BinaryFromNative as literal nil for the null branch, or as
// map[string]interface{}{"branchName": value} for exactly one non-null
// branch, where branchName is the branch's own Avro type name -- a primitive
// type's name ("string", "long", ...), "array", "map", or a named type's full
// name ("ns.Record"/"ns.Enum"/"ns.Fixed"). Passing a bare unwrapped value for
// a non-null union branch is a hard encode error, so every nullable/union
// field khaos generates or converts from a registry schema must be wrapped
// here first.
//
// khaos's own generated schemas (avroTypeOf) never emit a union at all --
// every field is required -- so this is a no-op walk for them. It only does
// real work against a schema fetched from Schema Registry and used verbatim
// (see resolveSchema in codec.go), which can be an arbitrary union.
//
// Per the verified goavro v2.15.0 behavior, no numeric/byte coercion is
// needed here: unlike hamba, goavro accepts any Go numeric kind for
// int/long/float/double and either string or []byte for string/bytes,
// coercing internally. The one primitive it does not coerce is boolean (exact
// Go bool only), but khaos's own generator already produces bool for a
// boolean field, so there is nothing to convert there either.
func coerceAvro(s avroSchemaNode, v any) any {
	if len(s.union) > 0 {
		if v == nil {
			return nil
		}
		for _, branch := range s.union {
			if branch.typeName == "null" {
				continue
			}
			if !avroValueFitsBranch(*branch, v) {
				continue
			}
			key := branch.typeName
			if branch.fullName != "" {
				key = branch.fullName
			}
			return map[string]any{key: coerceAvro(*branch, v)}
		}
		// No branch's Go type family matched: let goavro report the real type
		// mismatch rather than masking it here.
		return v
	}

	switch s.typeName {
	case "record":
		m, ok := v.(map[string]any)
		if !ok {
			return v
		}
		out := make(map[string]any, len(m))
		for k, e := range m {
			out[k] = e
		}
		for _, f := range s.fields {
			if e, ok := out[f.name]; ok {
				out[f.name] = coerceAvro(*f.typ, e)
			}
		}
		return out

	case "array":
		items, ok := v.([]any)
		if !ok || s.items == nil {
			return v
		}
		out := make([]any, len(items))
		for i, e := range items {
			out[i] = coerceAvro(*s.items, e)
		}
		return out

	case "map":
		m, ok := v.(map[string]any)
		if !ok || s.values == nil {
			return v
		}
		out := make(map[string]any, len(m))
		for k, e := range m {
			out[k] = coerceAvro(*s.values, e)
		}
		return out
	}

	return v
}

// avroValueFitsBranch reports whether v's Go type family is a plausible match
// for branch, used to pick a union branch by structural shape. khaos's own
// generator only ever emits the non-null branch of a field's declared type
// (see avroTypeOf), so this only has real work to do -- and ambiguity to
// resolve -- against a registry-fetched schema whose union has more than one
// non-null branch.
func avroValueFitsBranch(branch avroSchemaNode, v any) bool {
	switch branch.typeName {
	case "record", "map":
		_, ok := v.(map[string]any)
		return ok
	case "array":
		_, ok := v.([]any)
		return ok
	case "string", "enum":
		_, ok := v.(string)
		return ok
	case "bytes", "fixed":
		switch v.(type) {
		case string, []byte:
			return true
		default:
			return false
		}
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "int", "long", "float", "double",
		"long.timestamp-millis", "long.timestamp-micros",
		"int.time-millis", "long.time-micros", "int.date":
		switch v.(type) {
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			float32, float64:
			return true
		default:
			return false
		}
	default:
		return true
	}
}
