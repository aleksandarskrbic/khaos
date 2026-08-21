package codec

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// protoRootFile is the synthetic filename .proto source is compiled under.
const protoRootFile = "khaos.proto"

// compileProto compiles .proto source text into the descriptor of its first
// top-level message.
//
// WithStandardImports is required: registry schemas routinely import
// google/protobuf/timestamp.proto and friends, and without it compilation fails
// on the import rather than on anything khaos did wrong. sources maps filename
// to source text and must contain root plus any files it imports.
func compileProto(ctx context.Context, sources map[string]string, root string) (protoreflect.MessageDescriptor, error) {
	c := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			Accessor: protocompile.SourceAccessorFromMap(sources),
		}),
		SourceInfoMode: protocompile.SourceInfoNone,
	}
	files, err := c.Compile(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("codec: compile protobuf schema %q: %w", root, err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("codec: compile protobuf schema %q: no files produced", root)
	}
	msgs := files[0].Messages()
	if msgs.Len() == 0 {
		return nil, fmt.Errorf("codec: protobuf schema %q declares no messages", root)
	}
	// khaos always uses the first message declared in the schema, matching
	// Confluent's default message-index of [0].
	return msgs.Get(0), nil
}

// protoCodec encodes documents as protobuf using a runtime descriptor.
//
// With frame == nil, Encode produces a bare marshaled message with no magic
// byte.
type protoCodec struct {
	md    protoreflect.MessageDescriptor
	frame *frame
}

// protoMarshal writes a dynamic message in ascending field-number order.
//
// Deterministic is load-bearing, not a nicety: proto.Marshal's default field
// order for a dynamicpb message is Go map iteration order over the set
// fields, so without it the same document would encode to a different byte
// sequence on every call. It also fixes map entries into key order, giving
// every khaos record a stable, canonical layout.
var protoMarshal = proto.MarshalOptions{Deterministic: true}

// Encode builds a dynamic message from d and marshals it, behind a Confluent
// header when framed.
func (c *protoCodec) Encode(d *Doc) ([]byte, error) {
	msg := dynamicpb.NewMessage(c.md)
	if err := setMessageFields(msg, d.keys, d.vals); err != nil {
		return nil, err
	}
	body, err := protoMarshal.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("codec: encode protobuf: %w", err)
	}
	if c.frame == nil {
		return body, nil
	}
	return c.frame.prepend(body)
}

// Decode parses bytes with this codec's descriptor. The schema id in the
// header plays no part -- decoding always uses the fixed descriptor this
// codec was built with, and the header is only stripped to find the payload.
func (c *protoCodec) Decode(b []byte) (*Doc, error) {
	body := b
	if c.frame != nil {
		_, rest, err := c.frame.strip(b)
		if err != nil {
			return nil, err
		}
		body = rest
	}

	msg := dynamicpb.NewMessage(c.md)
	if err := proto.Unmarshal(body, msg); err != nil {
		return nil, fmt.Errorf("codec: decode protobuf: %w", err)
	}

	// UseProtoNames keeps field names as declared in the schema; note its
	// consequences: 64-bit integers come back as decimal strings and bytes as
	// base64.
	raw, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("codec: protobuf to json: %w", err)
	}
	doc := NewDoc()
	if err := json.Unmarshal(raw, doc); err != nil {
		return nil, fmt.Errorf("codec: parse protobuf json: %w", err)
	}
	// protojson does not promise a stable field order, so impose the
	// descriptor's.
	return orderByDescriptor(doc, c.md), nil
}

// orderByDescriptor rebuilds doc with keys in field-number order, recursing into
// nested messages so decoding is deterministic at every level.
func orderByDescriptor(doc *Doc, md protoreflect.MessageDescriptor) *Doc {
	out := NewDoc()
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		name := string(fd.Name())
		v, ok := doc.Get(name)
		if !ok {
			continue
		}
		out.Set(name, orderValue(v, fd))
	}
	for _, k := range doc.keys {
		if _, ok := out.Get(k); !ok {
			out.Set(k, doc.vals[k])
		}
	}
	return out
}

func orderValue(v any, fd protoreflect.FieldDescriptor) any {
	if fd.IsMap() || fd.Message() == nil {
		return v
	}
	switch t := v.(type) {
	case *Doc:
		return orderByDescriptor(t, fd.Message())
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			if d, ok := e.(*Doc); ok {
				out[i] = orderByDescriptor(d, fd.Message())
				continue
			}
			out[i] = e
		}
		return out
	default:
		return v
	}
}

// setMessageFields populates msg from ordered key/value pairs. Keys with no
// matching field are skipped silently.
func setMessageFields(msg protoreflect.Message, keys []string, vals map[string]any) error {
	fields := msg.Descriptor().Fields()
	for _, k := range keys {
		fd := fields.ByName(protoreflect.Name(k))
		if fd == nil {
			continue
		}
		v := vals[k]
		if v == nil {
			continue
		}
		if err := setField(msg, fd, v); err != nil {
			return fmt.Errorf("codec: field %q of %s: %w", k, msg.Descriptor().FullName(), err)
		}
	}
	return nil
}

// setField assigns one field, dispatching on the descriptor rather than on the
// Go type of the value. Getting this wrong panics inside protoreflect (Set
// validates the value's type against the field kind), so every kind is handled
// explicitly.
func setField(msg protoreflect.Message, fd protoreflect.FieldDescriptor, v any) error {
	switch {
	case fd.IsMap():
		return setMapField(msg, fd, v)

	case fd.IsList():
		items, ok := toSlice(v)
		if !ok {
			return fmt.Errorf("repeated field needs a list, got %T", v)
		}
		list := msg.Mutable(fd).List()
		for i, item := range items {
			if isMessageKind(fd) {
				elem := list.NewElement()
				if err := setMessageValue(elem.Message(), item); err != nil {
					return fmt.Errorf("index %d: %w", i, err)
				}
				list.Append(elem)
				continue
			}
			pv, err := scalarValue(fd, item)
			if err != nil {
				return fmt.Errorf("index %d: %w", i, err)
			}
			list.Append(pv)
		}
		return nil

	case isMessageKind(fd):
		return setMessageValue(msg.Mutable(fd).Message(), v)

	default:
		pv, err := scalarValue(fd, v)
		if err != nil {
			return err
		}
		msg.Set(fd, pv)
		return nil
	}
}

func isMessageKind(fd protoreflect.FieldDescriptor) bool {
	k := fd.Kind()
	return k == protoreflect.MessageKind || k == protoreflect.GroupKind
}

// setMessageValue fills a nested message from an object value.
func setMessageValue(msg protoreflect.Message, v any) error {
	keys, vals, ok := toFields(v)
	if !ok {
		return fmt.Errorf("message field needs an object, got %T", v)
	}
	return setMessageFields(msg, keys, vals)
}

// setMapField populates a protobuf map field. Generated khaos schemas never
// contain one, but registry-fetched schemas can.
func setMapField(msg protoreflect.Message, fd protoreflect.FieldDescriptor, v any) error {
	keys, vals, ok := toFields(v)
	if !ok {
		return fmt.Errorf("map field needs an object, got %T", v)
	}
	mp := msg.Mutable(fd).Map()
	keyFD := fd.MapKey()
	valFD := fd.MapValue()
	for _, k := range keys {
		kv, err := scalarValue(keyFD, k)
		if err != nil {
			return fmt.Errorf("map key %q: %w", k, err)
		}
		if isMessageKind(valFD) {
			elem := mp.NewValue()
			if err := setMessageValue(elem.Message(), vals[k]); err != nil {
				return fmt.Errorf("map value %q: %w", k, err)
			}
			mp.Set(kv.MapKey(), elem)
			continue
		}
		pv, err := scalarValue(valFD, vals[k])
		if err != nil {
			return fmt.Errorf("map value %q: %w", k, err)
		}
		mp.Set(kv.MapKey(), pv)
	}
	return nil
}

// scalarValue converts a Go value to the protoreflect.Value the field kind
// demands.
func scalarValue(fd protoreflect.FieldDescriptor, v any) (protoreflect.Value, error) {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		b, ok := v.(bool)
		if !ok {
			i, iok := toInt64(v)
			if !iok {
				return protoreflect.Value{}, fmt.Errorf("expected bool, got %T", v)
			}
			b = i != 0
		}
		return protoreflect.ValueOfBool(b), nil

	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		i, ok := toInt64(v)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected int32, got %T", v)
		}
		return protoreflect.ValueOfInt32(int32(i)), nil

	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		i, ok := toInt64(v)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected int64, got %T", v)
		}
		return protoreflect.ValueOfInt64(i), nil

	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		u, ok := toUint64(v)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected uint32, got %T", v)
		}
		return protoreflect.ValueOfUint32(uint32(u)), nil

	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		u, ok := toUint64(v)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected uint64, got %T", v)
		}
		return protoreflect.ValueOfUint64(u), nil

	case protoreflect.FloatKind:
		f, ok := toFloat64(v)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected float, got %T", v)
		}
		return protoreflect.ValueOfFloat32(float32(f)), nil

	case protoreflect.DoubleKind:
		f, ok := toFloat64(v)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected double, got %T", v)
		}
		return protoreflect.ValueOfFloat64(f), nil

	case protoreflect.StringKind:
		s, ok := v.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected string, got %T", v)
		}
		return protoreflect.ValueOfString(s), nil

	case protoreflect.BytesKind:
		switch t := v.(type) {
		case []byte:
			return protoreflect.ValueOfBytes(t), nil
		case string:
			return protoreflect.ValueOfBytes([]byte(t)), nil
		default:
			return protoreflect.Value{}, fmt.Errorf("expected bytes, got %T", v)
		}

	case protoreflect.EnumKind:
		return enumValue(fd, v)

	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported protobuf kind %s", fd.Kind())
	}
}

// enumValue resolves an enum field value. A string is looked up by symbol
// name and an unknown symbol becomes 0; numbers are used verbatim.
func enumValue(fd protoreflect.FieldDescriptor, v any) (protoreflect.Value, error) {
	if s, ok := v.(string); ok {
		ev := fd.Enum().Values().ByName(protoreflect.Name(s))
		if ev == nil {
			return protoreflect.ValueOfEnum(0), nil
		}
		return protoreflect.ValueOfEnum(ev.Number()), nil
	}
	i, ok := toInt64(v)
	if !ok {
		return protoreflect.Value{}, fmt.Errorf("expected enum symbol or number, got %T", v)
	}
	return protoreflect.ValueOfEnum(protoreflect.EnumNumber(int32(i))), nil
}
