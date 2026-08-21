package codec

import (
	"context"
	"fmt"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// Codec converts a generated document to Kafka record bytes and back. Every
// format-specific concern -- schema text, registry ids, wire headers -- is
// resolved once in [New], and the engine shares one Codec across a topic's
// whole producer pool, so every implementation must stay immutable and safe
// for concurrent use after construction: anything added here that caches,
// reuses a buffer, or holds a *rand.Rand needs its own synchronisation (a
// single Doc, by contrast, is not safe to share between goroutines).
type Codec interface {
	// Encode serialises a document into the bytes of one record value.
	Encode(*Doc) ([]byte, error)
	// Decode parses record value bytes back into a document.
	Decode([]byte) (*Doc, error)
}

// Data formats accepted in `message_schema.data_format`.
const (
	formatJSON     = "json"
	formatAvro     = "avro"
	formatProtobuf = "protobuf"
)

// New builds the codec for a topic: it resolves where the field definitions
// come from (inline or the registry), which format to use, and whether
// records carry a Confluent header.
//
// reg may be nil, in which case the registry paths are unavailable: Avro and
// protobuf still work but emit bare bytes with no header.
//
// Two fallbacks are deliberate: an "avro" or "protobuf" topic with no fields
// falls back to JSON rather than failing, since scenario validation already
// rejects that combination for inline schemas; and an unrecognised
// data_format also falls back to JSON.
//
// A schema fetched via schema_provider: registry (Avro or protobuf) is used
// verbatim -- its text drives encoding, its id goes into the header -- and is
// never re-registered, so pointing khaos at an existing subject never mutates
// it.
func New(ctx context.Context, t scenario.Topic, reg *Registry) (Codec, error) {
	format := t.MessageSchema.DataFormat
	fields := t.MessageSchema.Fields

	var fetched *subjectSchema
	if t.SchemaProvider == "registry" && t.SubjectName != "" {
		if reg == nil {
			return nil, fmt.Errorf("codec: topic %q uses schema_provider: registry but no schema registry is configured", t.Name)
		}
		s, err := reg.fetch(ctx, t.SubjectName)
		if err != nil {
			return nil, err
		}
		format, fields, fetched = s.format, s.fields, s
	}

	switch format {
	case formatAvro:
		if len(fields) == 0 {
			return jsonCodec{}, nil
		}
		return newAvroCodec(ctx, t, fields, fetched, reg)
	case formatProtobuf:
		if len(fields) == 0 {
			return jsonCodec{}, nil
		}
		return newProtoCodec(ctx, t, fields, fetched, reg)
	default:
		return jsonCodec{}, nil
	}
}

// newAvroCodec resolves the schema text and any registry id, then returns a
// bare or framed Avro codec.
func newAvroCodec(ctx context.Context, t scenario.Topic, fields []scenario.Field, fetched *subjectSchema, reg *Registry) (Codec, error) {
	text, fr, err := resolveSchema(ctx, t, fetched, reg, sr.TypeAvro, func() (string, error) {
		return AvroSchemaText(fields, recordName(t.Name))
	})
	if err != nil {
		return nil, err
	}

	parsed, err := parseAvro(text)
	if err != nil {
		return nil, fmt.Errorf("codec: parse avro schema for topic %q: %w", t.Name, err)
	}
	record, ok := parsed.(*avro.RecordSchema)
	if !ok {
		return nil, fmt.Errorf("codec: avro schema for topic %q is %s, want a record", t.Name, parsed.Type())
	}
	return &avroCodec{schema: parsed, record: record, frame: fr}, nil
}

// newProtoCodec compiles the schema and resolves any registry id, then returns
// a bare or framed protobuf codec.
func newProtoCodec(ctx context.Context, t scenario.Topic, fields []scenario.Field, fetched *subjectSchema, reg *Registry) (Codec, error) {
	text, fr, err := resolveSchema(ctx, t, fetched, reg, sr.TypeProtobuf, func() (string, error) {
		return ProtoSourceText(fields, recordName(t.Name))
	})
	if err != nil {
		return nil, err
	}
	if fr != nil {
		// Protobuf headers carry the message index; khaos always encodes the
		// first top-level message of the schema.
		fr.index = []int{0}
	}

	md, err := compileProto(ctx, map[string]string{protoRootFile: text}, protoRootFile)
	if err != nil {
		return nil, fmt.Errorf("codec: protobuf schema for topic %q: %w", t.Name, err)
	}
	return &protoCodec{md: md, frame: fr}, nil
}

// resolveSchema picks the schema text and the wire frame for a topic: a fetched
// schema is used as-is with its own id, a generated schema is registered when a
// registry is configured, and otherwise there is no frame at all.
func resolveSchema(
	ctx context.Context,
	t scenario.Topic,
	fetched *subjectSchema,
	reg *Registry,
	typ sr.SchemaType,
	generate func() (string, error),
) (string, *frame, error) {
	if fetched != nil {
		return fetched.text, &frame{id: fetched.id}, nil
	}
	text, err := generate()
	if err != nil {
		return "", nil, err
	}
	if reg == nil {
		return text, nil, nil
	}
	id, err := reg.register(ctx, subjectFor(t.Name), text, typ)
	if err != nil {
		return "", nil, err
	}
	return text, &frame{id: id}, nil
}
