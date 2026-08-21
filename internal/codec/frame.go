package codec

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/sr"
)

// frame is the Confluent Schema Registry wire header: magic byte 0x00, then the
// schema id as a big-endian uint32, then -- for protobuf only -- the varint
// message-index array.
//
// It is built explicitly here, which is what makes it possible to stamp a
// schema id that was fetched rather than registered (see [New]).
//
// A nil *frame means the no-registry paths: bare Avro or protobuf bytes with
// no header at all.
type frame struct {
	id int
	// index is the protobuf message-index path and is nil for Avro. khaos
	// always encodes the first top-level message, so it is []int{0}, which the
	// Confluent format shortcuts to a single zero byte.
	index []int
}

// prepend returns the header followed by body.
func (f *frame) prepend(body []byte) ([]byte, error) {
	var hdr sr.ConfluentHeader
	out, err := hdr.AppendEncode(nil, f.id, f.index)
	if err != nil {
		return nil, fmt.Errorf("codec: encode confluent header for schema id %d: %w", f.id, err)
	}
	return append(out, body...), nil
}

// strip removes the header and returns the embedded schema id with the
// remaining payload.
//
// The id is returned rather than checked: khaos decodes with the descriptor
// it was constructed with, using the header only to find where the payload
// starts.
func (f *frame) strip(b []byte) (int, []byte, error) {
	var hdr sr.ConfluentHeader
	id, rest, err := hdr.DecodeID(b)
	if err != nil {
		return 0, nil, fmt.Errorf("codec: decode confluent header: %w", err)
	}
	if len(f.index) > 0 {
		if _, rest, err = hdr.DecodeIndex(rest, 0); err != nil {
			return 0, nil, fmt.Errorf("codec: decode confluent message index: %w", err)
		}
	}
	return id, rest, nil
}
