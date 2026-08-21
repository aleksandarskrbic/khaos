package codec

import "fmt"

// jsonCodec is the plain UTF-8 JSON path: no schema, no registry, no header.
type jsonCodec struct{}

// Encode writes the document as a JSON object in field order.
func (jsonCodec) Encode(d *Doc) ([]byte, error) {
	b, err := d.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("codec: encode json: %w", err)
	}
	return b, nil
}

// Decode parses a JSON object back into a document, preserving key order.
func (jsonCodec) Decode(b []byte) (*Doc, error) {
	d := NewDoc()
	if err := d.UnmarshalJSON(b); err != nil {
		return nil, fmt.Errorf("codec: decode json: %w", err)
	}
	return d, nil
}
