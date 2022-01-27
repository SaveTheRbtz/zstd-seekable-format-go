package seekable

import (
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
)

func TestEncoder(t *testing.T) {
	t.Parallel()

	enc, err := zstd.NewWriter(nil)
	assert.NoError(t, err)

	e, err := NewEncoder(enc)
	assert.NoError(t, err)

	decBytes1 := sourceString[:4]
	encBytes1, err := e.Encode([]byte(decBytes1))
	assert.NoError(t, err)

	decBytes2 := sourceString[4:]
	encBytes2, err := e.Encode([]byte(decBytes2))
	assert.NoError(t, err)

	footer, err := e.EndStream()
	assert.NoError(t, err)

	// Standard Reader.
	dec, err := zstd.NewReader(nil)
	assert.NoError(t, err)

	combined := append(append([]byte{}, encBytes1...), encBytes2...)
	decompressed, err := dec.DecodeAll(combined, nil)
	assert.NoError(t, err)
	assert.Equal(t, sourceString, string(decompressed))

	// Seekable Decoder.
	d, err := NewDecoder(footer, dec)
	assert.NoError(t, err)

	assert.Equal(t, int64(len(sourceString)), d.Size())
	assert.Equal(t, int64(2), d.NumFrames())
}
