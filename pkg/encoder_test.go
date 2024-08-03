package seekable

import (
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoder(t *testing.T) {
	t.Parallel()

	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)

	e, err := NewEncoder(enc)
	require.NoError(t, err)

	decBytes1 := sourceString[:4]
	encBytes1, err := e.Encode([]byte(decBytes1))
	require.NoError(t, err)

	decBytes2 := sourceString[4:]
	encBytes2, err := e.Encode([]byte(decBytes2))
	require.NoError(t, err)

	footer, err := e.EndStream()
	require.NoError(t, err)

	// Standard Reader.
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)

	combined := append(append([]byte{}, encBytes1...), encBytes2...)
	decompressed, err := dec.DecodeAll(combined, nil)
	require.NoError(t, err)
	assert.Equal(t, sourceString, string(decompressed))

	// Seekable Decoder.
	d, err := NewDecoder(footer, dec)
	require.NoError(t, err)

	assert.Equal(t, int64(len(sourceString)), d.Size())
	assert.Equal(t, int64(2), d.NumFrames())
}
