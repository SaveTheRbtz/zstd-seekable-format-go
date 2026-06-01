package seekable

import (
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestEncoder(t *testing.T) {
	t.Parallel()

	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)

	e, err := NewEncoder(enc)
	require.NoError(t, err)

	chunks := [][]byte{
		[]byte(sourceString[:4]),
		[]byte(sourceString[4:]),
	}
	var combined []byte
	for _, chunk := range chunks {
		encoded, err := e.Encode(chunk)
		require.NoError(t, err)
		combined = append(combined, encoded...)
	}

	footer, err := e.EndStream()
	require.NoError(t, err)

	// Standard Reader.
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()

	decompressed, err := dec.DecodeAll(combined, nil)
	require.NoError(t, err)
	require.Equal(t, sourceString, string(decompressed))

	// Seek table metadata.
	table, err := NewSeekTable(footer)
	require.NoError(t, err)

	require.Equal(t, uint64(len(sourceString)), table.Size())
	require.Equal(t, int64(len(chunks)), table.NumFrames())
}
