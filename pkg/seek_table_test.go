package seekable

import (
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSeekTable(t *testing.T) {
	t.Parallel()

	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()

	table, err := NewSeekTable(checksum[17+18:])
	require.NoError(t, err)

	assert.Equal(t, uint64(len(sourceString)), table.Size())
	assert.Equal(t, int64(2), table.NumFrames())

	for _, tc := range []struct {
		name    string
		id      int64
		offsets []uint64
		data    []byte
	}{
		{name: "FirstFrame", id: 0, offsets: []uint64{0, 1, 3}, data: []byte("test")},
		{name: "SecondFrame", id: 1, offsets: []uint64{4, 5, 8}, data: []byte("test2")},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			indexByID, ok := table.EntryByID(tc.id)
			require.True(t, ok)
			assert.Equal(t, tc.id, indexByID.ID)
			assert.Equal(t, uint32(len(tc.data)), indexByID.DecompSize)
			assert.NotEqual(t, uint32(0), indexByID.Checksum)

			decomp, err := dec.DecodeAll(
				checksum[indexByID.CompOffset:indexByID.CompOffset+uint64(indexByID.CompSize)], nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tc.data, decomp)

			for _, off := range tc.offsets {
				indexByOffset, ok := table.EntryByDecompressedOffset(off)
				require.True(t, ok)
				assert.Equal(t, indexByID, indexByOffset)
			}
		})
	}

	for _, off := range []uint64{9, 99} {
		_, ok := table.EntryByDecompressedOffset(off)
		assert.False(t, ok)
	}

	for _, id := range []int64{-1, 2, 99} {
		_, ok := table.EntryByID(id)
		assert.False(t, ok)
	}
}
