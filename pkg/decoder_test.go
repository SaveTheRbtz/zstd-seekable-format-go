package seekable

import (
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder(t *testing.T) {
	t.Parallel()

	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()

	d, err := NewDecoder(checksum[17+18:])
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	assert.Equal(t, int64(len(sourceString)), d.Size())
	assert.Equal(t, int64(2), d.NumFrames())

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
			for _, off := range tc.offsets {
				indexByOffset := d.GetIndexByDecompOffset(off)
				indexByID := d.GetIndexByID(tc.id)
				assert.Equal(t, indexByID, indexByOffset)
				require.NotNil(t, indexByOffset)
				assert.Equal(t, tc.id, indexByOffset.ID)
				assert.Equal(t, uint32(len(tc.data)), indexByOffset.DecompSize)
				assert.NotEqual(t, uint32(0), indexByOffset.Checksum)

				decomp, err := dec.DecodeAll(
					checksum[indexByOffset.CompOffset:indexByOffset.CompOffset+uint64(indexByOffset.CompSize)], nil,
				)
				require.NoError(t, err)
				assert.Equal(t, tc.data, decomp)
			}
		})
	}

	for _, off := range []uint64{9, 99} {
		assert.Nil(t, d.GetIndexByDecompOffset(off))
	}

	for _, id := range []int64{-1, 2, 99} {
		assert.Nil(t, d.GetIndexByID(id))
	}
}

func TestDecoderIsMetadataOnly(t *testing.T) {
	t.Parallel()

	d, err := NewDecoder(checksum[17+18:])
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	_, ok := d.(Reader)
	assert.False(t, ok)
}

func TestDecoderCloseReleasesIndex(t *testing.T) {
	t.Parallel()

	d, err := NewDecoder(checksum[17+18:])
	require.NoError(t, err)

	impl := d.(*seekTableDecoder)
	table := impl.table.Load()
	require.NotNil(t, table)
	require.NotEmpty(t, table.entries)
	require.NotZero(t, table.size)

	require.NoError(t, d.Close())
	assert.Nil(t, impl.table.Load())
	assert.Zero(t, d.Size())
	assert.Zero(t, d.NumFrames())
	assert.Nil(t, d.GetIndexByID(0))
	assert.Nil(t, d.GetIndexByDecompOffset(0))

	require.NoError(t, d.Close())
	assert.Nil(t, impl.table.Load())
}
