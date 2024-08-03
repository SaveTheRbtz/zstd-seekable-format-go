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

	d, err := NewDecoder(checksum[17+18:], dec)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	assert.Equal(t, int64(len(sourceString)), d.Size())
	assert.Equal(t, int64(2), d.NumFrames())

	// First frame.

	bytes1 := []byte("test")
	for _, off := range []uint64{0, 1, 3} {
		indexOff0 := d.GetIndexByDecompOffset(off)
		indexID0 := d.GetIndexByID(0)
		assert.Equal(t, indexOff0, indexID0)
		assert.NotNil(t, indexOff0)
		assert.Equal(t, int64(0), indexOff0.ID)
		assert.Equal(t, uint32(len(bytes1)), indexOff0.DecompSize)
		assert.NotEqual(t, uint32(0), indexOff0.Checksum)

		decomp, err := dec.DecodeAll(
			checksum[indexOff0.CompOffset:indexOff0.CompOffset+uint64(indexOff0.CompSize)], nil)
		require.NoError(t, err)
		assert.Equal(t, decomp, bytes1)
	}

	// Second frame.

	bytes2 := []byte("test2")
	for _, off := range []uint64{4, 5, 8} {
		indexOff1 := d.GetIndexByDecompOffset(off)
		indexID1 := d.GetIndexByID(1)
		assert.Equal(t, indexOff1, indexID1)
		assert.NotNil(t, indexOff1)
		assert.Equal(t, int64(1), indexOff1.ID)
		assert.Equal(t, uint32(len(bytes2)), indexOff1.DecompSize)
		assert.NotEqual(t, uint32(0), indexOff1.Checksum)

		decomp, err := dec.DecodeAll(
			checksum[indexOff1.CompOffset:indexOff1.CompOffset+uint64(indexOff1.CompSize)], nil)
		require.NoError(t, err)
		assert.Equal(t, decomp, bytes2)
	}

	// Out of bounds.

	for _, off := range []uint64{9, 99} {
		assert.Nil(t, d.GetIndexByDecompOffset(off))
	}

	for _, id := range []int64{-1, 2, 99} {
		assert.Nil(t, d.GetIndexByID(id))
	}
}
