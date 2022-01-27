package seekable

import (
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
)

func TestDecoder(t *testing.T) {
	t.Parallel()

	dec, err := zstd.NewReader(nil)
	assert.NoError(t, err)

	d, err := NewDecoder(checksum[17+18:], dec)
	assert.NoError(t, err)

	assert.Equal(t, int64(len(sourceString)), d.Size())
	assert.Equal(t, int64(2), d.NumFrames())

	// First frame.

	bytes1 := []byte("test")
	for _, off := range []uint64{0, 1, 3} {
		index_off_0 := d.GetIndexByDecompOffset(off)
		index_id_0 := d.GetIndexByID(0)
		assert.Equal(t, index_off_0, index_id_0)
		assert.NotEqual(t, index_off_0, nil)
		assert.Equal(t, int64(0), index_off_0.ID)
		assert.Equal(t, uint32(len(bytes1)), index_off_0.DecompSize)
		assert.NotEqual(t, uint32(0), index_off_0.Checksum)

		decomp, err := dec.DecodeAll(
			checksum[index_off_0.CompOffset:index_off_0.CompOffset+uint64(index_off_0.CompSize)], nil)
		assert.NoError(t, err)
		assert.Equal(t, decomp, bytes1)
	}

	// Second frame.

	bytes2 := []byte("test2")
	for _, off := range []uint64{4, 5, 8} {
		index_off_1 := d.GetIndexByDecompOffset(off)
		index_id_1 := d.GetIndexByID(1)
		assert.Equal(t, index_off_1, index_id_1)
		assert.NotEqual(t, index_off_1, nil)
		assert.Equal(t, int64(1), index_off_1.ID)
		assert.Equal(t, uint32(len(bytes2)), index_off_1.DecompSize)
		assert.NotEqual(t, uint32(0), index_off_1.Checksum)

		decomp, err := dec.DecodeAll(
			checksum[index_off_1.CompOffset:index_off_1.CompOffset+uint64(index_off_1.CompSize)], nil)
		assert.NoError(t, err)
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
