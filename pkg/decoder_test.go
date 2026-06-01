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
			checksum[indexOff0.CompOffset:indexOff0.CompOffset+uint64(indexOff0.CompSize)], nil,
		)
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
			checksum[indexOff1.CompOffset:indexOff1.CompOffset+uint64(indexOff1.CompSize)], nil,
		)
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

func TestDecoderRejectsSeekTableEntryCountMismatch(t *testing.T) {
	t.Parallel()

	seekTable := mustCreateSeekTableFrame(t, []seekTableEntry{
		{CompressedSize: 1, DecompressedSize: 1},
		{CompressedSize: 1, DecompressedSize: 1},
	}, 1)

	d, err := NewDecoder(seekTable, nil)
	require.ErrorContains(t, err, "seek table entry count mismatch")
	assert.Nil(t, d)
}

func TestDecoderZeroSizeEntries(t *testing.T) {
	t.Parallel()

	entries := []seekTableEntry{
		{CompressedSize: 2, DecompressedSize: 0},
		{CompressedSize: 10, DecompressedSize: 3},
		{CompressedSize: 5, DecompressedSize: 0},
		{CompressedSize: 20, DecompressedSize: 4},
		{CompressedSize: 7, DecompressedSize: 0},
	}
	seekTable := mustCreateSeekTableFrame(t, entries, uint32(len(entries)))

	d, err := NewDecoder(seekTable, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	assert.Equal(t, int64(7), d.Size())
	assert.Equal(t, int64(5), d.NumFrames())

	indexID0 := d.GetIndexByID(0)
	require.NotNil(t, indexID0)
	assert.Equal(t, int64(0), indexID0.ID)
	assert.Equal(t, uint64(0), indexID0.DecompOffset)
	assert.Equal(t, uint32(0), indexID0.DecompSize)

	indexID2 := d.GetIndexByID(2)
	require.NotNil(t, indexID2)
	assert.Equal(t, int64(2), indexID2.ID)
	assert.Equal(t, uint64(3), indexID2.DecompOffset)
	assert.Equal(t, uint32(0), indexID2.DecompSize)

	indexID4 := d.GetIndexByID(4)
	require.NotNil(t, indexID4)
	assert.Equal(t, int64(4), indexID4.ID)
	assert.Equal(t, uint64(7), indexID4.DecompOffset)
	assert.Equal(t, uint32(0), indexID4.DecompSize)

	indexID1 := d.GetIndexByID(1)
	require.NotNil(t, indexID1)
	assert.Equal(t, int64(1), indexID1.ID)
	assert.Equal(t, uint64(0), indexID1.DecompOffset)
	assert.Equal(t, uint32(3), indexID1.DecompSize)

	indexID3 := d.GetIndexByID(3)
	require.NotNil(t, indexID3)
	assert.Equal(t, int64(3), indexID3.ID)
	assert.Equal(t, uint64(3), indexID3.DecompOffset)
	assert.Equal(t, uint32(4), indexID3.DecompSize)

	for _, off := range []uint64{0, 1, 2} {
		index := d.GetIndexByDecompOffset(off)
		require.NotNil(t, index)
		assert.Equal(t, int64(1), index.ID)
	}

	for _, off := range []uint64{3, 4, 6} {
		index := d.GetIndexByDecompOffset(off)
		require.NotNil(t, index)
		assert.Equal(t, int64(3), index.ID)
	}

	assert.Nil(t, d.GetIndexByDecompOffset(7))
}

func mustCreateSeekTableFrame(t testing.TB, entries []seekTableEntry, numberOfFrames uint32) []byte {
	t.Helper()

	const entrySize = 12
	seekTable := make([]byte, len(entries)*entrySize+seekTableFooterOffset)
	for i, entry := range entries {
		entry.marshalBinaryInline(seekTable[i*entrySize : (i+1)*entrySize])
	}

	footer := seekTableFooter{
		NumberOfFrames: numberOfFrames,
		SeekTableDescriptor: seekTableDescriptor{
			ChecksumFlag: true,
		},
		SeekableMagicNumber: seekableMagicNumber,
	}
	footer.marshalBinaryInline(seekTable[len(entries)*entrySize:])

	frame, err := createSkippableFrame(seekableTag, seekTable)
	require.NoError(t, err)
	return frame
}
