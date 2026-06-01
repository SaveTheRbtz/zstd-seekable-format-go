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

func TestDecoderRejectsSeekTableEntryCountMismatch(t *testing.T) {
	t.Parallel()

	seekTable := mustCreateSeekTableFrame(t, []seekTableEntry{
		{CompressedSize: 1, DecompressedSize: 1},
		{CompressedSize: 1, DecompressedSize: 1},
	}, 1)

	d, err := NewDecoder(seekTable)
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

	d, err := NewDecoder(seekTable)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	assert.Equal(t, int64(7), d.Size())
	assert.Equal(t, int64(5), d.NumFrames())

	for _, tc := range []struct {
		name         string
		id           int64
		decompOffset uint64
		decompSize   uint32
	}{
		{name: "LeadingZero", id: 0, decompOffset: 0, decompSize: 0},
		{name: "FirstNonZero", id: 1, decompOffset: 0, decompSize: 3},
		{name: "MiddleZero", id: 2, decompOffset: 3, decompSize: 0},
		{name: "SecondNonZero", id: 3, decompOffset: 3, decompSize: 4},
		{name: "TrailingZero", id: 4, decompOffset: 7, decompSize: 0},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			index := d.GetIndexByID(tc.id)
			require.NotNil(t, index)
			assert.Equal(t, tc.id, index.ID)
			assert.Equal(t, tc.decompOffset, index.DecompOffset)
			assert.Equal(t, tc.decompSize, index.DecompSize)
		})
	}

	for _, tc := range []struct {
		name    string
		offsets []uint64
		id      int64
	}{
		{name: "FirstNonZero", offsets: []uint64{0, 1, 2}, id: 1},
		{name: "SecondNonZero", offsets: []uint64{3, 4, 6}, id: 3},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, off := range tc.offsets {
				index := d.GetIndexByDecompOffset(off)
				require.NotNil(t, index)
				assert.Equal(t, tc.id, index.ID)
			}
		})
	}

	assert.Nil(t, d.GetIndexByDecompOffset(7))
}

func mustCreateSeekTableFrame(t testing.TB, entries []seekTableEntry, numberOfFrames uint32) []byte {
	t.Helper()

	entrySize := int(seekTableEntrySize(true))
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
