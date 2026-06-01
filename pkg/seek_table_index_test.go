package seekable

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSeekTableRejectsEntryCountMismatch(t *testing.T) {
	t.Parallel()

	frame := mustCreateSeekTableFrame(t, []seekTableEntry{
		{CompressedSize: 1, DecompressedSize: 1},
		{CompressedSize: 1, DecompressedSize: 1},
	}, 1)

	table, err := parseSeekTable(frame)
	require.ErrorContains(t, err, "seek table entry count mismatch")
	assert.Equal(t, seekTable{}, table)
}

func TestParseSeekTableZeroSizeEntries(t *testing.T) {
	t.Parallel()

	entries := []seekTableEntry{
		{CompressedSize: 2, DecompressedSize: 0},
		{CompressedSize: 10, DecompressedSize: 3},
		{CompressedSize: 5, DecompressedSize: 0},
		{CompressedSize: 20, DecompressedSize: 4},
		{CompressedSize: 7, DecompressedSize: 0},
	}
	frame := mustCreateSeekTableFrame(t, entries, uint32(len(entries)))

	table, err := parseSeekTable(frame)
	require.NoError(t, err)
	assert.True(t, table.checksums)
	assert.Equal(t, int64(7), table.size)
	assert.Equal(t, int64(5), table.numFrames())

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
			index := table.byID(tc.id)
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
				index := table.byDecompOffset(off)
				require.NotNil(t, index)
				assert.Equal(t, tc.id, index.ID)
			}
		})
	}

	assert.Nil(t, table.byDecompOffset(7))
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
