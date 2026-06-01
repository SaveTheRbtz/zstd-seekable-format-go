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

	_, err := parseSeekTableFrame(frame)
	require.ErrorContains(t, err, "seek table entry count mismatch")
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

	table, err := parseSeekTableFrame(frame)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), table.Size())
	assert.Equal(t, int64(5), table.NumFrames())

	for id, tc := range []struct {
		name         string
		decompOffset uint64
		decompSize   uint32
	}{
		{name: "LeadingZero", decompOffset: 0, decompSize: 0},
		{name: "FirstNonZero", decompOffset: 0, decompSize: 3},
		{name: "MiddleZero", decompOffset: 3, decompSize: 0},
		{name: "SecondNonZero", decompOffset: 3, decompSize: 4},
		{name: "TrailingZero", decompOffset: 7, decompSize: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			index, ok := table.EntryByID(int64(id))
			require.True(t, ok)
			assert.Equal(t, tc.decompOffset, index.DecompOffset)
			assert.Equal(t, tc.decompSize, index.DecompSize)
		})
	}

	for _, tc := range []struct {
		name    string
		offsets []uint64
		id      int64
	}{
		{name: "FirstNonZero", offsets: []uint64{0, 2}, id: 1},
		{name: "SecondNonZero", offsets: []uint64{3, 6}, id: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, off := range tc.offsets {
				index, ok := table.EntryByDecompressedOffset(off)
				require.True(t, ok)
				assert.Equal(t, tc.id, index.ID)
			}
		})
	}

	_, ok := table.EntryByDecompressedOffset(7)
	assert.False(t, ok)
}

func TestSeekTableFooterParsing(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		buf          []byte
		wantChecksum bool
		err          string
	}{
		{
			name:         "Checksum",
			buf:          seekTableFooterBytes(1 << 7),
			wantChecksum: true,
		},
		{
			name: "NoChecksum",
			buf:  seekTableFooterBytes(0x00),
		},
		{
			name:         "UnusedBits",
			buf:          seekTableFooterBytes((1 << 7) + 0x01 + 0x2),
			wantChecksum: true,
		},
		{
			name: "ReservedBits",
			buf:  seekTableFooterBytes(0x84),
			err:  "footer reserved bits",
		},
		{
			name: "ReservedHighBit",
			buf:  seekTableFooterBytes(0x80 + 0x40),
			err:  "footer reserved bits",
		},
		{
			name: "Size",
			buf: []byte{
				0xb1, 0xea, 0x92, 0x8f,
			},
			err: "footer length mismatch",
		},
		{
			name: "Magic",
			buf:  []byte{0x00, 0x00, 0x00, 0x00, 0x80, 0xea, 0x92, 0x8f, 0xb1},
			err:  "footer magic mismatch",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var footer seekTableFooter
			err := footer.UnmarshalBinary(tc.buf)
			if tc.err == "" {
				require.NoError(t, err)
				assert.Equal(t, tc.wantChecksum, footer.SeekTableDescriptor.ChecksumFlag)
				return
			}
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func seekTableFooterBytes(descriptor byte) []byte {
	return []byte{
		0x00, 0x00, 0x00, 0x00,
		descriptor,
		0xb1, 0xea, 0x92, 0x8f,
	}
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
