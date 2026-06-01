package seekable

import "sort"

type seekTable struct {
	entries   []FrameOffsetEntry
	checksums bool
}

// SeekTable provides random-access metadata parsed from a Zstandard seek-table skippable frame.
//
// Use NewSeekTable to construct a SeekTable from bytes written by Writer.Close
// or returned by Encoder.EndStream. Lookup methods can be used concurrently.
type SeekTable interface {
	// Size returns the size of the decompressed stream.
	Size() uint64

	// NumFrames returns the number of frames in the seek table.
	NumFrames() int64

	// EntryByDecompressedOffset returns the frame containing off in the decompressed stream.
	// It returns false if off is greater than or equal to Size().
	EntryByDecompressedOffset(off uint64) (FrameOffsetEntry, bool)

	// EntryByID returns the frame with id.
	// It returns false if id is greater than or equal to NumFrames() or less than 0.
	EntryByID(id int64) (FrameOffsetEntry, bool)
}

var _ SeekTable = (*seekTable)(nil)

// NewSeekTable parses the seek-table skippable frame written by Writer.Close
// or returned by Encoder.EndStream.
func NewSeekTable(buf []byte) (SeekTable, error) {
	table, err := parseSeekTableFrame(buf)
	if err != nil {
		return nil, err
	}

	return &table, nil
}

func (t seekTable) Size() uint64 {
	if len(t.entries) == 0 {
		return 0
	}

	last := t.entries[len(t.entries)-1]
	return last.DecompOffset + uint64(last.DecompSize)
}

func (t seekTable) NumFrames() int64 {
	return int64(len(t.entries))
}

func (t seekTable) EntryByDecompressedOffset(off uint64) (FrameOffsetEntry, bool) {
	if off >= t.Size() {
		return FrameOffsetEntry{}, false
	}

	// Find the first frame whose decompressed range contains off; this skips
	// zero-size entries that share an offset with a following non-empty frame.
	n := sort.Search(len(t.entries), func(n int) bool {
		return t.entries[n].DecompOffset+uint64(t.entries[n].DecompSize) > off
	})
	if n == len(t.entries) || t.entries[n].DecompOffset > off {
		return FrameOffsetEntry{}, false
	}
	return t.entries[n], true
}

func (t seekTable) EntryByID(id int64) (FrameOffsetEntry, bool) {
	if id < 0 || id >= int64(len(t.entries)) {
		return FrameOffsetEntry{}, false
	}

	return t.entries[int(id)], true
}
