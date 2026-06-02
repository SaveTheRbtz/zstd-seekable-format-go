package seekable

import "sort"

// SeekTable is parsed random-access metadata from a Zstandard seek-table skippable frame.
//
// Use NewSeekTable to construct a SeekTable from bytes written through
// WriterEnvironment.WriteSeekTable or returned by Encoder.EndStream. Lookup methods
// can be used concurrently.
type SeekTable struct {
	entries   []FrameOffsetEntry
	checksums bool
}

// NewSeekTable parses a seek-table skippable frame.
//
// buf must contain the final seek-table skippable frame itself, including the
// skippable-frame magic number and frame-size header. This is the byte sequence
// returned by Encoder.EndStream or passed to WriterEnvironment.WriteSeekTable, not
// the whole compressed stream.
func NewSeekTable(buf []byte) (*SeekTable, error) {
	table, err := parseSeekTableFrame(buf)
	if err != nil {
		return nil, err
	}

	return &table, nil
}

// Size returns the size of the decompressed stream.
func (t SeekTable) Size() uint64 {
	if len(t.entries) == 0 {
		return 0
	}

	last := t.entries[len(t.entries)-1]
	return last.DecompressedOffset + uint64(last.DecompressedSize)
}

// NumFrames returns the number of frames in the seek table.
func (t SeekTable) NumFrames() int64 {
	return int64(len(t.entries))
}

// HasChecksums reports whether entries in the seek table include checksum fields.
func (t SeekTable) HasChecksums() bool {
	return t.checksums
}

// EntryByDecompressedOffset returns the frame containing off in the decompressed stream.
// It returns false if off is greater than or equal to Size().
func (t SeekTable) EntryByDecompressedOffset(off uint64) (FrameOffsetEntry, bool) {
	if off >= t.Size() {
		return FrameOffsetEntry{}, false
	}

	// Find the first frame whose decompressed range contains off; this skips
	// zero-size entries that share an offset with a following non-empty frame.
	n := sort.Search(len(t.entries), func(n int) bool {
		return t.entries[n].DecompressedOffset+uint64(t.entries[n].DecompressedSize) > off
	})
	if n == len(t.entries) || t.entries[n].DecompressedOffset > off {
		return FrameOffsetEntry{}, false
	}
	return t.entries[n], true
}

// EntryByID returns the frame with id.
// It returns false if id is greater than or equal to NumFrames() or less than 0.
func (t SeekTable) EntryByID(id int64) (FrameOffsetEntry, bool) {
	if id < 0 || id >= int64(len(t.entries)) {
		return FrameOffsetEntry{}, false
	}

	return t.entries[int(id)], true
}
