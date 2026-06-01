package seekable

type seekTable struct {
	frameIndex
	checksums bool
}

// NewSeekTable parses a seek table into random-access metadata.
// The seek table can be produced by either Writer's WriteSeekTable or Encoder's EndStream.
// Lookup methods can be used concurrently.
func NewSeekTable(buf []byte) (*seekTable, error) {
	table, err := parseSeekTableFrame(buf)
	if err != nil {
		return nil, err
	}

	return &table, nil
}

func (t seekTable) Size() int64 {
	return t.size
}

func (t seekTable) NumFrames() int64 {
	return t.numFrames()
}

// EntryByDecompressedOffset returns the frame containing off in the decompressed stream.
// It returns false if off is greater than or equal to Size().
func (t seekTable) EntryByDecompressedOffset(off uint64) (FrameOffsetEntry, bool) {
	return t.entryByDecompressedOffset(off)
}

// EntryByID returns the frame with id.
// It returns false if id is greater than or equal to NumFrames() or less than 0.
func (t seekTable) EntryByID(id int64) (FrameOffsetEntry, bool) {
	return t.entryByID(id)
}
