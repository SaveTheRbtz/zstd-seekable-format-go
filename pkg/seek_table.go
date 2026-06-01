package seekable

import "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/env"

type seekTable struct {
	frameIndex
	checksums bool
}

// NewSeekTable parses a seek table into random-access metadata.
// The seek table can be produced by either Writer's WriteSeekTable or Encoder's EndStream.
// The returned value exposes Size, NumFrames, GetIndexByID, and GetIndexByDecompOffset.
// Lookup methods can be used concurrently.
func NewSeekTable(buf []byte) (*seekTable, error) {
	table, err := parseSeekTable(buf)
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

// GetIndexByDecompOffset returns FrameOffsetEntry for an offset in the decompressed stream.
// Will return nil if offset is greater or equal than Size().
func (t seekTable) GetIndexByDecompOffset(off uint64) (found *env.FrameOffsetEntry) {
	return t.byDecompOffset(off)
}

// GetIndexByID returns FrameOffsetEntry for a given frame id.
// Will return nil if offset is greater or equal than NumFrames() or less than 0.
func (t seekTable) GetIndexByID(id int64) (found *env.FrameOffsetEntry) {
	return t.byID(id)
}
