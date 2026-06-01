package seekable

import "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/env"

// SeekTable provides random-access metadata from a ZSTD seek table.
type SeekTable interface {
	// GetIndexByDecompOffset returns FrameOffsetEntry for an offset in the decompressed stream.
	// Will return nil if offset is greater or equal than Size().
	GetIndexByDecompOffset(off uint64) *env.FrameOffsetEntry

	// GetIndexByID returns FrameOffsetEntry for a given frame id.
	// Will return nil if offset is greater or equal than NumFrames() or less than 0.
	GetIndexByID(id int64) *env.FrameOffsetEntry

	// Size returns the size of the uncompressed stream.
	Size() int64

	// NumFrames returns number of frames in the compressed stream.
	NumFrames() int64
}

// Decoder is a parsed seek table for callers that do not need an io.ReadSeeker.
type Decoder = SeekTable

var _ Decoder = (*parsedSeekTable)(nil)

// NewDecoder creates a metadata Decoder from a seek table.
// The seek table can be produced by either Writer's WriteSeekTable or Encoder's EndStream.
// Lookup methods can be used concurrently.
func NewDecoder(seekTable []byte) (Decoder, error) {
	table, err := parseSeekTable(seekTable)
	if err != nil {
		return nil, err
	}

	return &table, nil
}
