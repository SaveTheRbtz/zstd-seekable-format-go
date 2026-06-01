package seekable

import "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/env"

// Decoder is a byte-oriented API that is useful for cases where wrapping io.ReadSeeker is not desirable.
type Decoder interface {
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

	// Close closes the decoder freeing up any resources.
	Close() error
}

type decoderImpl struct {
	index frameIndex
}

var _ Decoder = (*decoderImpl)(nil)

// NewDecoder creates a byte-oriented Decoder from a seek table.
// The seek table can be produced by either Writer's WriteSeekTable or Encoder's EndStream.
// Decoder can be used concurrently.
func NewDecoder(seekTable []byte) (Decoder, error) {
	table, err := parseSeekTable(seekTable)
	if err != nil {
		return nil, err
	}

	return &decoderImpl{
		index: table.frameIndex,
	}, nil
}

func (d *decoderImpl) Size() int64 {
	return d.index.size
}

func (d *decoderImpl) NumFrames() int64 {
	return d.index.numFrames()
}

func (d *decoderImpl) Close() error {
	return nil
}

func (d *decoderImpl) GetIndexByDecompOffset(off uint64) (found *env.FrameOffsetEntry) {
	return d.index.byDecompOffset(off)
}

func (d *decoderImpl) GetIndexByID(id int64) (found *env.FrameOffsetEntry) {
	return d.index.byID(id)
}
