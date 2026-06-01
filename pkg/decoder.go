package seekable

import (
	"sync/atomic"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/env"
)

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

// Decoder is a closable parsed seek table that is useful when wrapping io.ReadSeeker is not desirable.
type Decoder interface {
	SeekTable

	// Close closes the decoder freeing up any resources.
	Close() error
}

type seekTableDecoder struct {
	table atomic.Pointer[parsedSeekTable]
}

var _ Decoder = (*seekTableDecoder)(nil)

// NewDecoder creates a metadata Decoder from a seek table.
// The seek table can be produced by either Writer's WriteSeekTable or Encoder's EndStream.
// Decoder can be used concurrently.
func NewDecoder(seekTable []byte) (Decoder, error) {
	table, err := parseSeekTable(seekTable)
	if err != nil {
		return nil, err
	}

	d := &seekTableDecoder{}
	d.table.Store(&table)
	return d, nil
}

func (d *seekTableDecoder) Size() int64 {
	table := d.table.Load()
	if table == nil {
		return 0
	}
	return table.size
}

func (d *seekTableDecoder) NumFrames() int64 {
	table := d.table.Load()
	if table == nil {
		return 0
	}
	return table.numFrames()
}

func (d *seekTableDecoder) Close() error {
	d.table.Store(nil)
	return nil
}

func (d *seekTableDecoder) GetIndexByDecompOffset(off uint64) (found *env.FrameOffsetEntry) {
	table := d.table.Load()
	if table == nil {
		return nil
	}
	return table.byDecompOffset(off)
}

func (d *seekTableDecoder) GetIndexByID(id int64) (found *env.FrameOffsetEntry) {
	table := d.table.Load()
	if table == nil {
		return nil
	}
	return table.byID(id)
}
