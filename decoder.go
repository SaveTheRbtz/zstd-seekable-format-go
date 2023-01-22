package seekable

import (
	"github.com/SaveTheRbtz/zstd-seekable-format-go/env"
)

// Decoder is a byte-oriented API that is useful for cases where wrapping io.ReadSeeker is not desirable.
type Decoder interface {
	// GetIndexByDecompOffset returns SeekIndexEntry for an offset in the decompressed stream.
	// Will return nil if offset is greater or equal than Size().
	GetIndexByDecompOffset(off uint64) *env.SeekIndexEntry

	// GetIndexByID returns SeekIndexEntry for a given frame id.
	// Will return nil if offset is greater or equal than NumFrames() or less than 0.
	GetIndexByID(id int64) *env.SeekIndexEntry

	// Size returns the size of the uncompressed stream.
	Size() int64

	// NumFrames returns number of frames in the compressed stream.
	NumFrames() int64

	// Close closes the decoder feeing up any resources.
	Close() error
}

// NewDecoder creates a byte-oriented Decode interface from a given seektable index.
// This index can either be produced by either Writer's WriteSeekIndex or Encoder's EndStream.
// Decoder can be used concurrently.
func NewDecoder(seekTable []byte, decoder ZSTDDecoder, opts ...rOption) (Decoder, error) {
	opts = append(opts, WithREnvironment(&decoderEnv{seekTable: seekTable}))

	sr, err := NewReader(nil, decoder, opts...)
	if err != nil {
		return nil, err
	}

	// Release seekTable reference to not leak memory.
	sr.(*readerImpl).env = nil

	return sr.(*readerImpl), err
}

type decoderEnv struct {
	seekTable []byte
}

func (d *decoderEnv) GetFrameByIndex(_ env.SeekIndexEntry) (p []byte, err error) {
	panic("should not be used")
}

func (d *decoderEnv) ReadSeekIndexFooter() ([]byte, error) {
	return d.seekTable, nil
}

func (d *decoderEnv) ReadSeekIndex(skippableFrameOffset int64) ([]byte, error) {
	return d.seekTable, nil
}

func (r *readerImpl) Size() int64 {
	return r.endOffset
}

func (r *readerImpl) NumFrames() int64 {
	return r.numFrames
}

func (r *readerImpl) GetIndexByDecompOffset(off uint64) (found *env.SeekIndexEntry) {
	if off >= uint64(r.endOffset) {
		return nil
	}

	r.index.DescendLessOrEqual(&env.SeekIndexEntry{DecompOffset: off}, func(index *env.SeekIndexEntry) bool {
		found = index
		return false
	})
	return
}

func (r *readerImpl) GetIndexByID(id int64) (found *env.SeekIndexEntry) {
	if id < 0 {
		return nil
	}

	r.index.Descend(func(index *env.SeekIndexEntry) bool {
		if index.ID == id {
			found = index
			return false
		}
		return true
	})
	return
}
