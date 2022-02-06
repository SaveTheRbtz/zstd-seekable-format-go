package seekable

import "github.com/google/btree"

// Decoder is a byte-oriented API that is useful for cases where wrapping io.ReadSeeker is not desirable.
type Decoder interface {
	// GetIndexByDecompOffset returns FrameOffsetEntry for an offset in the decompressed stream.
	// Will return nil if offset is greater or equal than Size().
	GetIndexByDecompOffset(off uint64) *FrameOffsetEntry

	// GetIndexByID returns FrameOffsetEntry for a given frame id.
	// Will return nil if offset is greater or equal than NumFrames() or less than 0.
	GetIndexByID(id int64) *FrameOffsetEntry

	// Size returns the size of the uncompressed stream.
	Size() int64

	// NumFrames returns number of frames in the compressed stream.
	NumFrames() int64
}

// NewDecoder creates a byte-oriented Decode interface from a given seektable index.
// This index can either be produced by either Writer's WriteSeekTable or Encoder's EndStream.
// Decoder can be used concurrently.
func NewDecoder(seekTable []byte, decoder ZSTDDecoder, opts ...ROption) (Decoder, error) {
	opts = append(opts, WithREnvironment(&decoderEnv{seekTable: seekTable}))

	sr, err := NewReader(nil, decoder, opts...)
	if err != nil {
		return nil, err
	}

	// Release seekTable reference to not leak memory.
	sr.(*ReaderImpl).o.env = nil

	return sr.(*ReaderImpl), err
}

type decoderEnv struct {
	seekTable []byte
}

func (d *decoderEnv) GetFrameByIndex(index FrameOffsetEntry) (p []byte, err error) {
	panic("should not be used")
}

func (d *decoderEnv) ReadFooter() ([]byte, error) {
	return d.seekTable, nil
}

func (d *decoderEnv) ReadSkipFrame(skippableFrameOffset int64) ([]byte, error) {
	return d.seekTable, nil
}

func (s *ReaderImpl) Size() int64 {
	return s.endOffset
}

func (s *ReaderImpl) NumFrames() int64 {
	return s.numFrames
}

func (s *ReaderImpl) GetIndexByDecompOffset(off uint64) (found *FrameOffsetEntry) {
	if off >= uint64(s.endOffset) {
		return nil
	}

	s.index.DescendLessOrEqual(&FrameOffsetEntry{DecompOffset: off}, func(i btree.Item) bool {
		found = i.(*FrameOffsetEntry)
		return false
	})
	return
}

func (s *ReaderImpl) GetIndexByID(id int64) (found *FrameOffsetEntry) {
	if id < 0 {
		return nil
	}

	s.index.Descend(func(i btree.Item) bool {
		index := i.(*FrameOffsetEntry)
		if index.ID == id {
			found = index
			return false
		}
		return true
	})
	return
}
