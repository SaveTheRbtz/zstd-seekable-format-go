package seekable

import (
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/klauspost/compress/zstd"

	"encoding/binary"

	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type seekableWriterImpl struct {
	w            io.Writer
	enc          *zstd.Encoder
	frameEntries []seekTableEntry

	o writerOptions

	once *sync.Once
}

type SeekableZSTDWriter interface {
	io.WriteCloser
}

func NewWriter(w io.Writer, opts ...WOption) (SeekableZSTDWriter, error) {
	sw := seekableWriterImpl{
		w:    w,
		once: &sync.Once{},
	}

	sw.o.setDefault()
	for _, o := range opts {
		err := o(&sw.o)
		if err != nil {
			return nil, err
		}
	}

	var err error
	sw.enc, err = zstd.NewWriter(nil, sw.o.zstdEOpts...)
	if err != nil {
		return nil, err
	}
	return &sw, nil
}

func (s *seekableWriterImpl) Write(src []byte) (int, error) {
	if len(src) > math.MaxUint32 {
		return 0, fmt.Errorf("chunk size too big for seekable format: %d > %d",
			len(src), math.MaxUint32)
	}

	dst := s.enc.EncodeAll(src, nil)

	if len(dst) > math.MaxUint32 {
		return 0, fmt.Errorf("result size too big for seekable format: %d > %d",
			len(src), math.MaxUint32)
	}

	entry := seekTableEntry{
		CompressedSize:   uint32(len(dst)),
		DecompressedSize: uint32(len(src)),
		Checksum:         uint32((xxhash.Sum64(src) << 32) >> 32),
	}
	s.frameEntries = append(s.frameEntries, entry)

	s.o.logger.Debug("appending frame", zap.Object("frame", &entry))
	return s.w.Write(dst)
}

func (s *seekableWriterImpl) Close() (err error) {
	s.once.Do(func() {
		err = multierr.Append(err, s.writeSeekTable())
	})

	s.frameEntries = nil
	err = multierr.Append(err, s.enc.Close())
	return
}

func (s *seekableWriterImpl) writeSeekTable() error {
	seekTable := make([]byte, len(s.frameEntries)*12+9)
	for i, e := range s.frameEntries {
		e.marshalBinaryInline(seekTable[i*12 : (i+1)*12])
	}

	if len(s.frameEntries) > math.MaxUint32 {
		return fmt.Errorf("number of frames for seekable format: %d > %d",
			len(s.frameEntries), math.MaxUint32)
	}

	footer := seekTableFooter{
		NumberOfFrames: uint32(len(s.frameEntries)),
		SeekTableDescriptor: SeekTableDescriptor{
			ChecksumFlag: true,
		},
		SeekableMagicNumber: seekableMagicNumber,
	}

	footer.marshalBinaryInline(seekTable[len(s.frameEntries)*12 : len(s.frameEntries)*12+9])
	seekTableBytes, err := createSkippableFrame(seekableTag, seekTable)
	if err != nil {
		return err
	}

	_, err = s.w.Write(seekTableBytes)
	return err
}

// https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md#skippable-frames

// | `Magic_Number` | `Frame_Size` | `User_Data` |
// |:--------------:|:------------:|:-----------:|
// |   4 bytes      |  4 bytes     |   n bytes   |

// Skippable frames allow the insertion of user-defined metadata
// into a flow of concatenated frames.

// __`Magic_Number`__

// 4 Bytes, __little-endian__ format.
// Value : 0x184D2A5?, which means any value from 0x184D2A50 to 0x184D2A5F.
// All 16 values are valid to identify a skippable frame.
// This specification doesn't detail any specific tagging for skippable frames.

// __`Frame_Size`__

// This is the size, in bytes, of the following `User_Data`
// (without including the magic number nor the size field itself).
// This field is represented using 4 Bytes, __little-endian__ format, unsigned 32-bits.
// This means `User_Data` can’t be bigger than (2^32-1) bytes.

// __`User_Data`__

// The `User_Data` can be anything. Data will just be skipped by the decoder.
func createSkippableFrame(tag uint32, payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, nil
	}

	if tag > 0xf {
		return nil, fmt.Errorf("requested tag (%d) > 0xf", tag)
	}

	if len(payload) > math.MaxUint32 {
		return nil, fmt.Errorf("requested skippable frame size (%d) > max uint32", len(payload))
	}

	dst := make([]byte, 8, len(payload)+8)
	binary.LittleEndian.PutUint32(dst[0:], skippableFrameMagic+tag)
	binary.LittleEndian.PutUint32(dst[4:], uint32(len(payload)))
	return append(dst, payload...), nil
}
