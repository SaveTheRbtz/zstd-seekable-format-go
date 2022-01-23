package seekable

import (
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/klauspost/compress/zstd"

	"go.uber.org/multierr"
	"go.uber.org/zap"
)

var (
	_ io.Writer = (*writerImpl)(nil)
	_ io.Closer = (*writerImpl)(nil)
)

type writerImpl struct {
	w            io.Writer
	enc          *zstd.Encoder
	frameEntries []SeekTableEntry

	o writerOptions

	once *sync.Once
}

type ZSTDWriter interface {
	io.WriteCloser
}

func NewWriter(w io.Writer, opts ...WOption) (ZSTDWriter, error) {
	sw := writerImpl{
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

func (s *writerImpl) Write(src []byte) (int, error) {
	if len(src) > math.MaxUint32 {
		return 0, fmt.Errorf("chunk size too big for seekable format: %d > %d",
			len(src), math.MaxUint32)
	}

	dst := s.enc.EncodeAll(src, nil)

	if len(dst) > math.MaxUint32 {
		return 0, fmt.Errorf("result size too big for seekable format: %d > %d",
			len(src), math.MaxUint32)
	}

	entry := SeekTableEntry{
		CompressedSize:   uint32(len(dst)),
		DecompressedSize: uint32(len(src)),
		Checksum:         uint32((xxhash.Sum64(src) << 32) >> 32),
	}

	n, err := s.w.Write(dst)
	if err != nil {
		return 0, err
	}
	if n != len(dst) {
		return 0, fmt.Errorf("partial write: %d out of %d", n, len(dst))
	}

	s.o.logger.Debug("appending frame", zap.Object("frame", &entry))
	s.frameEntries = append(s.frameEntries, entry)

	return len(src), nil
}

func (s *writerImpl) Close() (err error) {
	s.once.Do(func() {
		err = multierr.Append(err, s.writeSeekTable())
	})

	s.frameEntries = nil
	err = multierr.Append(err, s.enc.Close())
	return
}

func (s *writerImpl) writeSeekTable() error {
	seekTable := make([]byte, len(s.frameEntries)*12+9)
	for i, e := range s.frameEntries {
		e.marshalBinaryInline(seekTable[i*12 : (i+1)*12])
	}

	if len(s.frameEntries) > math.MaxUint32 {
		return fmt.Errorf("number of frames for seekable format: %d > %d",
			len(s.frameEntries), math.MaxUint32)
	}

	footer := SeekTableFooter{
		NumberOfFrames: uint32(len(s.frameEntries)),
		SeekTableDescriptor: SeekTableDescriptor{
			ChecksumFlag: true,
		},
		SeekableMagicNumber: seekableMagicNumber,
	}

	footer.marshalBinaryInline(seekTable[len(s.frameEntries)*12 : len(s.frameEntries)*12+9])
	seekTableBytes, err := CreateSkippableFrame(seekableTag, seekTable)
	if err != nil {
		return err
	}

	_, err = s.w.Write(seekTableBytes)
	return err
}
