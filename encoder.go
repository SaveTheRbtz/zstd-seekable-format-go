package seekable

import (
	"fmt"
	"math"

	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/options"
)

// Encoder is a byte-oriented API that is useful where wrapping io.Writer is not desirable.
type Encoder interface {
	// Encode returns compressed data and appends a frame to in-memory seek table.
	Encode(src []byte) ([]byte, error)

	// EndStream returns in-memory seek table as a ZSTD's skippable frame.
	EndStream() ([]byte, error)
}

func NewEncoder(encoder ZSTDEncoder, opts ...options.WOption) (Encoder, error) {
	sw, err := NewWriter(nil, encoder, opts...)
	if err != nil {
		return nil, err
	}

	return sw.(*writerImpl), err
}

func (s *writerImpl) Encode(src []byte) ([]byte, error) {
	if len(src) > math.MaxUint32 {
		return nil, fmt.Errorf("chunk size too big for seekable format: %d > %d",
			len(src), math.MaxUint32)
	}

	if len(src) == 0 {
		return nil, nil
	}

	dst := s.enc.EncodeAll(src, nil)

	if len(dst) > math.MaxUint32 {
		return nil, fmt.Errorf("result size too big for seekable format: %d > %d",
			len(src), math.MaxUint32)
	}

	entry := seekTableEntry{
		CompressedSize:   uint32(len(dst)),
		DecompressedSize: uint32(len(src)),
		Checksum:         uint32((xxhash.Sum64(src) << 32) >> 32),
	}

	s.o.Logger.Debug("appending frame", zap.Object("frame", &entry))
	s.frameEntries = append(s.frameEntries, entry)

	return dst, nil
}

func (s *writerImpl) EndStream() ([]byte, error) {
	seekTable := make([]byte, len(s.frameEntries)*12+9)
	for i, e := range s.frameEntries {
		e.marshalBinaryInline(seekTable[i*12 : (i+1)*12])
	}

	if len(s.frameEntries) > math.MaxUint32 {
		return nil, fmt.Errorf("number of frames for seekable format: %d > %d",
			len(s.frameEntries), math.MaxUint32)
	}

	footer := seekTableFooter{
		NumberOfFrames: uint32(len(s.frameEntries)),
		SeekTableDescriptor: seekTableDescriptor{
			ChecksumFlag: true,
		},
		SeekableMagicNumber: seekableMagicNumber,
	}

	footer.marshalBinaryInline(seekTable[len(s.frameEntries)*12 : len(s.frameEntries)*12+9])
	return createSkippableFrame(seekableTag, seekTable)
}
