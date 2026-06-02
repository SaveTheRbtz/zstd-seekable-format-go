package seekable

import (
	"fmt"
	"log/slog"

	"github.com/cespare/xxhash/v2"
)

// Encoder is a byte-oriented API that is useful where wrapping io.Writer is not desirable.
//
// Each non-empty Encode call returns one compressed Zstandard frame and appends
// one entry to the in-memory seek table. EndStream returns the final seek-table
// skippable frame, which must be appended after all encoded frames to form a
// complete seekable stream. EndStream finalizes the encoder. After EndStream,
// Encode and EndStream return ErrClosed.
type Encoder interface {
	// Encode returns compressed data and appends a frame to in-memory seek table.
	// Empty inputs return an empty slice and do not add seek-table entries.
	Encode(src []byte) ([]byte, error)

	// EndStream returns the in-memory seek table as a Zstandard skippable frame
	// and finalizes the encoder.
	EndStream() ([]byte, error)
}

// NewEncoder returns a byte-oriented encoder that uses encoder for Zstandard compression.
//
// The caller remains responsible for closing encoder, if it requires closing.
func NewEncoder(encoder ZSTDEncoder, opts ...wOption) (Encoder, error) {
	sw, err := NewWriter(nil, encoder, opts...)
	if err != nil {
		return nil, err
	}

	return sw.(*writerImpl), err
}

func (s *writerImpl) encodeOne(src []byte) ([]byte, seekTableEntry, error) {
	if int64(len(src)) > maxChunkSize {
		return nil, seekTableEntry{},
			fmt.Errorf("chunk size too big for seekable format: %d > %d",
				len(src), maxChunkSize)
	}

	if len(src) == 0 {
		return nil, seekTableEntry{}, nil
	}

	dst := s.enc.EncodeAll(src, nil)

	if int64(len(dst)) > maxChunkSize {
		return nil, seekTableEntry{},
			fmt.Errorf("result size too big for seekable format: %d > %d",
				len(dst), maxChunkSize)
	}

	return dst, seekTableEntry{
		CompressedSize:   uint32(len(dst)),
		DecompressedSize: uint32(len(src)),
		Checksum:         uint32(xxhash.Sum64(src)),
	}, nil
}

func (s *writerImpl) Encode(src []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrClosed
	}
	if len(src) == 0 {
		return []byte{}, nil
	}

	dst, entry, err := s.encodeOne(src)
	if err != nil {
		return nil, err
	}

	s.logger.Debug("appending frame", slog.Any("frame", &entry))
	s.frameEntries = append(s.frameEntries, entry)
	return dst, nil
}

func (s *writerImpl) EndStream() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.endStreamLocked()
}

func (s *writerImpl) endStreamLocked() ([]byte, error) {
	if s.closed {
		return nil, ErrClosed
	}

	if int64(len(s.frameEntries)) > maxNumberOfFrames {
		return nil, fmt.Errorf("number of frames for seekable format: %d > %d",
			len(s.frameEntries), maxNumberOfFrames)
	}

	seekTable := make([]byte, len(s.frameEntries)*12+9)
	for i, e := range s.frameEntries {
		e.marshalBinaryInline(seekTable[i*12 : (i+1)*12])
	}

	footer := seekTableFooter{
		NumberOfFrames: uint32(len(s.frameEntries)),
		SeekTableDescriptor: seekTableDescriptor{
			ChecksumFlag: true,
		},
		SeekableMagicNumber: seekableMagicNumber,
	}

	footer.marshalBinaryInline(seekTable[len(s.frameEntries)*12 : len(s.frameEntries)*12+9])
	frame, err := createSkippableFrame(seekableTag, seekTable)
	if err != nil {
		return nil, err
	}

	s.closed = true
	s.frameEntries = nil
	return frame, nil
}
