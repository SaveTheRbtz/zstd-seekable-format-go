package seekable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/cespare/xxhash"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/google/btree"
)

var (
	_ io.ReadSeeker = (*ReaderImpl)(nil)
	_ io.ReaderAt   = (*ReaderImpl)(nil)
)

type cachedFrame struct {
	m sync.Mutex

	offset uint64
	data   []byte
}

func (f *cachedFrame) replace(offset uint64, data []byte) {
	f.m.Lock()
	defer f.m.Unlock()

	f.offset = offset
	f.data = data
}

func (f *cachedFrame) get() (uint64, []byte) {
	f.m.Lock()
	defer f.m.Unlock()

	return f.offset, f.data
}

// Environment can be used to inject a custom file reader that is different from normal ReadSeeker.
// This is useful when, for example there is a custom chunking code.
type REnvironment interface {
	// GetFrameByIndex returns the compressed frame by its index.
	GetFrameByIndex(index FrameOffsetEntry) ([]byte, error)
	// ReadFooter returns buffer whose last 9 bytes are interpreted as a `Seek_Table_Footer`.
	ReadFooter() ([]byte, error)
	// ReadSkipFrame returns the full Seek Table Skippable frame
	// including the `Skippable_Magic_Number` and `Frame_Size`.
	ReadSkipFrame(skippableFrameOffset int64) ([]byte, error)
}

// readSeekerEnvImpl is the environment implementation for the io.ReadSeeker.
type readSeekerEnvImpl struct {
	rs io.ReadSeeker
}

func (rs *readSeekerEnvImpl) GetFrameByIndex(index FrameOffsetEntry) (p []byte, err error) {
	p = make([]byte, index.CompSize)
	off := int64(index.CompOffset)

	switch v := rs.rs.(type) {
	case io.ReaderAt:
		_, err = v.ReadAt(p, off)
		if errors.Is(err, io.EOF) {
			err = nil
		}
	default:
		_, err = v.Seek(off, io.SeekStart)
		if err != nil {
			return nil, err
		}
		_, err = io.ReadFull(rs.rs, p)
	}

	return
}

func (rs *readSeekerEnvImpl) ReadFooter() ([]byte, error) {
	n, err := rs.rs.Seek(-seekTableFooterOffset, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to: %d: %w", -seekTableFooterOffset, err)
	}

	buf := make([]byte, seekTableFooterOffset)
	_, err = io.ReadFull(rs.rs, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read footer at: %d: %w", n, err)
	}

	return buf, nil
}

func (rs *readSeekerEnvImpl) ReadSkipFrame(skippableFrameOffset int64) ([]byte, error) {
	n, err := rs.rs.Seek(-skippableFrameOffset, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to: %d: %w", -skippableFrameOffset, err)
	}

	buf := make([]byte, skippableFrameOffset)
	_, err = io.ReadFull(rs.rs, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read skippable frame header at: %d: %w", n, err)
	}
	return buf, nil
}

type ReaderImpl struct {
	dec   ZSTDDecoder
	index *btree.BTree

	checksums bool

	offset int64

	numFrames int64
	endOffset int64

	o readerOptions

	// TODO: Add simple LRU cache.
	cachedFrame cachedFrame
}

type Reader interface {
	io.ReadSeeker
	io.ReaderAt
}

type ZSTDDecoder interface {
	DecodeAll(input, dst []byte) ([]byte, error)
}

// NewReader returns ZSTD stream reader that can be randomly accessed using uncompressed data offset.
// Ideally, passed io.ReadSeeker should implement io.ReaderAt interface.
func NewReader(rs io.ReadSeeker, decoder ZSTDDecoder, opts ...ROption) (Reader, error) {
	sr := ReaderImpl{
		dec: decoder,
	}

	sr.o.setDefault()
	for _, o := range opts {
		err := o(&sr.o)
		if err != nil {
			return nil, err
		}
	}

	if sr.o.env == nil {
		sr.o.env = &readSeekerEnvImpl{
			rs: rs,
		}
	}

	tree, last, err := sr.indexFooter()
	if err != nil {
		return nil, err
	}

	sr.index = tree
	sr.endOffset = int64(last.DecompOffset) + int64(last.DecompSize)
	sr.numFrames = last.ID + 1

	return &sr, nil
}

// ReadAt implements io.ReaderAt interface to randomly access data.
// This method is goroutine-safe and can be called concurrently ONLY if
// the underlying reader supports io.ReaderAt interface.
func (s *ReaderImpl) ReadAt(p []byte, off int64) (n int, err error) {
	for m := 0; n < len(p) && err == nil; n += m {
		_, m, err = s.read(p[n:], off+int64(n))
	}
	return
}

// Read implements io.Reader interface to randomly access data.
// This method is NOT goroutine-safe and CAN NOT be called
// concurrently since it modifies the underlying offset.
func (s *ReaderImpl) Read(p []byte) (n int, err error) {
	offset, n, err := s.read(p, s.offset)
	if err != nil {
		if errors.Is(err, io.EOF) {
			s.offset = s.endOffset
		}
		return
	}
	s.offset = offset
	return
}

func (s *ReaderImpl) read(dst []byte, off int64) (int64, int, error) {
	if off >= s.endOffset {
		return 0, 0, io.EOF
	}
	if off < 0 {
		return 0, 0, fmt.Errorf("offset before the start of the file: %d", off)
	}

	index := s.GetIndexByDecompOffset(uint64(off))
	if index == nil {
		return 0, 0, fmt.Errorf("failed to get index by offest: %d", off)
	}
	if off < int64(index.DecompOffset) || off > int64(index.DecompOffset)+int64(index.DecompSize) {
		return 0, 0, fmt.Errorf("offset outside of index bounds: %d: min: %d, max: %d",
			off, int64(index.DecompOffset), int64(index.DecompOffset)+int64(index.DecompSize))
	}

	var decompressed []byte

	cachedOffset, cachedData := s.cachedFrame.get()
	if cachedOffset == index.DecompOffset && cachedData != nil {
		// fastpath
		decompressed = cachedData
	} else {
		// slowpath
		src, err := s.o.env.GetFrameByIndex(*index)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to read compressed data at: %d, %w", index.CompOffset, err)
		}

		if len(src) != int(index.CompSize) {
			return 0, 0, fmt.Errorf("compressed size does not match index at: %d: expected: %d, index: %+v",
				off, len(src), index)
		}

		decompressed, err = s.dec.DecodeAll(src, nil)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to decompress data data at: %d, %w", index.CompOffset, err)
		}

		if s.checksums {
			checksum := uint32((xxhash.Sum64(decompressed) << 32) >> 32)
			if index.Checksum != checksum {
				return 0, 0, fmt.Errorf("checksum verification failed at: %d: expected: %d, actual: %d",
					index.CompOffset, index.Checksum, checksum)
			}
		}
		s.cachedFrame.replace(index.DecompOffset, decompressed)
	}

	if len(decompressed) != int(index.DecompSize) {
		return 0, 0, fmt.Errorf("index corruption: len: %d, expected: %d", len(decompressed), int(index.DecompSize))
	}

	offsetWithinFrame := uint64(off) - index.DecompOffset

	size := uint64(len(decompressed)) - offsetWithinFrame
	if size > uint64(len(dst)) {
		size = uint64(len(dst))
	}

	s.o.logger.Debug("decompressed", zap.Uint64("offsetWithinFrame", offsetWithinFrame), zap.Uint64("end", offsetWithinFrame+size),
		zap.Uint64("size", size), zap.Int("lenDecompressed", len(decompressed)), zap.Int("lenDst", len(dst)), zap.Object("index", index))
	copy(dst, decompressed[offsetWithinFrame:offsetWithinFrame+size])

	return off + int64(size), int(size), nil
}

// Seek implements io.Seeker interface to randomly access data.
// This method is NOT goroutine-safe and CAN NOT be called
// concurrently since it modifies the underlying offset.
func (s *ReaderImpl) Seek(offset int64, whence int) (int64, error) {
	newOffset := s.offset
	switch whence {
	case io.SeekCurrent:
		newOffset += offset
	case io.SeekStart:
		newOffset = offset
	case io.SeekEnd:
		newOffset = s.endOffset + offset
	default:
		return 0, fmt.Errorf("unknown whence: %d", whence)
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("offset before the start of the file: %d (%d + %d)",
			newOffset, s.offset, offset)
	}

	s.offset = newOffset
	return s.offset, nil
}

// FrameOffsetEntry is the post-proccessed view of the Seek_Table_Entries suitable for indexing.
type FrameOffsetEntry struct {
	// ID is the is the sequence number of the frame in the index.
	ID int64

	// CompOffset is the offset within compressed stream.
	CompOffset uint64
	// DecompOffset is the offset within decompressed stream.
	DecompOffset uint64
	// CompSize is the size of the compressed frame.
	CompSize uint32
	// DecompSize is the size of the original data.
	DecompSize uint32

	// Checksum is the lower 32 bits of the XXH64 hash of the uncompressed data.
	Checksum uint32
}

func (o *FrameOffsetEntry) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddInt64("ID", o.ID)

	enc.AddUint64("CompOffset", o.CompOffset)
	enc.AddUint64("DecompOffset", o.DecompOffset)
	enc.AddUint32("CompSize", o.CompSize)
	enc.AddUint32("DecompSize", o.DecompSize)
	enc.AddUint32("Checksum", o.Checksum)

	return nil
}

func (o *FrameOffsetEntry) Less(than btree.Item) bool {
	return o.DecompOffset < than.(*FrameOffsetEntry).DecompOffset
}

func (s *ReaderImpl) indexFooter() (*btree.BTree, *FrameOffsetEntry, error) {
	// read SeekTableFooter
	buf, err := s.o.env.ReadFooter()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read footer: %w", err)
	}
	if len(buf) < seekTableFooterOffset {
		return nil, nil, fmt.Errorf("footer is too small: %d", len(buf))
	}

	// parse SeekTableFooter
	footer := SeekTableFooter{}
	err = footer.UnmarshalBinary(buf[len(buf)-seekTableFooterOffset:])
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse footer %+v: %w", buf, err)
	}
	s.o.logger.Debug("loaded", zap.Object("footer", &footer))

	s.checksums = footer.SeekTableDescriptor.ChecksumFlag

	// read SeekTableEntries
	seekTableEntrySize := int64(8)
	if footer.SeekTableDescriptor.ChecksumFlag {
		seekTableEntrySize += 4
	}

	skippableFrameOffset := seekTableFooterOffset + seekTableEntrySize*int64(footer.NumberOfFrames)
	skippableFrameOffset += frameSizeFieldSize
	skippableFrameOffset += skippableMagicNumberFieldSize

	buf, err = s.o.env.ReadSkipFrame(skippableFrameOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read footer: %w", err)
	}

	if len(buf) <= frameSizeFieldSize+skippableMagicNumberFieldSize+seekTableFooterOffset {
		return nil, nil, fmt.Errorf("skip frame is too small: %d", len(buf))
	}

	// parse SeekTableEntries
	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != SkippableFrameMagic+seekableTag {
		return nil, nil, fmt.Errorf("skippable frame magic mismatch %d vs %d",
			magic, SkippableFrameMagic+seekableTag)
	}

	expectedFrameSize := int64(len(buf)) - frameSizeFieldSize - skippableMagicNumberFieldSize
	frameSize := int64(binary.LittleEndian.Uint32(buf[4:8]))
	if frameSize != expectedFrameSize {
		return nil, nil, fmt.Errorf("skippable frame size mismatch: expected: %d, actual: %d",
			expectedFrameSize, frameSize)
	}

	return s.indexSeekTableEntries(buf[8:len(buf)-seekTableFooterOffset], uint64(seekTableEntrySize))
}

func (s *ReaderImpl) indexSeekTableEntries(p []byte, entrySize uint64) (*btree.BTree, *FrameOffsetEntry, error) {
	if len(p) == 0 {
		return nil, nil, fmt.Errorf("seek table is empty")
	}

	if uint64(len(p))%entrySize != 0 {
		return nil, nil, fmt.Errorf("seek table size is not multiple of %d", entrySize)
	}

	// TODO: make fan-out tunable?
	t := btree.New(16)
	entry := SeekTableEntry{}
	var compOffset, decompOffset uint64

	var last *FrameOffsetEntry
	var i int64
	for indexOffset := uint64(0); indexOffset < uint64(len(p)); indexOffset += entrySize {
		err := entry.UnmarshalBinary(p[indexOffset : indexOffset+entrySize])
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse entry %+v at: %d: %w",
				p[indexOffset:indexOffset+entrySize], indexOffset, err)
		}

		last = &FrameOffsetEntry{
			ID:           i,
			CompOffset:   compOffset,
			DecompOffset: decompOffset,
			CompSize:     entry.CompressedSize,
			DecompSize:   entry.DecompressedSize,
			Checksum:     entry.Checksum,
		}
		t.ReplaceOrInsert(last)
		compOffset += uint64(entry.CompressedSize)
		decompOffset += uint64(entry.DecompressedSize)
		i++
	}

	// TODO: empty file should be valid.
	if last == nil {
		return nil, nil, fmt.Errorf("seek index is empty")
	}

	return t, last, nil
}
