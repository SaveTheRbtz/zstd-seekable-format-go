package seekable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/google/btree"
)

var (
	_ io.ReadSeekCloser = (*ReaderImpl)(nil)
	_ io.ReaderAt       = (*ReaderImpl)(nil)
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

type ReaderImpl struct {
	rs    io.ReadSeeker
	dec   *zstd.Decoder
	index *btree.BTree

	checksums bool

	offset    int64
	endOffset int64

	o readerOptions

	// TODO: Add simple LRU cache.
	cachedFrame cachedFrame
}

type ZSTDReader interface {
	io.ReadSeekCloser
	io.ReaderAt
}

// NewReader returns ZSTD stream reader that can be randomly-accessible using uncompressed data offset.
// Ideally, passed io.ReadSeeker should implement io.ReaderAt interface.
func NewReader(rs io.ReadSeeker, opts ...ROption) (ZSTDReader, error) {
	sr := ReaderImpl{
		rs: rs,
	}

	sr.o.setDefault()
	for _, o := range opts {
		err := o(&sr.o)
		if err != nil {
			return nil, err
		}
	}

	var err error
	sr.dec, err = zstd.NewReader(nil, sr.o.zstdDOpts...)
	if err != nil {
		return nil, err
	}

	tree, err := sr.indexFooter()
	if err != nil {
		return nil, err
	}
	sr.index = tree

	sr.index.Descend(func(i btree.Item) bool {
		last := i.(frameOffset)
		sr.endOffset = int64(last.decompOffset) + int64(last.decompSize)
		return false
	})

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

	var index frameOffset
	s.index.DescendLessOrEqual(frameOffset{decompOffset: uint64(off)}, func(i btree.Item) bool {
		index = i.(frameOffset)
		return false
	})

	if off < int64(index.decompOffset) || off > int64(index.decompOffset)+int64(index.decompSize) {
		return 0, 0, fmt.Errorf("offset outside of index bounds: %d: min: %d, max: %d",
			off, int64(index.decompOffset), int64(index.decompOffset)+int64(index.decompSize))
	}

	var err error
	var decompressed []byte

	cachedOffset, cachedData := s.cachedFrame.get()
	if cachedOffset == index.decompOffset && cachedData != nil {
		// fastpath
		decompressed = cachedData
		if len(decompressed) != int(index.decompSize) {
			panic(fmt.Sprintf("cache corruption: len: %d, expected: %d",
				len(decompressed), int(index.decompSize)))
		}
	} else {
		// slowpath
		src := make([]byte, index.compSize)
		err = s.readSegment(src, int64(index.compOffset))
		if err != nil {
			return 0, 0, fmt.Errorf("failed to read compressed data at: %d, %w", index.compOffset, err)
		}

		decompressed, err = s.dec.DecodeAll(src, nil)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to decompress data data at: %d, %w", index.compOffset, err)
		}

		if s.checksums {
			checksum := uint32((xxhash.Sum64(decompressed) << 32) >> 32)
			if index.checksum != checksum {
				return 0, 0, fmt.Errorf("checksum verification failed at: %d: expected: %d, actual: %d",
					index.compOffset, index.checksum, checksum)
			}
		}
		s.cachedFrame.replace(index.decompOffset, decompressed)
	}

	offsetWithinFrame := uint64(off) - index.decompOffset

	size := uint64(len(decompressed)) - offsetWithinFrame
	if size > uint64(len(dst)) {
		size = uint64(len(dst))
	}

	s.o.logger.Debug("decompressed", zap.Uint64("offsetWithinFrame", offsetWithinFrame), zap.Uint64("end", offsetWithinFrame+size),
		zap.Uint64("size", size), zap.Int("lenDecompressed", len(decompressed)), zap.Int("lenDst", len(dst)), zap.Object("index", &index))
	copy(dst, decompressed[offsetWithinFrame:offsetWithinFrame+size])

	return off + int64(size), int(size), nil
}

func (s *ReaderImpl) readSegment(p []byte, off int64) (err error) {
	switch v := s.rs.(type) {
	case io.ReaderAt:
		n, err := v.ReadAt(p, off)
		if errors.Is(err, io.EOF) {
			if n == len(p) {
				return nil
			}
		}
		return err
	default:
		_, err = v.Seek(off, io.SeekStart)
		if err != nil {
			return err
		}
		_, err = io.ReadFull(s.rs, p)
		return err
	}
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
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("offset before the start of the file: %d (%d + %d)",
			newOffset, s.offset, offset)
	}

	s.offset = newOffset
	return s.offset, nil
}

// Close implement io.Closer interface.  Calling Close releases occupied memory.
//
// Caller is still responsible to Close the underlying reader.
func (s *ReaderImpl) Close() (err error) {
	s.index.Clear(false)
	s.dec.Close()

	s.cachedFrame.replace(0, nil)
	return
}

type frameOffset struct {
	compOffset   uint64
	decompOffset uint64
	compSize     uint32
	decompSize   uint32

	checksum uint32
}

func (o *frameOffset) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddUint64("compOffset", o.compOffset)
	enc.AddUint64("decompOffset", o.decompOffset)
	enc.AddUint32("compSize", o.compSize)
	enc.AddUint32("decompSize", o.decompSize)
	enc.AddUint32("checksum", o.checksum)
	return nil
}

func (o frameOffset) Less(than btree.Item) bool {
	return o.decompOffset < than.(frameOffset).decompOffset
}

func (s *ReaderImpl) readFooter() ([]byte, error) {
	n, err := s.rs.Seek(-seekTableFooterOffset, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to: %d: %w", -seekTableFooterOffset, err)
	}

	s.o.logger.Debug("loading footer", zap.Int64("offset", n))
	buf := make([]byte, seekTableFooterOffset)
	_, err = io.ReadFull(s.rs, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read footer at: %d: %w", n, err)
	}

	return buf, nil
}

func (s *ReaderImpl) readSkipFrame(seekTableEntrySize, numberOfFrames int64) ([]byte, error) {
	skippableFrameOffset := seekTableFooterOffset + seekTableEntrySize*numberOfFrames
	skippableFrameOffset += frameSizeFieldSize
	skippableFrameOffset += skippableMagicNumberFieldSize

	n, err := s.rs.Seek(-skippableFrameOffset, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to: %d: %w", -skippableFrameOffset, err)
	}

	buf := make([]byte, skippableFrameOffset)
	_, err = io.ReadFull(s.rs, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read skippable frame header at: %d: %w", n, err)
	}
	return buf, nil
}

func (s *ReaderImpl) indexFooter() (*btree.BTree, error) {
	// read SeekTableFooter
	buf, err := s.readFooter()
	if err != nil {
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}

	// parse SeekTableFooter
	footer := SeekTableFooter{}
	err = footer.UnmarshalBinary(buf[len(buf)-seekTableFooterOffset:])
	if err != nil {
		return nil, fmt.Errorf("failed to parse footer %+v: %w", buf, err)
	}
	s.o.logger.Debug("loaded", zap.Object("footer", &footer))

	s.checksums = footer.SeekTableDescriptor.ChecksumFlag

	// read SeekTableEntries
	seekTableEntrySize := int64(8)
	if footer.SeekTableDescriptor.ChecksumFlag {
		seekTableEntrySize += 4
	}

	buf, err = s.readSkipFrame(seekTableEntrySize, int64(footer.NumberOfFrames))
	if err != nil {
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}

	// parse SeekTableEntries
	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != SkippableFrameMagic+seekableTag {
		return nil, fmt.Errorf("skippable frame magic mismatch %d vs %d",
			magic, SkippableFrameMagic+seekableTag)
	}

	expectedFrameSize := int64(len(buf)) - frameSizeFieldSize - skippableMagicNumberFieldSize
	frameSize := int64(binary.LittleEndian.Uint32(buf[4:8]))
	if frameSize != expectedFrameSize {
		return nil, fmt.Errorf("skippable frame size mismatch: expected: %d, actual: %d",
			expectedFrameSize, frameSize)
	}

	t, err := s.indexSeekTableEntries(buf[8:len(buf)-seekTableFooterOffset], uint64(seekTableEntrySize))
	return t, err
}

func (s *ReaderImpl) indexSeekTableEntries(p []byte, entrySize uint64) (*btree.BTree, error) {
	// TODO: Rewrite btree using generics.
	t := btree.New(16)
	entry := SeekTableEntry{}
	var compOffset, decompOffset uint64

	for indexOffset := uint64(0); indexOffset < uint64(len(p)); indexOffset += entrySize {
		err := entry.UnmarshalBinary(p[indexOffset : indexOffset+entrySize])
		if err != nil {
			return nil, fmt.Errorf("failed to parse entry %+v at: %d: %w",
				p[indexOffset:indexOffset+entrySize], indexOffset, err)
		}

		t.ReplaceOrInsert(frameOffset{
			compOffset:   compOffset,
			decompOffset: decompOffset,
			compSize:     entry.CompressedSize,
			decompSize:   entry.DecompressedSize,
			checksum:     entry.Checksum,
		})
		compOffset += uint64(entry.CompressedSize)
		decompOffset += uint64(entry.DecompressedSize)
	}

	return t, nil
}
