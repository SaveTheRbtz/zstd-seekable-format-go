package seekable

import (
	"encoding/binary"
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
	_ io.ReadSeekCloser = (*seekableReaderImpl)(nil)
	_ io.ReaderAt       = (*seekableReaderImpl)(nil)
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

type seekableReaderImpl struct {
	rs    io.ReadSeeker
	dec   *zstd.Decoder
	index *btree.BTree

	checksums bool

	offset    int64
	endOffset int64

	o readerOptions

	// TODO: add simple LRU cache ontop
	cachedFrame cachedFrame
}

type SeekableZSTDReader interface {
	io.ReadSeekCloser
	io.ReaderAt
}

func NewReader(rs io.ReadSeeker, opts ...ROption) (SeekableZSTDReader, error) {
	sr := seekableReaderImpl{
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

	tree, err := sr.readFooter()
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

func (s *seekableReaderImpl) ReadAt(p []byte, off int64) (n int, err error) {
	_, n, err = s.read(p, off)
	return
}

func (s *seekableReaderImpl) Read(p []byte) (n int, err error) {
	offset, n, err := s.read(p, s.offset)
	if err != nil {
		return
	}
	s.offset = offset
	return
}

func (s *seekableReaderImpl) read(dst []byte, off int64) (int64, int, error) {
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

func (s *seekableReaderImpl) readSegment(p []byte, off int64) (err error) {
	switch v := s.rs.(type) {
	case io.ReaderAt:
		n, err := v.ReadAt(p, off)
		if err == io.EOF {
			if n == len(p) {
				return nil
			}
		}
		return err
	default:
		_, err = v.Seek(int64(off), io.SeekStart)
		if err != nil {
			return err
		}
		_, err = io.ReadFull(s.rs, p)
		return err
	}
}

func (s *seekableReaderImpl) Seek(offset int64, whence int) (int64, error) {
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

func (s *seekableReaderImpl) Close() (err error) {
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

func (s *seekableReaderImpl) readFooter() (t *btree.BTree, err error) {
	_, err = s.rs.Seek(-seekTableFooterOffset, io.SeekEnd)
	if err != nil {
		return
	}

	buf := make([]byte, seekTableFooterOffset)
	_, err = io.ReadFull(s.rs, buf)
	if err != nil {
		return
	}

	footer := SeekTableFooter{}
	err = footer.UnmarshalBinary(buf)
	if err != nil {
		return
	}

	s.checksums = footer.SeekTableDescriptor.ChecksumFlag

	seekTableEntrySize := int64(8)
	if footer.SeekTableDescriptor.ChecksumFlag {
		seekTableEntrySize += 4
	}

	skippableFrameOffset := seekTableFooterOffset + seekTableEntrySize*int64(footer.NumberOfFrames)
	// Frame_Size
	skippableFrameOffset += 4
	// Skippable_Magic_Number
	skippableFrameOffset += 4

	_, err = s.rs.Seek(-skippableFrameOffset, io.SeekEnd)
	if err != nil {
		return
	}

	buf = make([]byte, skippableFrameOffset-seekTableFooterOffset)
	_, err = io.ReadFull(s.rs, buf)
	if err != nil {
		return
	}

	magic := binary.LittleEndian.Uint32(buf[0:])
	if magic != skippableFrameMagic+seekableTag {
		return nil, fmt.Errorf("skippable frame magic mismatch %d vs %d",
			magic, skippableFrameMagic+seekableTag)
	}
	frameSize := int64(binary.LittleEndian.Uint32(buf[4:]))
	if frameSize != skippableFrameOffset-8 {
		return nil, fmt.Errorf("skippable frame size mismatch %d vs %d",
			frameSize, skippableFrameOffset-8)
	}

	t, err = s.indexSeekTableEntries(buf[8:], uint64(seekTableEntrySize))
	return
}

func (s *seekableReaderImpl) indexSeekTableEntries(p []byte, entrySize uint64) (*btree.BTree, error) {
	// TODO: rewrite btree using generics
	t := btree.New(16)
	entry := SeekTableEntry{}
	var indexOffset, compOffset, decompOffset uint64
	for {
		if indexOffset >= uint64(len(p)) {
			break
		}
		err := entry.UnmarshalBinary(p[indexOffset : indexOffset+entrySize])
		if err != nil {
			return nil, err
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
		indexOffset += entrySize
	}
	return t, nil
}
