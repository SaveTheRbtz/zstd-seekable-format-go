package seekable

import (
	"fmt"
	"io"

	"github.com/cespare/xxhash"
	"github.com/klauspost/compress/zstd"

	"encoding/binary"

	"github.com/google/btree"
)

type cachedFrame struct {
	offset uint64
	data   []byte
}

func newCachedFrame(offset uint64, data []byte) *cachedFrame {
	return &cachedFrame{
		offset: offset,
		data:   data,
	}
}

type seekableReaderImpl struct {
	rs    io.ReadSeeker
	dec   *zstd.Decoder
	index *btree.BTree

	checksums bool

	offset    int64
	endOffset int64

	cachedFrame *cachedFrame
}

func NewReader(rs io.ReadSeeker, opts ...zstd.DOption) (io.ReadSeekCloser, error) {
	dec, err := zstd.NewReader(nil, opts...)
	if err != nil {
		return nil, err
	}

	sr := seekableReaderImpl{
		rs:  rs,
		dec: dec,
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

func (s *seekableReaderImpl) Read(dst []byte) (int, error) {
	if s.offset >= s.endOffset {
		return 0, io.EOF
	}

	var index frameOffset
	s.index.DescendLessOrEqual(frameOffset{decompOffset: uint64(s.offset)}, func(i btree.Item) bool {
		index = i.(frameOffset)
		return false
	})

	var err error
	var decompressed []byte
	if s.cachedFrame != nil && s.cachedFrame.offset == index.decompOffset {
		// fastpath
		decompressed = s.cachedFrame.data
	} else {
		// slowpath
		src := make([]byte, index.compSize)
		s.rs.Seek(int64(index.compOffset), io.SeekStart)
		_, err = io.ReadFull(s.rs, src)
		if err != nil {
			return 0, fmt.Errorf("failed to read compressed data at: %d, %w", index.compOffset, err)
		}

		decompressed, err = s.dec.DecodeAll(src, nil)
		if err != nil {
			return 0, fmt.Errorf("failed to decompress data data at: %d, %w", index.compOffset, err)
		}

		if s.checksums {
			checksum := uint32((xxhash.Sum64(decompressed) << 32) >> 32)
			if index.checksum != checksum {
				return 0, fmt.Errorf("checksum verification failed at: %d: expected: %d, actual: %d",
					index.compOffset, index.checksum, checksum)
			}
		}
		s.cachedFrame = newCachedFrame(index.decompOffset, decompressed)
	}

	offsetWithinFrame := uint64(s.offset) - index.decompOffset

	size := uint64(len(decompressed)) - offsetWithinFrame
	if size > uint64(len(dst)) {
		size = uint64(len(dst))
	}

	// TODO: add logger
	//fmt.Fprintf(os.Stderr, "decompressed [%d:%d] size: %d, decom: %d, dst: %d, index: %+v\n",
	//	offsetWithinFrame, offsetWithinFrame+size, size, len(decompressed), len(dst), index)
	copy(dst, decompressed[offsetWithinFrame:offsetWithinFrame+size])

	s.offset += int64(size)
	return int(size), nil
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

	s.cachedFrame = nil
	return
}

type frameOffset struct {
	compOffset   uint64
	decompOffset uint64
	compSize     uint32
	decompSize   uint32

	checksum uint32
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

	footer := seekTableFooter{}
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

	t = s.indexSeekTableEntries(buf[8:], uint64(seekTableEntrySize))
	return
}

func (s *seekableReaderImpl) indexSeekTableEntries(p []byte, entrySize uint64) *btree.BTree {
	// TODO: rewrite btree using generics
	t := btree.New(16)
	entry := seekTableEntry{}
	var indexOffset, compOffset, decompOffset uint64
	for {
		if indexOffset >= uint64(len(p)) {
			break
		}
		entry.UnmarshalBinary(p[indexOffset : indexOffset+entrySize])
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
	return t
}
