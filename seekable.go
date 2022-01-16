package seekable

import (
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/klauspost/compress/zstd"

	"encoding/binary"

	"github.com/google/btree"
	"go.uber.org/multierr"
)

const (
	skippableFrameMagic = 0x184D2A50
	seekableMagicNumber = 0x8F92EAB1

	seekTableFooterOffset = 9

	seekableTag = 0xE
)

type seekableWriterImpl struct {
	w            io.WriteCloser
	enc          *zstd.Encoder
	frameEntries []seekTableEntry

	once *sync.Once
}

func NewWriter(w io.WriteCloser, opts ...zstd.EOption) (io.WriteCloser, error) {
	enc, err := zstd.NewWriter(nil, opts...)
	if err != nil {
		return nil, err
	}
	sw := seekableWriterImpl{
		w:    w,
		enc:  enc,
		once: &sync.Once{},
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

	s.frameEntries = append(s.frameEntries, seekTableEntry{
		CompressedSize:   uint32(len(dst)),
		DecompressedSize: uint32(len(src)),
		Checksum:         uint32((xxhash.Sum64(src) << 32) >> 32),
	})
	return s.w.Write(dst)
}

func (s *seekableWriterImpl) Close() (err error) {
	s.once.Do(func() {
		err = multierr.Append(err, s.writeSeekTable())
	})

	s.frameEntries = nil
	err = multierr.Append(err, s.enc.Close())
	err = multierr.Append(err, s.w.Close())
	return
}

func (s *seekableWriterImpl) writeSeekTable() error {
	// TODO: preallocate
	seekTable := make([]byte, 0)
	for _, e := range s.frameEntries {
		entryBytes, err := e.MarshalBinary()
		if err != nil {
			return err
		}
		seekTable = append(seekTable, entryBytes...)
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

	footerBytes, err := footer.MarshalBinary()
	if err != nil {
		return err
	}
	seekTable = append(seekTable, footerBytes...)

	seekTableBytes, err := createSkippableFrame(seekableTag, seekTable)
	if err != nil {
		return err
	}

	_, err = s.w.Write(seekTableBytes)
	if err != nil {
		return err
	}

	return nil
}

/*
A bitfield describing the format of the seek table.

| Bit number | Field name                |
| ---------- | ----------                |
| 7          | `Checksum_Flag`           |
| 6-2        | `Reserved_Bits`           |
| 1-0        | `Unused_Bits`             |

While only `Checksum_Flag` currently exists, there are 7 other bits in this field that can be used for future changes to the format,
for example the addition of inline dictionaries.
*/
type SeekTableDescriptor struct {
	// If the checksum flag is set, each of the seek table entries contains a 4 byte checksum
	// of the uncompressed data contained in its frame.
	ChecksumFlag bool
}

/*
The seek table footer format is as follows:

|`Number_Of_Frames`|`Seek_Table_Descriptor`|`Seekable_Magic_Number`|
|------------------|-----------------------|-----------------------|
| 4 bytes          | 1 byte                | 4 bytes               |
*/
type seekTableFooter struct {
	// The number of stored frames in the data.
	NumberOfFrames uint32
	// A bitfield describing the format of the seek table.
	SeekTableDescriptor SeekTableDescriptor
	// Value : 0x8F92EAB1.
	SeekableMagicNumber uint32
}

func (f *seekTableFooter) marshalBinaryInline(dst []byte) {
	binary.LittleEndian.PutUint32(dst[0:], f.NumberOfFrames)
	if f.SeekTableDescriptor.ChecksumFlag {
		dst[4] |= 1 << 7
	}
	binary.LittleEndian.PutUint32(dst[5:], seekableMagicNumber)
}

func (f *seekTableFooter) MarshalBinary() ([]byte, error) {
	dst := make([]byte, seekTableFooterOffset)
	f.marshalBinaryInline(dst)
	return dst, nil
}

func (f *seekTableFooter) UnmarshalBinary(p []byte) error {
	if len(p) != seekTableFooterOffset {
		return fmt.Errorf("footer length mismatch %d vs %d", len(p), seekTableFooterOffset)
	}
	f.NumberOfFrames = binary.LittleEndian.Uint32(p[0:])
	f.SeekTableDescriptor.ChecksumFlag = (p[4] & (1 << 7)) > 0
	f.SeekableMagicNumber = binary.LittleEndian.Uint32(p[5:])
	if f.SeekableMagicNumber != seekableMagicNumber {
		return fmt.Errorf("footer magic mismatch %d vs %d", f.SeekableMagicNumber, seekableMagicNumber)
	}
	return nil
}

/*
`Seek_Table_Entries` consists of `Number_Of_Frames` (one for each frame in the data, not including the seek table frame) entries of the following form, in sequence:

|`Compressed_Size`|`Decompressed_Size`|`[Checksum]`|
|-----------------|-------------------|------------|
| 4 bytes         | 4 bytes           | 4 bytes    |
*/
type seekTableEntry struct {
	// The compressed size of the frame.
	// The cumulative sum of the `Compressed_Size` fields of frames `0` to `i` gives the offset in the compressed file of frame `i+1`.
	CompressedSize uint32
	// The size of the decompressed data contained in the frame.  For skippable or otherwise empty frames, this value is 0.
	DecompressedSize uint32
	// Only present if `Checksum_Flag` is set in the `Seek_Table_Descriptor`.  Value : the least significant 32 bits of the XXH64 digest of the uncompressed data, stored in little-endian format.
	Checksum uint32
}

func (e *seekTableEntry) marshalBinaryInline(dst []byte) {
	binary.LittleEndian.PutUint32(dst[0:], e.CompressedSize)
	binary.LittleEndian.PutUint32(dst[4:], e.DecompressedSize)
	binary.LittleEndian.PutUint32(dst[8:], e.Checksum)
}

func (e *seekTableEntry) MarshalBinary() ([]byte, error) {
	dst := make([]byte, 12)
	e.marshalBinaryInline(dst)
	return dst, nil
}

func (e *seekTableEntry) UnmarshalBinary(p []byte) error {
	if len(p) < 8 {
		return fmt.Errorf("entry length mismatch %d vs %d", len(p), 8)
	}
	e.CompressedSize = binary.LittleEndian.Uint32(p[0:])
	e.DecompressedSize = binary.LittleEndian.Uint32(p[4:])
	if len(p) >= 12 {
		e.Checksum = binary.LittleEndian.Uint32(p[8:])
	}
	return nil
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

	dst := make([]byte, 8)
	binary.LittleEndian.PutUint32(dst[0:], skippableFrameMagic+tag)
	binary.LittleEndian.PutUint32(dst[4:], uint32(len(payload)))
	return append(dst, payload...), nil
}

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
	rsc   io.ReadSeekCloser
	dec   *zstd.Decoder
	index *btree.BTree

	checksums bool

	offset    int64
	endOffset int64

	cachedFrame *cachedFrame
}

func NewReader(rsc io.ReadSeekCloser, opts ...zstd.DOption) (io.ReadSeekCloser, error) {
	dec, err := zstd.NewReader(nil, opts...)
	if err != nil {
		return nil, err
	}

	sr := seekableReaderImpl{
		rsc: rsc,
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
	if s.cachedFrame != nil && s.cachedFrame.offset == index.compOffset {
		// fastpath
		decompressed = s.cachedFrame.data
	} else {
		// slowpath
		src := make([]byte, index.compSize)
		s.rsc.Seek(int64(index.compOffset), io.SeekStart)
		_, err = io.ReadFull(s.rsc, src)
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
		s.cachedFrame = newCachedFrame(index.compOffset, decompressed)
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
	switch whence {
	case io.SeekCurrent:
		s.offset += offset
	case io.SeekStart:
		s.offset = offset
	case io.SeekEnd:
		s.offset = s.endOffset + offset
	}
	return 0, nil
}

func (s *seekableReaderImpl) Close() (err error) {
	s.index.Clear(false)
	s.dec.Close()

	s.cachedFrame = nil
	err = multierr.Append(err, s.rsc.Close())
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
	_, err = s.rsc.Seek(-seekTableFooterOffset, io.SeekEnd)
	if err != nil {
		return
	}

	buf := make([]byte, seekTableFooterOffset)
	_, err = io.ReadFull(s.rsc, buf)
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

	_, err = s.rsc.Seek(-skippableFrameOffset, io.SeekEnd)
	if err != nil {
		return
	}

	buf = make([]byte, skippableFrameOffset-seekTableFooterOffset)
	_, err = io.ReadFull(s.rsc, buf)
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
