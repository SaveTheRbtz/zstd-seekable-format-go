package seekable

import (
	"fmt"

	"encoding/binary"
)

const (
	skippableFrameMagic = 0x184D2A50
	seekableMagicNumber = 0x8F92EAB1

	seekTableFooterOffset = 9

	seekableTag = 0xE
)

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
