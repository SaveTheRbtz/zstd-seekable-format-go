package seekable

/*
## Format

The format consists of a number of frames (Zstandard compressed frames and skippable frames), followed by a final skippable frame at the end containing the seek table.

### Seek Table Format
The structure of the seek table frame is as follows:

|`Skippable_Magic_Number`|`Frame_Size`|`[Seek_Table_Entries]`|`Seek_Table_Footer`|
|------------------------|------------|----------------------|-------------------|
| 4 bytes                | 4 bytes    | 8-12 bytes each      | 9 bytes           |

__`Skippable_Magic_Number`__

Value : 0x184D2A5E.
This is for compatibility with [Zstandard skippable frames].
Since it is legal for other Zstandard skippable frames to use the same
magic number, it is not recommended for a decoder to recognize frames
solely on this.

__`Frame_Size`__

The total size of the skippable frame, not including the `Skippable_Magic_Number` or `Frame_Size`.
This is for compatibility with [Zstandard skippable frames].

[Zstandard skippable frames]: https://github.com/facebook/zstd/blob/release/doc/zstd_compression_format.md#skippable-frames

https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md
*/

import (
	"encoding/binary"
	"fmt"
	"math"

	"go.uber.org/zap/zapcore"
)

const (
	skippableFrameMagic = 0x184D2A50
	seekableMagicNumber = 0x8F92EAB1

	seekTableFooterOffset = 9

	seekableTag = 0xE
)

/*
SeekTableDescriptor is a Go representation of a bitfiled.

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
SeekTableFooter is the footer of a seekable ZSTD stream.

The seek table footer format is as follows:

|`Number_Of_Frames`|`Seek_Table_Descriptor`|`Seekable_Magic_Number`|
|------------------|-----------------------|-----------------------|
| 4 bytes          | 1 byte                | 4 bytes               |

https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md#seek_table_footer
*/
type SeekTableFooter struct {
	// The number of stored frames in the data.
	NumberOfFrames uint32
	// A bitfield describing the format of the seek table.
	SeekTableDescriptor SeekTableDescriptor
	// Value : 0x8F92EAB1.
	SeekableMagicNumber uint32
}

func (f *SeekTableFooter) marshalBinaryInline(dst []byte) {
	binary.LittleEndian.PutUint32(dst[0:], f.NumberOfFrames)
	if f.SeekTableDescriptor.ChecksumFlag {
		dst[4] |= 1 << 7
	}
	binary.LittleEndian.PutUint32(dst[5:], seekableMagicNumber)
}

func (f *SeekTableFooter) MarshalBinary() ([]byte, error) {
	dst := make([]byte, seekTableFooterOffset)
	f.marshalBinaryInline(dst)
	return dst, nil
}

func (f *SeekTableFooter) UnmarshalBinary(p []byte) error {
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
SeekTableEntry is an element of the Seek Table describing each of the ZSTD-compressed frames in the stream.

`Seek_Table_Entries` consists of `Number_Of_Frames` (one for each frame in the data, not including the seek table frame) entries of the following form, in sequence:

|`Compressed_Size`|`Decompressed_Size`|`[Checksum]`|
|-----------------|-------------------|------------|
| 4 bytes         | 4 bytes           | 4 bytes    |

https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md#seek_table_entries
*/
type SeekTableEntry struct {
	// The compressed size of the frame.
	// The cumulative sum of the `Compressed_Size` fields of frames `0` to `i` gives the offset in the compressed file of frame `i+1`.
	CompressedSize uint32
	// The size of the decompressed data contained in the frame.  For skippable or otherwise empty frames, this value is 0.
	DecompressedSize uint32
	// Only present if `Checksum_Flag` is set in the `Seek_Table_Descriptor`.  Value : the least significant 32 bits of the XXH64 digest of the uncompressed data, stored in little-endian format.
	Checksum uint32
}

func (e *SeekTableEntry) marshalBinaryInline(dst []byte) {
	binary.LittleEndian.PutUint32(dst[0:], e.CompressedSize)
	binary.LittleEndian.PutUint32(dst[4:], e.DecompressedSize)
	binary.LittleEndian.PutUint32(dst[8:], e.Checksum)
}

func (e *SeekTableEntry) MarshalBinary() ([]byte, error) {
	dst := make([]byte, 12)
	e.marshalBinaryInline(dst)
	return dst, nil
}

func (e *SeekTableEntry) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddUint32("CompressedSize", e.CompressedSize)
	enc.AddUint32("DecompressedSize", e.DecompressedSize)
	enc.AddUint32("Checksum", e.Checksum)
	return nil
}

func (e *SeekTableEntry) UnmarshalBinary(p []byte) error {
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

/*
CreateSkippableFrame returns a payload formatted as a ZSDT skippable frame.

| `Magic_Number` | `Frame_Size` | `User_Data` |
|:--------------:|:------------:|:-----------:|
|   4 bytes      |  4 bytes     |   n bytes   |

Skippable frames allow the insertion of user-defined metadata
into a flow of concatenated frames.

__`Magic_Number`__

4 Bytes, __little-endian__ format.
Value : 0x184D2A5?, which means any value from 0x184D2A50 to 0x184D2A5F.
All 16 values are valid to identify a skippable frame.
This specification doesn't detail any specific tagging for skippable frames.

__`Frame_Size`__

This is the size, in bytes, of the following `User_Data`
(without including the magic number nor the size field itself).
This field is represented using 4 Bytes, __little-endian__ format, unsigned 32-bits.
This means `User_Data` can’t be bigger than (2^32-1) bytes.

__`User_Data`__

The `User_Data` can be anything. Data will just be skipped by the decoder.

https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md#skippable-frames
*/
func CreateSkippableFrame(tag uint32, payload []byte) ([]byte, error) {
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
