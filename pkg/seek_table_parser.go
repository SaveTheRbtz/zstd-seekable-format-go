package seekable

import (
	"encoding/binary"
	"fmt"
)

const skippableFrameHeaderSize = frameSizeFieldSize + skippableMagicNumberFieldSize

func readSeekTable(renv ReaderEnvironment) (SeekTable, error) {
	footerBuf, err := renv.ReadFooter()
	if err != nil {
		return SeekTable{}, fmt.Errorf("failed to read footer: %w", err)
	}

	footer, entrySize, err := parseSeekTableFooter(footerBuf)
	if err != nil {
		return SeekTable{}, err
	}

	frameOffset, err := seekTableFrameOffset(footer, entrySize)
	if err != nil {
		return SeekTable{}, err
	}

	frameBuf, err := renv.ReadSkipFrame(frameOffset)
	if err != nil {
		return SeekTable{}, fmt.Errorf("failed to read seek table frame: %w", err)
	}

	return parseSeekTableFrame(frameBuf)
}

func parseSeekTableFrame(buf []byte) (SeekTable, error) {
	footer, entrySize, err := parseSeekTableFooter(buf)
	if err != nil {
		return SeekTable{}, err
	}
	if _, err := seekTableFrameOffset(footer, entrySize); err != nil {
		return SeekTable{}, err
	}

	if len(buf) < skippableFrameHeaderSize+seekTableFooterOffset {
		return SeekTable{}, fmt.Errorf("skip frame is too small: %d", len(buf))
	}

	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != skippableFrameMagic+seekableTag {
		return SeekTable{}, fmt.Errorf("skippable frame magic mismatch %d vs %d",
			magic, skippableFrameMagic+seekableTag)
	}

	expectedFrameSize := int64(len(buf)) - skippableFrameHeaderSize
	frameSize := int64(binary.LittleEndian.Uint32(buf[4:8]))
	if frameSize != expectedFrameSize {
		return SeekTable{}, fmt.Errorf("skippable frame size mismatch: expected: %d, actual: %d",
			expectedFrameSize, frameSize)
	}

	if frameSize > int64(maxDecoderFrameSize) {
		return SeekTable{}, fmt.Errorf("frame is too big: %d > %d", frameSize, maxDecoderFrameSize)
	}

	entries, err := parseSeekTableEntries(
		buf[skippableFrameHeaderSize:len(buf)-seekTableFooterOffset],
		uint64(entrySize),
		footer.NumberOfFrames,
	)
	if err != nil {
		return SeekTable{}, err
	}

	return SeekTable{
		entries:   entries,
		checksums: footer.SeekTableDescriptor.ChecksumFlag,
	}, nil
}

func parseSeekTableFooter(buf []byte) (seekTableFooter, int64, error) {
	if len(buf) < seekTableFooterOffset {
		return seekTableFooter{}, 0, fmt.Errorf("footer is too small: %d", len(buf))
	}

	footer := seekTableFooter{}
	footerBuf := buf[len(buf)-seekTableFooterOffset:]
	if err := footer.UnmarshalBinary(footerBuf); err != nil {
		return seekTableFooter{}, 0, fmt.Errorf("failed to parse footer: input len=%d footer excerpt=%+v: %w",
			len(buf), footerBuf, err)
	}

	return footer, seekTableEntrySize(footer.SeekTableDescriptor.ChecksumFlag), nil
}

func seekTableFrameOffset(footer seekTableFooter, entrySize int64) (int64, error) {
	frameOffset := seekTableFooterOffset + entrySize*int64(footer.NumberOfFrames)
	frameOffset += skippableFrameHeaderSize

	if frameOffset > int64(maxDecoderFrameSize) {
		return 0, fmt.Errorf("frame offset is too big: %d > %d",
			frameOffset, maxDecoderFrameSize)
	}
	return frameOffset, nil
}

func seekTableEntrySize(checksum bool) int64 {
	const (
		baseSize     = 8
		checksumSize = 4
	)
	if checksum {
		return baseSize + checksumSize
	}
	return baseSize
}

func parseSeekTableEntries(p []byte, entrySize uint64, numberOfFrames uint32) ([]FrameOffsetEntry, error) {
	if entrySize == 0 {
		return nil, fmt.Errorf("seek table entry size is 0")
	}
	if uint64(len(p))%entrySize != 0 {
		return nil, fmt.Errorf("seek table size is not multiple of %d", entrySize)
	}
	parsedEntries := uint64(len(p)) / entrySize
	if parsedEntries != uint64(numberOfFrames) {
		return nil, fmt.Errorf("seek table entry count mismatch: parsed %d, footer %d",
			parsedEntries, numberOfFrames)
	}

	entries := make([]FrameOffsetEntry, 0, int(parsedEntries))
	entry := seekTableEntry{}
	var compressedOffset, decompressedOffset uint64

	var i int64
	for indexOffset := uint64(0); indexOffset < uint64(len(p)); indexOffset += entrySize {
		err := entry.UnmarshalBinary(p[indexOffset : indexOffset+entrySize])
		if err != nil {
			return nil, fmt.Errorf("failed to parse entry %+v at: %d: %w",
				p[indexOffset:indexOffset+entrySize], indexOffset, err)
		}

		entries = append(entries, FrameOffsetEntry{
			ID:                 i,
			CompressedOffset:   compressedOffset,
			DecompressedOffset: decompressedOffset,
			CompressedSize:     entry.CompressedSize,
			DecompressedSize:   entry.DecompressedSize,
			Checksum:           entry.Checksum,
		})
		compressedOffset += uint64(entry.CompressedSize)
		decompressedOffset += uint64(entry.DecompressedSize)
		i++
	}

	return entries, nil
}
