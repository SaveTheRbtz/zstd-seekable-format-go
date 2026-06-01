package seekable

import (
	"encoding/binary"
	"fmt"
)

const skippableFrameHeaderSize = frameSizeFieldSize + skippableMagicNumberFieldSize

func readSeekTable(renv REnvironment) (seekTable, error) {
	footerBuf, err := renv.ReadFooter()
	if err != nil {
		return seekTable{}, fmt.Errorf("failed to read footer: %w", err)
	}

	footer, entrySize, err := parseSeekTableFooter(footerBuf)
	if err != nil {
		return seekTable{}, err
	}

	frameOffset, err := seekTableFrameOffset(footer, entrySize)
	if err != nil {
		return seekTable{}, err
	}

	frameBuf, err := renv.ReadSkipFrame(frameOffset)
	if err != nil {
		return seekTable{}, fmt.Errorf("failed to read seek table frame: %w", err)
	}

	return parseSeekTableFrame(frameBuf)
}

func parseSeekTableFrame(buf []byte) (seekTable, error) {
	footer, entrySize, err := parseSeekTableFooter(buf)
	if err != nil {
		return seekTable{}, err
	}
	if _, err := seekTableFrameOffset(footer, entrySize); err != nil {
		return seekTable{}, err
	}

	if len(buf) < skippableFrameHeaderSize+seekTableFooterOffset {
		return seekTable{}, fmt.Errorf("skip frame is too small: %d", len(buf))
	}

	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != skippableFrameMagic+seekableTag {
		return seekTable{}, fmt.Errorf("skippable frame magic mismatch %d vs %d",
			magic, skippableFrameMagic+seekableTag)
	}

	expectedFrameSize := int64(len(buf)) - skippableFrameHeaderSize
	frameSize := int64(binary.LittleEndian.Uint32(buf[4:8]))
	if frameSize != expectedFrameSize {
		return seekTable{}, fmt.Errorf("skippable frame size mismatch: expected: %d, actual: %d",
			expectedFrameSize, frameSize)
	}

	if frameSize > maxDecoderFrameSize {
		return seekTable{}, fmt.Errorf("frame is too big: %d > %d", frameSize, maxDecoderFrameSize)
	}

	entries, err := parseSeekTableEntries(
		buf[skippableFrameHeaderSize:len(buf)-seekTableFooterOffset],
		uint64(entrySize),
		footer.NumberOfFrames,
	)
	if err != nil {
		return seekTable{}, err
	}

	return seekTable{
		entries:   entries,
		checksums: footer.SeekTableDescriptor.ChecksumFlag,
	}, nil
}

func parseSeekTableFooter(buf []byte) (seekTableFooter, int64, error) {
	if len(buf) < seekTableFooterOffset {
		return seekTableFooter{}, 0, fmt.Errorf("footer is too small: %d", len(buf))
	}

	footer := seekTableFooter{}
	if err := footer.UnmarshalBinary(buf[len(buf)-seekTableFooterOffset:]); err != nil {
		return seekTableFooter{}, 0, fmt.Errorf("failed to parse footer %+v: %w", buf, err)
	}

	return footer, seekTableEntrySize(footer.SeekTableDescriptor.ChecksumFlag), nil
}

func seekTableFrameOffset(footer seekTableFooter, entrySize int64) (int64, error) {
	frameOffset := seekTableFooterOffset + entrySize*int64(footer.NumberOfFrames)
	frameOffset += skippableFrameHeaderSize

	if frameOffset > maxDecoderFrameSize {
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
	var compOffset, decompOffset uint64

	var i int64
	for indexOffset := uint64(0); indexOffset < uint64(len(p)); indexOffset += entrySize {
		err := entry.UnmarshalBinary(p[indexOffset : indexOffset+entrySize])
		if err != nil {
			return nil, fmt.Errorf("failed to parse entry %+v at: %d: %w",
				p[indexOffset:indexOffset+entrySize], indexOffset, err)
		}

		entries = append(entries, FrameOffsetEntry{
			ID:           i,
			CompOffset:   compOffset,
			DecompOffset: decompOffset,
			CompSize:     entry.CompressedSize,
			DecompSize:   entry.DecompressedSize,
			Checksum:     entry.Checksum,
		})
		compOffset += uint64(entry.CompressedSize)
		decompOffset += uint64(entry.DecompressedSize)
		i++
	}

	return entries, nil
}
