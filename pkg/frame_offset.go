package seekable

import "log/slog"

// FrameOffsetEntry is the post-processed view of a seek table entry suitable for indexing.
type FrameOffsetEntry struct {
	// ID is the sequence number of the frame in the index.
	ID int64

	// CompressedOffset is the offset within the compressed stream.
	CompressedOffset uint64
	// DecompressedOffset is the offset within the decompressed stream.
	DecompressedOffset uint64
	// CompressedSize is the size of the compressed frame.
	CompressedSize uint32
	// DecompressedSize is the size of the frame's decompressed data.
	DecompressedSize uint32

	// Checksum is the lower 32 bits of the XXH64 hash of the uncompressed data.
	// It is meaningful only when SeekTable.HasChecksums reports true.
	Checksum uint32
}

type seekTableIndexEntry struct {
	CompressedOffset   uint64
	DecompressedOffset uint64
	CompressedSize     uint32
	DecompressedSize   uint32
	Checksum           uint32
}

func (e seekTableIndexEntry) frameOffsetEntry(id int64) FrameOffsetEntry {
	return FrameOffsetEntry{
		ID:                 id,
		CompressedOffset:   e.CompressedOffset,
		DecompressedOffset: e.DecompressedOffset,
		CompressedSize:     e.CompressedSize,
		DecompressedSize:   e.DecompressedSize,
		Checksum:           e.Checksum,
	}
}

// LogValue implements slog.LogValuer.
func (o FrameOffsetEntry) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int64("ID", o.ID),
		slog.Uint64("CompressedOffset", o.CompressedOffset),
		slog.Uint64("DecompressedOffset", o.DecompressedOffset),
		slog.Uint64("CompressedSize", uint64(o.CompressedSize)),
		slog.Uint64("DecompressedSize", uint64(o.DecompressedSize)),
		slog.Uint64("Checksum", uint64(o.Checksum)),
	)
}
