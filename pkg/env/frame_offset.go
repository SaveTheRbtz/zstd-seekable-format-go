package env

import (
	"go.uber.org/zap/zapcore"
)

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

func Less(a, b *FrameOffsetEntry) bool {
	return a.DecompOffset < b.DecompOffset
}
