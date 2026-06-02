package seekable

// WriterEnvironment is an advanced hook for custom storage implementations.
// It can be used to write frames and seek tables somewhere other than a normal io.Writer.
type WriterEnvironment interface {
	// WriteFrame writes one complete compressed Zstandard frame.
	//
	// It is called once for each non-empty frame, in stream order. It should
	// follow io.Writer conventions and return len(p), nil after a complete write.
	WriteFrame(p []byte) (n int, err error)

	// WriteSeekTable writes the final seek-table skippable frame.
	//
	// p includes the skippable-frame magic number, frame-size field, seek-table
	// entries, and seek-table footer. It should follow io.Writer conventions and
	// return len(p), nil after a complete write.
	WriteSeekTable(p []byte) (n int, err error)
}

// ReaderEnvironment is an advanced hook for custom storage implementations.
// It can be used to read frames and seek tables from somewhere other than a normal io.ReadSeeker.
type ReaderEnvironment interface {
	// GetFrameByIndex returns one complete compressed frame by seek-table entry.
	//
	// The returned slice must contain exactly index.CompressedSize bytes starting
	// at index.CompressedOffset in the compressed stream.
	GetFrameByIndex(index FrameOffsetEntry) ([]byte, error)

	// ReadFooter returns bytes whose last 9 bytes are interpreted as a Seek_Table_Footer.
	ReadFooter() ([]byte, error)

	// ReadSkipFrame returns the full final seek-table skippable frame.
	//
	// skippableFrameOffset is the number of bytes from the end of the stream
	// back to the start of the final skippable frame. The returned bytes must
	// include the Skippable_Magic_Number and Frame_Size fields.
	ReadSkipFrame(skippableFrameOffset int64) ([]byte, error)
}
