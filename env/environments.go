package env

// WEnvironment can be used to inject a custom file writer that is different from normal WriteCloser.
// This is useful when, for example there is a custom chunking code.
type WEnvironment interface {
	// WriteFrame is called each time frame is encoded and needs to be written upstream.
	WriteFrame(p []byte) (n int, err error)
	// WriteSeekIndex is called on Close to flush the seek table.
	WriteSeekIndex(p []byte) (n int, err error)
}

// REnvironment can be used to inject a custom file reader that is different from normal ReadSeeker.
// This is useful when, for example there is a custom chunking code.
type REnvironment interface {
	// GetFrameByIndex returns the compressed frame by its index.
	GetFrameByIndex(index SeekIndexEntry) ([]byte, error)
	// ReadSeekIndexFooter returns footer of the Seek Table Skippable frame.
	// This specifies format and location of the Seek Table.
	// Then ReadSeekIndex is called to fetch and parse the Seek Table.
	//
	// If more than 9 bytes are returned only the first 9 bytes are used.
	ReadSeekIndexFooter() ([]byte, error)
	// ReadSeekIndex returns the full Seek Table Skippable frame
	// including the `Skippable_Magic_Number` and `Frame_Size`.
	ReadSeekIndex(indexPosition int64) ([]byte, error)
}
