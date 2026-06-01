package seekable

// WEnvironment is an advanced hook for custom storage implementations.
// It can be used to write frames and seek tables somewhere other than a normal io.Writer.
type WEnvironment interface {
	// WriteFrame is called each time frame is encoded and needs to be written upstream.
	WriteFrame(p []byte) (n int, err error)
	// WriteSeekTable is called on Close to flush the seek table.
	WriteSeekTable(p []byte) (n int, err error)
}

// REnvironment is an advanced hook for custom storage implementations.
// It can be used to read frames and seek tables from somewhere other than a normal io.ReadSeeker.
type REnvironment interface {
	// GetFrameByIndex returns the compressed frame by its index.
	GetFrameByIndex(index FrameOffsetEntry) ([]byte, error)
	// ReadFooter returns buffer whose last 9 bytes are interpreted as a `Seek_Table_Footer`.
	ReadFooter() ([]byte, error)
	// ReadSkipFrame returns the full Seek Table Skippable frame
	// including the `Skippable_Magic_Number` and `Frame_Size`.
	ReadSkipFrame(skippableFrameOffset int64) ([]byte, error)
}
