package seekable

// ReaderOption configures NewReader.
// Use WithLogger and WithReaderEnvironment to create ReaderOptions.
type ReaderOption interface {
	applyReader(*readerImpl) error
}

type readerOptionFunc func(*readerImpl) error

func (f readerOptionFunc) applyReader(r *readerImpl) error {
	return f(r)
}

// WithReaderEnvironment sets a custom read environment for advanced storage implementations.
//
// When this option is supplied, NewReader uses e instead of the io.ReadSeeker
// argument for all seek-table and frame reads.
func WithReaderEnvironment(e ReaderEnvironment) ReaderOption {
	return readerOptionFunc(func(r *readerImpl) error { r.env = e; return nil })
}
