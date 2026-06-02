package seekable

import (
	"fmt"
)

// WriterOption configures NewWriter and NewEncoder.
// Use WithLogger and WithWriterEnvironment to create WriterOptions.
type WriterOption interface {
	applyWriter(*writerImpl) error
}

type writerOptionFunc func(*writerImpl) error

func (f writerOptionFunc) applyWriter(w *writerImpl) error {
	return f(w)
}

// WithWriterEnvironment sets a custom write environment for advanced storage implementations.
//
// When this option is supplied, NewWriter uses e instead of the io.Writer
// argument for all frame and seek-table writes.
func WithWriterEnvironment(e WriterEnvironment) WriterOption {
	return writerOptionFunc(func(w *writerImpl) error { w.env = e; return nil })
}

type writeManyOptions struct {
	concurrency   int
	writeCallback func(uint32)
}

// WriteManyOption configures ConcurrentWriter.WriteMany.
type WriteManyOption func(options *writeManyOptions) error

// WithConcurrency sets the maximum number of concurrent frame encoding operations.
//
// The default is runtime.GOMAXPROCS(0).
func WithConcurrency(concurrency int) WriteManyOption {
	return func(options *writeManyOptions) error {
		if concurrency < 1 {
			return fmt.Errorf("concurrency must be positive: %d", concurrency)
		}
		options.concurrency = concurrency
		return nil
	}
}

// WithWriteCallback calls cb after each frame is written.
//
// cb receives the decompressed size of the frame that was just written. It is
// called in stream order from the WriteMany writer goroutine.
func WithWriteCallback(cb func(size uint32)) WriteManyOption {
	return func(options *writeManyOptions) error {
		options.writeCallback = cb
		return nil
	}
}
