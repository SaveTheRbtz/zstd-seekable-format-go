package seekable

import (
	"fmt"
	"log/slog"
)

// WriterOption configures NewWriter and NewEncoder.
// Options that configure output environments apply only to NewWriter.
type WriterOption func(*Writer) error

// WithWriterLogger sets the logger used by Writer and Encoder internals.
//
// Passing nil restores the default discard logger.
func WithWriterLogger(l *slog.Logger) WriterOption {
	if l == nil {
		l = discardLogger
	}
	return func(w *Writer) error { w.logger = l; return nil }
}

// WithWriterEnvironment sets a custom write environment for advanced storage implementations.
//
// When this option is supplied to NewWriter, NewWriter uses e instead of the
// io.Writer argument for all frame and seek-table writes. NewEncoder returns
// compressed frames directly, so WithWriterEnvironment has no effect there.
func WithWriterEnvironment(e WriterEnvironment) WriterOption {
	return func(w *Writer) error { w.env = e; return nil }
}

type writeManyOptions struct {
	concurrency   int
	frameCallback func(FrameOffsetEntry)
}

// WriteManyOption configures Writer.WriteMany.
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

// WithWriteCallback calls cb after each non-empty frame is written.
//
// cb receives the seek-table entry for the frame that was just written. It is
// called in stream order from the WriteMany writer goroutine, after the frame
// has been written and added to the Writer's in-memory seek table.
//
// cb must not call methods on the same Writer. To stop WriteMany from cb,
// cancel the WriteMany context or make the FrameSource return an error.
func WithWriteCallback(cb func(entry FrameOffsetEntry)) WriteManyOption {
	return func(options *writeManyOptions) error {
		options.frameCallback = cb
		return nil
	}
}
