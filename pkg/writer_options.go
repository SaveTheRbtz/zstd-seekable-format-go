package seekable

import (
	"fmt"
	"log/slog"
)

type wOption func(*writerImpl) error

// WithWLogger sets the logger used by Writer and Encoder internals.
func WithWLogger(l *slog.Logger) wOption {
	if l == nil {
		l = discardLogger
	}
	return func(w *writerImpl) error { w.logger = l; return nil }
}

// WithWEnvironment sets a custom write environment for advanced storage implementations.
func WithWEnvironment(e WEnvironment) wOption {
	return func(w *writerImpl) error { w.env = e; return nil }
}

type writeManyOptions struct {
	concurrency   int
	writeCallback func(uint32)
}

type WriteManyOption func(options *writeManyOptions) error

// WithConcurrency sets the maximum number of concurrent frame encoding operations.
func WithConcurrency(concurrency int) WriteManyOption {
	return func(options *writeManyOptions) error {
		if concurrency < 1 {
			return fmt.Errorf("concurrency must be positive: %d", concurrency)
		}
		options.concurrency = concurrency
		return nil
	}
}

// WithWriteCallback calls cb after each frame is written, passing the decompressed frame size.
func WithWriteCallback(cb func(size uint32)) WriteManyOption {
	return func(options *writeManyOptions) error {
		options.writeCallback = cb
		return nil
	}
}
