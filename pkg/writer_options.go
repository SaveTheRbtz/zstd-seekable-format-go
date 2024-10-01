package seekable

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/env"
)

type wOption func(*writerImpl) error

func WithWLogger(l *zap.Logger) wOption {
	return func(w *writerImpl) error { w.logger = l; return nil }
}

func WithWEnvironment(e env.WEnvironment) wOption {
	return func(w *writerImpl) error { w.env = e; return nil }
}

type writeManyOptions struct {
	concurrency   int
	writeCallback func(uint32)
}

type WriteManyOption func(options *writeManyOptions) error

func WithConcurrency(concurrency int) WriteManyOption {
	return func(options *writeManyOptions) error {
		if concurrency < 1 {
			return fmt.Errorf("concurrency must be positive: %d", concurrency)
		}
		options.concurrency = concurrency
		return nil
	}
}

func WithWriteCallback(cb func(size uint32)) WriteManyOption {
	return func(options *writeManyOptions) error {
		options.writeCallback = cb
		return nil
	}
}
