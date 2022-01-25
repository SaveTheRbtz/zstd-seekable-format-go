package seekable

import (
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

type ROption func(*readerOptions) error

type readerOptions struct {
	logger    *zap.Logger
	zstdDOpts []zstd.DOption
	env       REnvironment
}

func (o *readerOptions) setDefault() {
	*o = readerOptions{
		logger: zap.NewNop(),
	}
}

func WithZSTDDOptions(opts ...zstd.DOption) ROption {
	return func(o *readerOptions) error { o.zstdDOpts = opts; return nil }
}

func WithRLogger(l *zap.Logger) ROption {
	return func(o *readerOptions) error { o.logger = l; return nil }
}

func WithREnvironment(env REnvironment) ROption {
	return func(o *readerOptions) error { o.env = env; return nil }
}
