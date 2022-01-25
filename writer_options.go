package seekable

import (
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

type WOption func(*writerOptions) error

type writerOptions struct {
	logger    *zap.Logger
	zstdEOpts []zstd.EOption
	env       WEnvironment
}

func (o *writerOptions) setDefault() {
	*o = writerOptions{
		logger: zap.NewNop(),
	}
}

func WithZSTDEOptions(opts ...zstd.EOption) WOption {
	return func(o *writerOptions) error { o.zstdEOpts = opts; return nil }
}

func WithWLogger(l *zap.Logger) WOption {
	return func(o *writerOptions) error { o.logger = l; return nil }
}

func WithWEnvironment(env WEnvironment) WOption {
	return func(o *writerOptions) error { o.env = env; return nil }
}
