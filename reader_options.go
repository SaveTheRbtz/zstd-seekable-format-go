package seekable

import (
	"go.uber.org/zap"
)

type ROption func(*readerOptions) error

type readerOptions struct {
	logger *zap.Logger
	env    REnvironment
}

func (o *readerOptions) setDefault() {
	*o = readerOptions{
		logger: zap.NewNop(),
	}
}

func WithRLogger(l *zap.Logger) ROption {
	return func(o *readerOptions) error { o.logger = l; return nil }
}

func WithREnvironment(env REnvironment) ROption {
	return func(o *readerOptions) error { o.env = env; return nil }
}
