package seekable

import (
	"go.uber.org/zap"
)

type WOption func(*writerOptions) error

type writerOptions struct {
	logger *zap.Logger
	env    WEnvironment
}

func (o *writerOptions) setDefault() {
	*o = writerOptions{
		logger: zap.NewNop(),
	}
}

func WithWLogger(l *zap.Logger) WOption {
	return func(o *writerOptions) error { o.logger = l; return nil }
}

func WithWEnvironment(env WEnvironment) WOption {
	return func(o *writerOptions) error { o.env = env; return nil }
}
