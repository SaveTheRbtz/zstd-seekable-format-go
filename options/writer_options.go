package options

import (
	"go.uber.org/zap"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/env"
)

type WOption func(*WriterOptions) error

type WriterOptions struct {
	Logger *zap.Logger
	Env    env.WEnvironment
}

func (o *WriterOptions) SetDefault() {
	*o = WriterOptions{
		Logger: zap.NewNop(),
	}
}

func WithWLogger(l *zap.Logger) WOption {
	return func(o *WriterOptions) error { o.Logger = l; return nil }
}

func WithWEnvironment(e env.WEnvironment) WOption {
	return func(o *WriterOptions) error { o.Env = e; return nil }
}
