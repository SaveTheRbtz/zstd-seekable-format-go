package options

import (
	"go.uber.org/zap"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/env"
)

type ROption func(*ReaderOptions) error

type ReaderOptions struct {
	Logger *zap.Logger
	Env    env.REnvironment
}

func (o *ReaderOptions) SetDefault() {
	*o = ReaderOptions{
		Logger: zap.NewNop(),
	}
}

func WithRLogger(l *zap.Logger) ROption {
	return func(o *ReaderOptions) error { o.Logger = l; return nil }
}

func WithREnvironment(e env.REnvironment) ROption {
	return func(o *ReaderOptions) error { o.Env = e; return nil }
}
