package seekable

import (
	"go.uber.org/zap"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/env"
)

type rOption func(*readerImpl) error

func WithRLogger(l *zap.Logger) rOption {
	return func(r *readerImpl) error { r.logger = l; return nil }
}

func WithREnvironment(e env.REnvironment) rOption {
	return func(r *readerImpl) error { r.env = e; return nil }
}
