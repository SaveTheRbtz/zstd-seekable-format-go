package seekable

import (
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
