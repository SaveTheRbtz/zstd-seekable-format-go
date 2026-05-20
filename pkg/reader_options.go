package seekable

import (
	"log/slog"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/env"
)

type rOption func(*readerImpl) error

func WithRLogger(l *slog.Logger) rOption {
	return func(r *readerImpl) error {
		if l == nil {
			l = discardLogger
		}
		r.logger = l
		return nil
	}
}

func WithREnvironment(e env.REnvironment) rOption {
	return func(r *readerImpl) error { r.env = e; return nil }
}
