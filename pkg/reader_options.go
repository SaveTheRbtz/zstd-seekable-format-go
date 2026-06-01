package seekable

import (
	"log/slog"
)

type rOption func(*readerImpl) error

func WithRLogger(l *slog.Logger) rOption {
	if l == nil {
		l = discardLogger
	}
	return func(r *readerImpl) error { r.logger = l; return nil }
}

func WithREnvironment(e REnvironment) rOption {
	return func(r *readerImpl) error { r.env = e; return nil }
}
