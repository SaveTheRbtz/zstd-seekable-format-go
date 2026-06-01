package seekable

import (
	"log/slog"
)

type rOption func(*readerImpl) error

// WithRLogger sets the logger used by Reader internals.
func WithRLogger(l *slog.Logger) rOption {
	if l == nil {
		l = discardLogger
	}
	return func(r *readerImpl) error { r.logger = l; return nil }
}

// WithREnvironment sets a custom read environment for advanced storage implementations.
func WithREnvironment(e REnvironment) rOption {
	return func(r *readerImpl) error { r.env = e; return nil }
}
