package seekable

import (
	"log/slog"
)

type rOption func(*readerImpl) error

// WithRLogger sets the logger used by Reader internals.
//
// Passing nil restores the default discard logger.
func WithRLogger(l *slog.Logger) rOption {
	if l == nil {
		l = discardLogger
	}
	return func(r *readerImpl) error { r.logger = l; return nil }
}

// WithREnvironment sets a custom read environment for advanced storage implementations.
//
// When this option is supplied, NewReader uses e instead of the io.ReadSeeker
// argument for all seek-table and frame reads.
func WithREnvironment(e REnvironment) rOption {
	return func(r *readerImpl) error { r.env = e; return nil }
}
