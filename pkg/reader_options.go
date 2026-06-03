package seekable

import (
	"log/slog"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/framecache"
)

// ReaderOption configures NewReader.
type ReaderOption func(*Reader) error

// WithReaderLogger sets the logger used by Reader internals.
//
// Passing nil restores the default discard logger.
func WithReaderLogger(l *slog.Logger) ReaderOption {
	if l == nil {
		l = discardLogger
	}
	return func(r *Reader) error { r.logger = l; return nil }
}

// WithReaderEnvironment sets a custom read environment for advanced storage implementations.
//
// When this option is supplied, NewReader uses e instead of the io.ReadSeeker
// argument for all seek-table and frame reads.
func WithReaderEnvironment(e ReaderEnvironment) ReaderOption {
	return func(r *Reader) error { r.env = e; return nil }
}

// WithReaderFrameCache sets the Reader's decoded-frame cache.
//
// A nil cache selects the default one-frame FIFO cache. To disable caching, use
// framecache.NewFIFO(framecache.Limits{MaxFrames: 0}).
//
// If NewReader succeeds, the Reader owns the cache, clearing it before use and
// on Close. Callers must not use the cache directly or share it with another
// Reader.
func WithReaderFrameCache(cache framecache.Cache) ReaderOption {
	return func(r *Reader) error {
		r.frameCache = newReaderFrameCache(cache)
		return nil
	}
}
