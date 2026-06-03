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

// WithReaderFrameCache sets the decoded-frame cache used by Reader.
//
// Passing nil restores the default one-frame FIFO cache. To disable caching,
// pass framecache.NewFIFO(framecache.Limits{MaxFrames: 0}).
//
// Reader borrows the supplied cache and does not clear it on Close. Built-in
// framecache caches are safe for concurrent use. Custom caches shared between
// concurrently used Readers must be safe for concurrent use or externally
// synchronized.
func WithReaderFrameCache(cache framecache.Cache) ReaderOption {
	return func(r *Reader) error {
		selected := cache
		if selected == nil {
			selected = defaultFrameCache()
		}
		r.frameCache = selected
		return nil
	}
}
