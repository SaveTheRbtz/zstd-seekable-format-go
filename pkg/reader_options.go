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
// A nil cache selects the default one-frame FIFO cache. To disable caching, use
// framecache.NewFIFO(framecache.Limits{MaxFrames: 0}).
//
// Reader uses the supplied cache directly and does not clear it on Close.
// Built-in framecache caches are safe for concurrent use; wrap unsynchronized
// custom caches with framecache.NewSynchronized before using them with
// concurrent ReadAt calls or sharing them between Readers.
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
