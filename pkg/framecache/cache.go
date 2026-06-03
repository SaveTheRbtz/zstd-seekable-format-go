// Package framecache provides decoded-frame cache implementations for seekable readers.
//
// Cache implementations in this package are not safe for direct concurrent use.
// seekable.Reader serializes calls to a configured Cache.
package framecache

// Cache stores decoded frames by seek-table frame ID.
//
// Values returned by Get are immutable by convention. Callers must not mutate
// them after putting them in a cache or after getting them back from a cache.
type Cache interface {
	Get(frameID int64) ([]byte, bool)
	Put(frameID int64, data []byte)
	Clear()
}

// Limits configures cache capacity.
type Limits struct {
	// MaxFrames is the required capacity knob. MaxFrames <= 0 means cache nothing.
	MaxFrames int

	// MaxBytes caps decoded bytes stored in the cache. MaxBytes == 0 means no
	// byte limit. When MaxBytes > 0, entries larger than MaxBytes are ignored.
	MaxBytes uint64
}

type cacheEntry struct {
	frameID int64
	data    []byte
	size    uint64
	visited bool
}

func newCacheEntry(frameID int64, data []byte) *cacheEntry {
	return &cacheEntry{
		frameID: frameID,
		data:    data,
		size:    uint64(len(data)),
	}
}

func canStore(limits Limits, size uint64) bool {
	if limits.MaxFrames <= 0 {
		return false
	}
	return limits.MaxBytes == 0 || size <= limits.MaxBytes
}

func overLimits(limits Limits, frames int, bytes uint64) bool {
	if limits.MaxFrames > 0 && frames > limits.MaxFrames {
		return true
	}
	return limits.MaxBytes > 0 && bytes > limits.MaxBytes
}
