// Package framecache provides decoded-frame cache implementations for seekable readers.
//
// Cache implementations in this package are safe for direct concurrent use.
package framecache

// Key identifies one decoded frame in one Reader namespace.
//
// Namespace is assigned by seekable.Reader. It is unique to one Reader instance
// and is not a stable stream fingerprint.
type Key struct {
	namespace uint64
	frameID   int64
}

// NewKey returns a cache key for direct cache use.
//
// Most callers do not need this; seekable.Reader creates keys for configured
// caches. namespace must not be treated as a stable stream identity.
func NewKey(namespace uint64, frameID int64) Key {
	return Key{namespace: namespace, frameID: frameID}
}

// FrameID returns the seek-table frame ID embedded in k.
func (k Key) FrameID() int64 {
	return k.frameID
}

// Cache stores decoded frames by key.
//
// Values returned by Get are immutable by convention. Callers must not mutate
// them after putting them in a cache or after getting them back from a cache.
type Cache interface {
	Get(key Key) ([]byte, bool)
	Put(key Key, data []byte)
}

// Clearer is implemented by caches that support explicit clearing.
type Clearer interface {
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
	key     Key
	data    []byte
	size    uint64
	visited bool
}

func newCacheEntry(key Key, data []byte) *cacheEntry {
	return &cacheEntry{
		key:  key,
		data: data,
		size: uint64(len(data)),
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
