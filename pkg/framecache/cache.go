// Package framecache provides decoded-frame cache implementations for seekable readers.
//
// Cache implementations in this package are safe for direct concurrent use.
package framecache

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
