// Package framecache provides decoded-frame cache implementations for seekable readers.
//
// Cache implementations in this package are safe for direct concurrent use.
// Use NewSynchronized to adapt a simple custom cache for concurrent use.
package framecache

// Cache stores decoded frames by key.
//
// Cache implementations passed to a concurrently used seekable.Reader must be
// safe for concurrent use. The built-in caches satisfy that requirement.
type Cache interface {
	// Get returns the frame stored for key, if any.
	//
	// Implementations may return the same []byte supplied to Put. Callers must
	// not mutate the returned slice.
	Get(key Key) ([]byte, bool)

	// Put stores data for key, replacing any existing value.
	//
	// Implementations may retain data directly. Callers must not mutate data
	// after passing it to Put.
	Put(key Key, data []byte)
}

// Clearer is implemented by caches that support explicit clearing.
type Clearer interface {
	// Clear removes all cached frames.
	Clear()
}

// Limits configures cache capacity.
type Limits struct {
	// MaxFrames caps the number of stored frames. MaxFrames <= 0 disables storage.
	MaxFrames int

	// MaxBytes caps decoded bytes stored in the cache. MaxBytes == 0 means no
	// byte limit. Entries larger than MaxBytes are not stored; if the key
	// already exists, the existing entry is removed.
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
