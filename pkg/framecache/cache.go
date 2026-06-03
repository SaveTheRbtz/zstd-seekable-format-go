// Package framecache provides decoded-frame cache implementations for seekable readers.
//
// When a cache is used by a seekable.Reader, the reader owns it and serializes
// access, so Cache implementations used only that way need not be safe for
// concurrent use.
package framecache

// Cache stores decoded frames by seek-table frame ID.
type Cache interface {
	// Get returns the frame stored for frameID, if any.
	//
	// Implementations may return the same []byte supplied to Put. Callers must
	// not mutate the returned slice.
	Get(frameID int64) ([]byte, bool)

	// Put stores data for frameID, replacing any existing value.
	//
	// Implementations may retain data directly. Callers must not mutate data
	// after passing it to Put.
	Put(frameID int64, data []byte)

	// Clear removes all cached frames.
	Clear()
}

// Limits configures cache capacity.
type Limits struct {
	// MaxFrames caps the number of stored frames. MaxFrames <= 0 disables storage.
	MaxFrames int

	// MaxBytes caps decoded bytes stored in the cache. A zero value applies no
	// byte limit. If data passed to Put is larger than a positive MaxBytes, Put
	// does not store it and removes any existing entry for that frame ID.
	MaxBytes uint64
}

func (limits Limits) canStore(size uint64) bool {
	if limits.MaxFrames <= 0 {
		return false
	}
	return limits.MaxBytes == 0 || size <= limits.MaxBytes
}

func (limits Limits) overLimits(frames int, bytes uint64) bool {
	if limits.MaxFrames > 0 && frames > limits.MaxFrames {
		return true
	}
	return limits.MaxBytes > 0 && bytes > limits.MaxBytes
}
