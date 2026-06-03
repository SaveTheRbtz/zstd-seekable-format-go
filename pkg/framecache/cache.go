// Package framecache provides decoded-frame cache implementations for seekable readers.
//
// A cache is owned by one seekable.Reader. Reader serializes cache access, so
// Cache implementations do not need to be safe for concurrent use.
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

	// MaxBytes caps decoded bytes stored in the cache. MaxBytes == 0 means no
	// byte limit. Entries larger than MaxBytes are not stored; if the frame ID
	// already exists, the existing entry is removed.
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
