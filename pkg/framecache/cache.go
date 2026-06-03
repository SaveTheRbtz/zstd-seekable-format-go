// Package framecache provides decoded-frame cache implementations for seekable readers.
//
// A Cache used by seekable.Reader need not be safe for concurrent use; the
// reader serializes access.
package framecache

// Cache stores decoded frames by seek-table frame ID.
type Cache interface {
	// Get returns the frame for frameID, if present. The returned slice must not
	// be modified and may alias data passed to Put.
	Get(frameID int64) ([]byte, bool)

	// Put stores data for frameID, replacing any existing value. The caller must
	// not modify data after Put returns.
	Put(frameID int64, data []byte)

	// Clear removes all cached frames.
	Clear()
}

// Limits configures cache capacity.
type Limits struct {
	// MaxFrames caps the number of stored frames. Values <= 0 disable storage.
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
