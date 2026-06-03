package framecache

import "sync"

// Synchronized wraps a Cache so its operations are safe for concurrent use.
type Synchronized struct {
	mu    sync.Mutex
	cache Cache
}

var (
	_ Cache   = (*Synchronized)(nil)
	_ Clearer = (*Synchronized)(nil)
)

// NewSynchronized returns a concurrent-safe wrapper around cache.
//
// It is intended for simple custom caches that are used with concurrent
// seekable.Reader.ReadAt calls or shared by multiple Readers. Built-in caches
// are already safe for concurrent use and do not need this wrapper.
//
// NewSynchronized panics if cache is nil.
func NewSynchronized(cache Cache) *Synchronized {
	if cache == nil {
		panic("framecache: nil cache")
	}
	return &Synchronized{cache: cache}
}

// Get returns the cached frame for key from the wrapped cache.
func (c *Synchronized) Get(key Key) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cache.Get(key)
}

// Put stores data for key in the wrapped cache.
func (c *Synchronized) Put(key Key, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache.Put(key, data)
}

// Clear clears the wrapped cache if it implements Clearer. Otherwise Clear does
// nothing.
func (c *Synchronized) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if clearer, ok := c.cache.(Clearer); ok {
		clearer.Clear()
	}
}
