package framecache

import "sync"

// Synchronized serializes calls to another cache.
type Synchronized struct {
	mu    sync.Mutex
	cache Cache
}

var (
	_ Cache   = (*Synchronized)(nil)
	_ Clearer = (*Synchronized)(nil)
)

// NewSynchronized returns a cache wrapper that serializes calls to cache.
//
// It is intended for simple custom caches that are used with concurrent
// seekable.Reader.ReadAt calls or shared by multiple Readers. Built-in caches
// are already safe for concurrent use and do not need this wrapper. The supplied
// cache must be non-nil.
//
// The returned wrapper's Clear method calls the wrapped cache's Clear method if
// it implements Clearer. Otherwise Clear is a no-op.
func NewSynchronized(cache Cache) *Synchronized {
	return &Synchronized{cache: cache}
}

func (c *Synchronized) Get(key Key) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cache.Get(key)
}

func (c *Synchronized) Put(key Key, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache.Put(key, data)
}

// Clear clears the wrapped cache if it implements Clearer.
func (c *Synchronized) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if clearer, ok := c.cache.(Clearer); ok {
		clearer.Clear()
	}
}
