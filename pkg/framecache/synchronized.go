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
// are already safe for concurrent use and do not need this wrapper.
//
// The returned wrapper's Clear method calls the wrapped cache's Clear method if
// it implements Clearer. Otherwise Clear is a no-op.
//
// NewSynchronized panics if cache is nil.
func NewSynchronized(cache Cache) *Synchronized {
	if cache == nil {
		panic("framecache: nil cache")
	}
	return &Synchronized{cache: cache}
}

// Get calls the wrapped cache's Get method while holding c's lock.
func (c *Synchronized) Get(key Key) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cache.Get(key)
}

// Put calls the wrapped cache's Put method while holding c's lock.
func (c *Synchronized) Put(key Key, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache.Put(key, data)
}

// Clear calls the wrapped cache's Clear method while holding c's lock if the
// wrapped cache implements Clearer. Otherwise Clear is a no-op.
func (c *Synchronized) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if clearer, ok := c.cache.(Clearer); ok {
		clearer.Clear()
	}
}
