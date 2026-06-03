package framecache

import (
	"container/list"
	"sync"
)

// FIFO is a first-in, first-out decoded-frame cache.
//
// Hits do not affect eviction order. FIFO is safe for concurrent use.
type FIFO struct {
	limits Limits
	mu     sync.Mutex
	items  map[Key]*list.Element
	order  list.List
	bytes  uint64
}

// NewFIFO returns a FIFO cache with the provided limits.
func NewFIFO(limits Limits) *FIFO {
	return &FIFO{
		limits: limits,
		items:  make(map[Key]*list.Element),
	}
}

// Get returns the cached frame for key.
func (c *FIFO) Get(key Key) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}
	return elem.Value.(*cacheEntry).data, true
}

// Put stores data for key, replacing any existing entry.
func (c *FIFO) Put(key Key, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	size := uint64(len(data))
	if !canStore(c.limits, size) {
		c.remove(key)
		return
	}

	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
	}

	c.evictFor(1, size)
	entry := newCacheEntry(key, data)
	c.items[key] = c.order.PushBack(entry)
	c.bytes += entry.size
}

// Clear removes all cached frames.
func (c *FIFO) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	clear(c.items)
	c.order.Init()
	c.bytes = 0
}

func (c *FIFO) remove(key Key) {
	elem, ok := c.items[key]
	if !ok {
		return
	}
	c.removeElement(elem)
}

func (c *FIFO) removeElement(elem *list.Element) {
	entry := elem.Value.(*cacheEntry)
	delete(c.items, entry.key)
	c.bytes -= entry.size
	c.order.Remove(elem)
}

func (c *FIFO) evictFor(extraFrames int, extraBytes uint64) {
	for overLimits(c.limits, c.order.Len()+extraFrames, c.bytes+extraBytes) {
		elem := c.order.Front()
		if elem == nil {
			return
		}
		c.removeElement(elem)
	}
}
