package framecache

import (
	"container/list"
	"sync"
)

// Sieve is a decoded-frame cache using the Sieve replacement policy.
//
// Hits set a visited bit. Eviction scans entries and clears visited bits until
// it finds an unvisited entry to evict. Sieve is safe for concurrent use.
type Sieve struct {
	limits Limits
	mu     sync.Mutex
	items  map[Key]*list.Element
	order  list.List
	hand   *list.Element
	bytes  uint64
}

// NewSieve returns a Sieve cache with the provided limits.
func NewSieve(limits Limits) *Sieve {
	return &Sieve{
		limits: limits,
		items:  make(map[Key]*list.Element),
	}
}

// Get returns the cached frame for key and marks it visited.
func (c *Sieve) Get(key Key) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}
	entry := elem.Value.(*cacheEntry)
	entry.visited = true
	return entry.data, true
}

// Put stores data for key, replacing any existing entry.
func (c *Sieve) Put(key Key, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	size := uint64(len(data))
	if !canStore(c.limits, size) {
		c.remove(key)
		return
	}

	visited := false
	if elem, ok := c.items[key]; ok {
		visited = true
		c.removeElement(elem)
	}

	c.evictFor(1, size)
	entry := newCacheEntry(key, data)
	entry.visited = visited
	elem := c.order.PushFront(entry)
	c.items[key] = elem
	c.bytes += entry.size
	if c.hand == nil {
		c.hand = c.order.Back()
	}
}

// Clear removes all cached frames.
func (c *Sieve) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	clear(c.items)
	c.order.Init()
	c.hand = nil
	c.bytes = 0
}

func (c *Sieve) remove(key Key) {
	elem, ok := c.items[key]
	if !ok {
		return
	}
	c.removeElement(elem)
}

func (c *Sieve) removeElement(elem *list.Element) {
	next := c.prevCircular(elem)
	entry := elem.Value.(*cacheEntry)
	delete(c.items, entry.key)
	c.bytes -= entry.size
	c.order.Remove(elem)

	switch {
	case c.order.Len() == 0:
		c.hand = nil
	case c.hand == elem:
		if next != nil {
			c.hand = next
		} else {
			c.hand = c.order.Back()
		}
	}
}

func (c *Sieve) evictFor(extraFrames int, extraBytes uint64) {
	for overLimits(c.limits, c.order.Len()+extraFrames, c.bytes+extraBytes) {
		if c.hand == nil {
			c.hand = c.order.Back()
		}
		if c.hand == nil {
			return
		}

		elem := c.hand
		entry := elem.Value.(*cacheEntry)
		if entry.visited {
			entry.visited = false
			next := c.prevCircular(elem)
			if next != nil {
				c.hand = next
			}
			continue
		}

		c.removeElement(elem)
	}
}

func (c *Sieve) prevCircular(elem *list.Element) *list.Element {
	if c.order.Len() <= 1 {
		return nil
	}
	if prev := elem.Prev(); prev != nil {
		return prev
	}
	return c.order.Back()
}
