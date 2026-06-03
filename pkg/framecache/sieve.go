package framecache

import "container/list"

// Sieve is a decoded-frame cache using the Sieve replacement policy.
//
// Hits set a visited bit. Eviction scans entries and clears visited bits until
// it finds an unvisited entry to evict. Sieve is not safe for direct concurrent use.
type Sieve struct {
	limits Limits
	items  map[int64]*list.Element
	order  list.List
	hand   *list.Element
	bytes  uint64
}

// NewSieve returns a Sieve cache with the provided limits.
func NewSieve(limits Limits) *Sieve {
	return &Sieve{
		limits: limits,
		items:  make(map[int64]*list.Element),
	}
}

// Get returns the cached frame for frameID and marks it visited.
func (c *Sieve) Get(frameID int64) ([]byte, bool) {
	elem, ok := c.items[frameID]
	if !ok {
		return nil, false
	}
	entry := elem.Value.(*cacheEntry)
	entry.visited = true
	return entry.data, true
}

// Put stores data for frameID, replacing any existing entry.
func (c *Sieve) Put(frameID int64, data []byte) {
	size := uint64(len(data))
	if !canStore(c.limits, size) {
		c.remove(frameID)
		return
	}

	if elem, ok := c.items[frameID]; ok {
		entry := elem.Value.(*cacheEntry)
		c.bytes -= entry.size
		entry.data = data
		entry.size = size
		entry.visited = true
		c.bytes += size
		c.evict()
		return
	}

	entry := newCacheEntry(frameID, data)
	elem := c.order.PushFront(entry)
	c.items[frameID] = elem
	c.bytes += entry.size
	if c.hand == nil {
		c.hand = c.order.Back()
	}
	c.evict()
}

// Clear removes all cached frames.
func (c *Sieve) Clear() {
	clear(c.items)
	c.order.Init()
	c.hand = nil
	c.bytes = 0
}

func (c *Sieve) remove(frameID int64) {
	elem, ok := c.items[frameID]
	if !ok {
		return
	}
	c.removeElement(elem)
}

func (c *Sieve) removeElement(elem *list.Element) {
	next := c.prevCircular(elem)
	entry := elem.Value.(*cacheEntry)
	delete(c.items, entry.frameID)
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

func (c *Sieve) evict() {
	for overLimits(c.limits, c.order.Len(), c.bytes) {
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
