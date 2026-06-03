package framecache

import "container/list"

// Sieve is a decoded-frame cache using the Sieve replacement policy.
//
// Hits and replacements mark entries visited. During eviction, visited entries
// get one second chance; the first unvisited entry is evicted.
type Sieve struct {
	limits Limits
	items  map[int64]*list.Element
	order  list.List
	hand   *list.Element
	bytes  uint64
}

type sieveEntry struct {
	frameID int64
	data    []byte
	visited bool
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
	entry := elem.Value.(*sieveEntry)
	entry.visited = true
	return entry.data, true
}

// Put stores data for frameID, replacing any existing entry.
func (c *Sieve) Put(frameID int64, data []byte) {
	size := uint64(len(data))
	if !c.limits.canStore(size) {
		c.remove(frameID)
		return
	}

	visited := false
	if elem, ok := c.items[frameID]; ok {
		visited = true
		c.removeElement(elem)
	}

	c.evictFor(1, size)
	entry := &sieveEntry{frameID: frameID, data: data, visited: visited}
	elem := c.order.PushFront(entry)
	c.items[frameID] = elem
	c.bytes += uint64(len(entry.data))
	if c.hand == nil {
		c.hand = c.order.Back()
	}
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
	entry := elem.Value.(*sieveEntry)
	delete(c.items, entry.frameID)
	c.bytes -= uint64(len(entry.data))
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
	for c.limits.overLimits(c.order.Len()+extraFrames, c.bytes+extraBytes) {
		if c.hand == nil {
			c.hand = c.order.Back()
		}
		if c.hand == nil {
			return
		}

		elem := c.hand
		entry := elem.Value.(*sieveEntry)
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
