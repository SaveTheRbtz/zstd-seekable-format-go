package framecache

import "container/list"

// Sieve is a decoded-frame cache using the SIEVE-k replacement policy.
//
// Hits and updates increment a per-entry counter, capped at 16. During
// eviction, entries with positive counters are decremented and kept; the first
// entry with a zero counter is evicted.
type Sieve struct {
	limits Limits
	items  map[int64]*list.Element
	order  list.List
	hand   *list.Element
	bytes  uint64
}

const sieveMaxCount = 16

type sieveEntry struct {
	frameID int64
	data    []byte
	count   uint8
}

// NewSieve returns a Sieve cache with the provided limits.
func NewSieve(limits Limits) *Sieve {
	return &Sieve{
		limits: limits,
		items:  make(map[int64]*list.Element),
	}
}

// Get returns the frame stored for frameID, if any. On a hit, Get increments
// the frame's counter.
func (c *Sieve) Get(frameID int64) ([]byte, bool) {
	elem, ok := c.items[frameID]
	if !ok {
		return nil, false
	}
	entry := elem.Value.(*sieveEntry)
	entry.touch()
	return entry.data, true
}

// Put stores data for frameID, replacing any existing entry.
func (c *Sieve) Put(frameID int64, data []byte) {
	size := uint64(len(data))
	if !c.limits.canStore(size) {
		c.remove(frameID)
		return
	}

	if elem, ok := c.items[frameID]; ok {
		entry := elem.Value.(*sieveEntry)
		c.bytes -= uint64(len(entry.data))
		entry.data = data
		entry.touch()
		c.bytes += size
		c.evictForExcept(0, 0, elem)
		return
	}

	c.evictFor(1, size)
	entry := &sieveEntry{frameID: frameID, data: data}
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
	c.evictForExcept(extraFrames, extraBytes, nil)
}

func (c *Sieve) evictForExcept(extraFrames int, extraBytes uint64, protected *list.Element) {
	for c.limits.overLimits(c.order.Len()+extraFrames, c.bytes+extraBytes) {
		if c.hand == nil {
			c.hand = c.order.Back()
		}
		if c.hand == nil {
			return
		}

		elem := c.hand
		if elem == protected {
			next := c.prevCircular(elem)
			if next == nil {
				return
			}
			c.hand = next
			continue
		}

		entry := elem.Value.(*sieveEntry)
		if entry.count > 0 {
			entry.count--
			next := c.prevCircular(elem)
			if next != nil {
				c.hand = next
			}
			continue
		}

		c.removeElement(elem)
	}
}

func (entry *sieveEntry) touch() {
	if entry.count < sieveMaxCount {
		entry.count++
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
