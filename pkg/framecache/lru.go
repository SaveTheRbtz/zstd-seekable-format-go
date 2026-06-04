package framecache

import "container/list"

// LRU is a decoded-frame cache that evicts the least recently used frame.
//
// Put and successful Get calls mark frames most recently used.
type LRU struct {
	limits Limits
	items  map[int64]*list.Element
	order  list.List
	bytes  uint64
}

type lruEntry struct {
	frameID int64
	data    []byte
}

// NewLRU returns an LRU cache with the provided limits.
func NewLRU(limits Limits) *LRU {
	return &LRU{
		limits: limits,
		items:  make(map[int64]*list.Element),
	}
}

// Get returns the frame stored for frameID, if any. On a hit, Get marks the
// frame most recently used.
func (c *LRU) Get(frameID int64) ([]byte, bool) {
	elem, ok := c.items[frameID]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(elem)
	return elem.Value.(*lruEntry).data, true
}

// Put stores data for frameID, replacing any existing frame and marking it most
// recently used.
func (c *LRU) Put(frameID int64, data []byte) {
	_, _ = c.PutWithEvicted(frameID, data)
}

// PutWithEvicted stores data and returns one evicted frame buffer, if any.
func (c *LRU) PutWithEvicted(frameID int64, data []byte) ([]byte, bool) {
	size := uint64(len(data))
	if !c.limits.canStore(size) {
		return c.remove(frameID), false
	}

	if elem, ok := c.items[frameID]; ok {
		entry := elem.Value.(*lruEntry)
		evicted := entry.data
		c.bytes -= uint64(len(entry.data))
		entry.data = data
		c.bytes += size
		c.order.MoveToFront(elem)
		if removed := c.evict(); removed != nil {
			evicted = removed
		}
		return evicted, true
	}

	entry := &lruEntry{frameID: frameID, data: data}
	c.items[frameID] = c.order.PushFront(entry)
	c.bytes += uint64(len(entry.data))
	return c.evict(), true
}

// Clear removes all cached frames.
func (c *LRU) Clear() {
	clear(c.items)
	c.order.Init()
	c.bytes = 0
}

func (c *LRU) remove(frameID int64) []byte {
	elem, ok := c.items[frameID]
	if !ok {
		return nil
	}
	return c.removeElement(elem)
}

func (c *LRU) removeElement(elem *list.Element) []byte {
	entry := elem.Value.(*lruEntry)
	delete(c.items, entry.frameID)
	c.bytes -= uint64(len(entry.data))
	c.order.Remove(elem)
	return entry.data
}

func (c *LRU) evict() []byte {
	var evicted []byte
	for c.limits.overLimits(c.order.Len(), c.bytes) {
		elem := c.order.Back()
		if elem == nil {
			return evicted
		}
		evicted = c.removeElement(elem)
	}
	return evicted
}
