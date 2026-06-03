package framecache

import "container/list"

// LRU is a decoded-frame cache using the least-recently-used replacement policy.
//
// Hits and replacements mark entries recently used.
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

// Get returns the cached frame for frameID and marks it recently used.
func (c *LRU) Get(frameID int64) ([]byte, bool) {
	elem, ok := c.items[frameID]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(elem)
	return elem.Value.(*lruEntry).data, true
}

// Put stores data for frameID, replacing any existing entry.
func (c *LRU) Put(frameID int64, data []byte) {
	size := uint64(len(data))
	if !c.limits.canStore(size) {
		c.remove(frameID)
		return
	}

	if elem, ok := c.items[frameID]; ok {
		entry := elem.Value.(*lruEntry)
		c.bytes -= uint64(len(entry.data))
		entry.data = data
		c.bytes += size
		c.order.MoveToFront(elem)
		c.evict()
		return
	}

	entry := &lruEntry{frameID: frameID, data: data}
	c.items[frameID] = c.order.PushFront(entry)
	c.bytes += uint64(len(entry.data))
	c.evict()
}

// Clear removes all cached frames.
func (c *LRU) Clear() {
	clear(c.items)
	c.order.Init()
	c.bytes = 0
}

func (c *LRU) remove(frameID int64) {
	elem, ok := c.items[frameID]
	if !ok {
		return
	}
	c.removeElement(elem)
}

func (c *LRU) removeElement(elem *list.Element) {
	entry := elem.Value.(*lruEntry)
	delete(c.items, entry.frameID)
	c.bytes -= uint64(len(entry.data))
	c.order.Remove(elem)
}

func (c *LRU) evict() {
	for c.limits.overLimits(c.order.Len(), c.bytes) {
		elem := c.order.Back()
		if elem == nil {
			return
		}
		c.removeElement(elem)
	}
}
