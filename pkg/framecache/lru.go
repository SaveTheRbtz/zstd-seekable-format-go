package framecache

import "container/list"

// LRU is a least-recently-used decoded-frame cache.
//
// Hits move entries to the front of the cache. LRU is not safe for direct
// concurrent use.
type LRU struct {
	limits Limits
	items  map[int64]*list.Element
	order  list.List
	bytes  uint64
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
	return elem.Value.(*cacheEntry).data, true
}

// Put stores data for frameID, replacing any existing entry.
func (c *LRU) Put(frameID int64, data []byte) {
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
		c.bytes += size
		c.order.MoveToFront(elem)
		c.evict()
		return
	}

	entry := newCacheEntry(frameID, data)
	c.items[frameID] = c.order.PushFront(entry)
	c.bytes += entry.size
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
	entry := elem.Value.(*cacheEntry)
	delete(c.items, entry.frameID)
	c.bytes -= entry.size
	c.order.Remove(elem)
}

func (c *LRU) evict() {
	for overLimits(c.limits, c.order.Len(), c.bytes) {
		elem := c.order.Back()
		if elem == nil {
			return
		}
		c.removeElement(elem)
	}
}
