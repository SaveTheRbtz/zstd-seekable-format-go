package framecache

import "container/list"

// FIFO is a first-in, first-out decoded-frame cache.
//
// Hits do not affect eviction order. FIFO is not safe for direct concurrent use.
type FIFO struct {
	limits Limits
	items  map[int64]*list.Element
	order  list.List
	bytes  uint64
}

// NewFIFO returns a FIFO cache with the provided limits.
func NewFIFO(limits Limits) *FIFO {
	return &FIFO{
		limits: limits,
		items:  make(map[int64]*list.Element),
	}
}

// Get returns the cached frame for frameID.
func (c *FIFO) Get(frameID int64) ([]byte, bool) {
	elem, ok := c.items[frameID]
	if !ok {
		return nil, false
	}
	return elem.Value.(*cacheEntry).data, true
}

// Put stores data for frameID, replacing any existing entry.
func (c *FIFO) Put(frameID int64, data []byte) {
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
		c.evict()
		return
	}

	entry := newCacheEntry(frameID, data)
	c.items[frameID] = c.order.PushBack(entry)
	c.bytes += entry.size
	c.evict()
}

// Clear removes all cached frames.
func (c *FIFO) Clear() {
	clear(c.items)
	c.order.Init()
	c.bytes = 0
}

func (c *FIFO) remove(frameID int64) {
	elem, ok := c.items[frameID]
	if !ok {
		return
	}
	c.removeElement(elem)
}

func (c *FIFO) removeElement(elem *list.Element) {
	entry := elem.Value.(*cacheEntry)
	delete(c.items, entry.frameID)
	c.bytes -= entry.size
	c.order.Remove(elem)
}

func (c *FIFO) evict() {
	for overLimits(c.limits, c.order.Len(), c.bytes) {
		elem := c.order.Front()
		if elem == nil {
			return
		}
		c.removeElement(elem)
	}
}
