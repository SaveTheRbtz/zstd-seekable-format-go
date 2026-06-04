package framecache

import "container/list"

// FIFO is a decoded-frame cache using first-in, first-out replacement.
//
// Calls to Get do not affect eviction order.
type FIFO struct {
	limits Limits
	items  map[int64]*list.Element
	order  list.List
	bytes  uint64
}

type fifoEntry struct {
	frameID int64
	data    []byte
}

// NewFIFO returns a FIFO cache with the provided limits.
func NewFIFO(limits Limits) *FIFO {
	return &FIFO{
		limits: limits,
		items:  make(map[int64]*list.Element),
	}
}

// Get returns the frame stored for frameID, if any.
func (c *FIFO) Get(frameID int64) ([]byte, bool) {
	elem, ok := c.items[frameID]
	if !ok {
		return nil, false
	}
	return elem.Value.(*fifoEntry).data, true
}

// Put stores data for frameID as a new FIFO insertion, replacing any existing
// entry.
func (c *FIFO) Put(frameID int64, data []byte) {
	_, _ = c.PutWithEvicted(frameID, data)
}

// PutWithEvicted stores data and returns one evicted frame buffer, if any.
func (c *FIFO) PutWithEvicted(frameID int64, data []byte) ([]byte, bool) {
	size := uint64(len(data))
	if !c.limits.canStore(size) {
		return c.remove(frameID), false
	}

	var evicted []byte
	if elem, ok := c.items[frameID]; ok {
		evicted = c.removeElement(elem)
	}

	if removed := c.evictFor(1, size); removed != nil {
		evicted = removed
	}
	entry := &fifoEntry{frameID: frameID, data: data}
	c.items[frameID] = c.order.PushBack(entry)
	c.bytes += uint64(len(entry.data))
	return evicted, true
}

// Clear removes all cached frames.
func (c *FIFO) Clear() {
	clear(c.items)
	c.order.Init()
	c.bytes = 0
}

func (c *FIFO) remove(frameID int64) []byte {
	elem, ok := c.items[frameID]
	if !ok {
		return nil
	}
	return c.removeElement(elem)
}

func (c *FIFO) removeElement(elem *list.Element) []byte {
	entry := elem.Value.(*fifoEntry)
	delete(c.items, entry.frameID)
	c.bytes -= uint64(len(entry.data))
	c.order.Remove(elem)
	return entry.data
}

func (c *FIFO) evictFor(extraFrames int, extraBytes uint64) []byte {
	var evicted []byte
	for c.limits.overLimits(c.order.Len()+extraFrames, c.bytes+extraBytes) {
		elem := c.order.Front()
		if elem == nil {
			return evicted
		}
		evicted = c.removeElement(elem)
	}
	return evicted
}
