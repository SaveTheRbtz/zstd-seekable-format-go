package framecache

// FIFO is a decoded-frame cache using first-in, first-out replacement.
//
// Calls to Get do not affect eviction order.
type FIFO struct {
	limits Limits
	items  map[int64]*fifoEntry
	order  intrusiveList[*fifoEntry]
	bytes  uint64
}

type fifoEntry struct {
	frameID int64
	data    []byte
	intrusiveLinks[*fifoEntry]
}

// NewFIFO returns a FIFO cache with the provided limits.
func NewFIFO(limits Limits) *FIFO {
	return &FIFO{
		limits: limits,
		items:  make(map[int64]*fifoEntry),
	}
}

// Get returns the frame stored for frameID, if any.
func (c *FIFO) Get(frameID int64) ([]byte, bool) {
	entry, ok := c.items[frameID]
	if !ok {
		return nil, false
	}
	return entry.data, true
}

// Put stores data for frameID as a new FIFO insertion, replacing any existing
// entry.
func (c *FIFO) Put(frameID int64, data []byte) {
	size := uint64(len(data))
	if !c.limits.canStore(size) {
		c.remove(frameID)
		return
	}

	if entry, ok := c.items[frameID]; ok {
		c.removeElement(entry)
	}

	c.evictFor(1, size)
	entry := &fifoEntry{frameID: frameID, data: data}
	c.items[frameID] = c.order.PushBack(entry)
	c.bytes += uint64(len(entry.data))
}

// Clear removes all cached frames.
func (c *FIFO) Clear() {
	clear(c.items)
	c.order.Init()
	c.bytes = 0
}

func (c *FIFO) remove(frameID int64) {
	entry, ok := c.items[frameID]
	if !ok {
		return
	}
	c.removeElement(entry)
}

func (c *FIFO) removeElement(entry *fifoEntry) {
	delete(c.items, entry.frameID)
	c.bytes -= uint64(len(entry.data))
	c.order.Remove(entry)
}

func (c *FIFO) evictFor(extraFrames int, extraBytes uint64) {
	for c.limits.overLimits(c.order.Len()+extraFrames, c.bytes+extraBytes) {
		entry := c.order.Front()
		if entry == nil {
			return
		}
		c.removeElement(entry)
	}
}
