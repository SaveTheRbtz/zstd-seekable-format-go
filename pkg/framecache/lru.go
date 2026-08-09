package framecache

// LRU is a decoded-frame cache that evicts the least recently used frame.
//
// Put and successful Get calls mark frames most recently used.
type LRU struct {
	limits Limits
	items  map[int64]*lruEntry
	order  intrusiveList[*lruEntry]
	bytes  uint64
}

type lruEntry struct {
	frameID                   int64
	data                      []byte
	intrusiveLinks[*lruEntry] //nolint:unused // Used through method promotion by intrusiveList.
}

// NewLRU returns an LRU cache with the provided limits.
func NewLRU(limits Limits) *LRU {
	return &LRU{
		limits: limits,
		items:  make(map[int64]*lruEntry),
	}
}

// Get returns the frame stored for frameID, if any. On a hit, Get marks the
// frame most recently used.
func (c *LRU) Get(frameID int64) ([]byte, bool) {
	entry, ok := c.items[frameID]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(entry)
	return entry.data, true
}

// Put stores data for frameID, replacing any existing frame and marking it most
// recently used.
func (c *LRU) Put(frameID int64, data []byte) {
	size := uint64(len(data))
	if !c.limits.canStore(size) {
		c.remove(frameID)
		return
	}

	if entry, ok := c.items[frameID]; ok {
		c.bytes -= uint64(len(entry.data))
		entry.data = data
		c.bytes += size
		c.order.MoveToFront(entry)
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
	entry, ok := c.items[frameID]
	if !ok {
		return
	}
	c.removeElement(entry)
}

func (c *LRU) removeElement(entry *lruEntry) {
	delete(c.items, entry.frameID)
	c.bytes -= uint64(len(entry.data))
	c.order.Remove(entry)
}

func (c *LRU) evict() {
	for c.limits.overLimits(c.order.Len(), c.bytes) {
		entry := c.order.Back()
		if entry == nil {
			return
		}
		c.removeElement(entry)
	}
}
