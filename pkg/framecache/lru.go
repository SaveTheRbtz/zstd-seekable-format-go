package framecache

import (
	"container/list"
	"sync"
)

// LRU is a decoded-frame cache using the least-recently-used replacement policy.
//
// Hits and replacements mark entries recently used. LRU is safe for concurrent
// use.
type LRU struct {
	limits Limits
	mu     sync.Mutex
	items  map[Key]*list.Element
	order  list.List
	bytes  uint64
}

type lruEntry struct {
	key  Key
	data []byte
}

// NewLRU returns an LRU cache with the provided limits.
func NewLRU(limits Limits) *LRU {
	return &LRU{
		limits: limits,
		items:  make(map[Key]*list.Element),
	}
}

// Get returns the cached frame for key and marks it recently used.
func (c *LRU) Get(key Key) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(elem)
	return elem.Value.(*lruEntry).data, true
}

// Put stores data for key, replacing any existing entry.
func (c *LRU) Put(key Key, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	size := uint64(len(data))
	if !canStore(c.limits, size) {
		c.remove(key)
		return
	}

	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*lruEntry)
		c.bytes -= uint64(len(entry.data))
		entry.data = data
		c.bytes += size
		c.order.MoveToFront(elem)
		c.evict()
		return
	}

	entry := &lruEntry{key: key, data: data}
	c.items[key] = c.order.PushFront(entry)
	c.bytes += uint64(len(entry.data))
	c.evict()
}

// Clear removes all cached frames.
func (c *LRU) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	clear(c.items)
	c.order.Init()
	c.bytes = 0
}

func (c *LRU) remove(key Key) {
	elem, ok := c.items[key]
	if !ok {
		return
	}
	c.removeElement(elem)
}

func (c *LRU) removeElement(elem *list.Element) {
	entry := elem.Value.(*lruEntry)
	delete(c.items, entry.key)
	c.bytes -= uint64(len(entry.data))
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
