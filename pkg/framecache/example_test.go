package framecache_test

import (
	"fmt"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/framecache"
)

type clockCache struct {
	maxFrames int
	hand      int
	items     map[int64]int
	slots     []clockSlot
}

type clockSlot struct {
	frameID int64
	data    []byte
	used    bool
}

var _ framecache.Cache = (*clockCache)(nil)

func newClockCache(maxFrames int) *clockCache {
	return &clockCache{
		maxFrames: maxFrames,
		items:     make(map[int64]int),
	}
}

func (c *clockCache) Get(frameID int64) ([]byte, bool) {
	index, ok := c.items[frameID]
	if !ok {
		return nil, false
	}
	c.slots[index].used = true
	return c.slots[index].data, true
}

func (c *clockCache) Put(frameID int64, data []byte) {
	if c.maxFrames <= 0 {
		return
	}

	if index, ok := c.items[frameID]; ok {
		c.slots[index].data = data
		c.slots[index].used = true
		return
	}

	if len(c.slots) < c.maxFrames {
		c.items[frameID] = len(c.slots)
		c.slots = append(c.slots, clockSlot{frameID: frameID, data: data})
		return
	}

	for {
		slot := &c.slots[c.hand]
		if slot.used {
			slot.used = false
			c.advance()
			continue
		}

		delete(c.items, slot.frameID)
		*slot = clockSlot{frameID: frameID, data: data}
		c.items[frameID] = c.hand
		c.advance()
		return
	}
}

func (c *clockCache) Clear() {
	c.hand = 0
	c.items = make(map[int64]int)
	c.slots = nil
}

func (c *clockCache) advance() {
	c.hand = (c.hand + 1) % len(c.slots)
}

func ExampleCache_customReplacementPolicy() {
	cache := newClockCache(2)

	first := int64(1)
	second := int64(2)
	third := int64(3)

	cache.Put(first, []byte("first"))
	cache.Put(second, []byte("second"))

	// Mark first as used so the custom cache evicts second.
	_, _ = cache.Get(first)
	cache.Put(third, []byte("third"))

	printFrame("first", cache, first)
	printFrame("second", cache, second)
	printFrame("third", cache, third)

	// Output:
	// first: first
	// second: miss
	// third: third
}

func printFrame(label string, cache framecache.Cache, frameID int64) {
	data, ok := cache.Get(frameID)
	if !ok {
		fmt.Println(label + ": miss")
		return
	}
	fmt.Println(label + ": " + string(data))
}
