package framecache_test

import (
	"fmt"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/framecache"
)

type clockCache struct {
	maxFrames int
	hand      int
	items     map[framecache.Key]int
	slots     []clockSlot
}

type clockSlot struct {
	key  framecache.Key
	data []byte
	used bool
}

var _ framecache.Cache = (*clockCache)(nil)

func newClockCache(maxFrames int) *clockCache {
	return &clockCache{
		maxFrames: maxFrames,
		items:     make(map[framecache.Key]int),
	}
}

func (c *clockCache) Get(key framecache.Key) ([]byte, bool) {
	index, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.slots[index].used = true
	return c.slots[index].data, true
}

func (c *clockCache) Put(key framecache.Key, data []byte) {
	if c.maxFrames <= 0 {
		return
	}

	if index, ok := c.items[key]; ok {
		c.slots[index].data = data
		c.slots[index].used = true
		return
	}

	if len(c.slots) < c.maxFrames {
		c.items[key] = len(c.slots)
		c.slots = append(c.slots, clockSlot{key: key, data: data})
		return
	}

	for {
		slot := &c.slots[c.hand]
		if slot.used {
			slot.used = false
			c.advance()
			continue
		}

		delete(c.items, slot.key)
		*slot = clockSlot{key: key, data: data}
		c.items[key] = c.hand
		c.advance()
		return
	}
}

func (c *clockCache) advance() {
	c.hand = (c.hand + 1) % len(c.slots)
}

func ExampleCache_customReplacementPolicy() {
	cache := framecache.NewSynchronized(newClockCache(2))

	first := framecache.NewKey(1, 1)
	second := framecache.NewKey(1, 2)
	third := framecache.NewKey(1, 3)

	cache.Put(first, []byte("first"))
	cache.Put(second, []byte("second"))

	// Touch first so CLOCK gives it a second chance during replacement.
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

func ExampleLimits_disabled() {
	cache := framecache.NewFIFO(framecache.Limits{MaxFrames: 0})
	cache.Put(framecache.NewKey(1, 1), []byte("decoded frame"))
	_, ok := cache.Get(framecache.NewKey(1, 1))
	fmt.Println(ok)

	// Output:
	// false
}

func printFrame(label string, cache framecache.Cache, key framecache.Key) {
	data, ok := cache.Get(key)
	if !ok {
		fmt.Println(label + ": miss")
		return
	}
	fmt.Println(label + ": " + string(data))
}
