package seekable

import (
	"sync"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/framecache"
)

type readerFrameCache struct {
	mu    sync.Mutex
	cache framecache.Cache
}

type evictingFrameCache interface {
	PutWithEvicted(frameID int64, data []byte) ([]byte, bool)
}

func defaultFrameCache() framecache.Cache {
	return framecache.NewFIFO(framecache.Limits{MaxFrames: 1})
}

func newReaderFrameCache(cache framecache.Cache) *readerFrameCache {
	if cache == nil {
		cache = defaultFrameCache()
	}
	return &readerFrameCache{cache: cache}
}

func (c *readerFrameCache) Get(frameID int64) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cache.Get(frameID)
}

func (c *readerFrameCache) Copy(frameID int64, dst []byte, offset, size uint64, expectedLen int) (int, bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, ok := c.cache.Get(frameID)
	if !ok {
		return 0, false, true
	}
	if len(data) != expectedLen {
		return len(data), true, false
	}
	return copy(dst, data[offset:offset+size]), true, true
}

func (c *readerFrameCache) Put(frameID int64, data []byte) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cache, ok := c.cache.(evictingFrameCache); ok {
		return cache.PutWithEvicted(frameID, data)
	}
	c.cache.Put(frameID, data)
	return nil, true
}

func (c *readerFrameCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache.Clear()
}
