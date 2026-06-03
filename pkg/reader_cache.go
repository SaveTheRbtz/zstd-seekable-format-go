package seekable

import (
	"sync"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/framecache"
)

type readerFrameCache struct {
	mu    sync.Mutex
	cache framecache.Cache
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

func (c *readerFrameCache) Put(frameID int64, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache.Put(frameID, data)
}

func (c *readerFrameCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache.Clear()
}
