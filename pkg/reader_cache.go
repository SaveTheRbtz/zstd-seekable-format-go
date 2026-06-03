package seekable

import "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/framecache"

func defaultFrameCache() framecache.Cache {
	return framecache.NewFIFO(framecache.Limits{MaxFrames: 1})
}

func (r *Reader) getCachedFrame(key framecache.Key) ([]byte, bool) {
	r.frameCacheMu.Lock()
	defer r.frameCacheMu.Unlock()

	return r.frameCache.Get(key)
}

func (r *Reader) putCachedFrame(key framecache.Key, data []byte) {
	r.frameCacheMu.Lock()
	defer r.frameCacheMu.Unlock()

	r.frameCache.Put(key, data)
}

func (r *Reader) dropFrameCache() {
	r.frameCacheMu.Lock()
	defer r.frameCacheMu.Unlock()

	r.frameCache = nil
}
