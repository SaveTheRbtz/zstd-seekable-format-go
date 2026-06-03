package seekable

import "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/framecache"

func defaultFrameCache() framecache.Cache {
	return framecache.NewFIFO(framecache.Limits{MaxFrames: 1})
}

func (r *Reader) getCachedFrame(frameID int64) ([]byte, bool) {
	r.frameCacheMu.Lock()
	defer r.frameCacheMu.Unlock()

	return r.frameCache.Get(frameID)
}

func (r *Reader) putCachedFrame(frameID int64, data []byte) {
	r.frameCacheMu.Lock()
	defer r.frameCacheMu.Unlock()

	r.frameCache.Put(frameID, data)
}

func (r *Reader) clearFrameCache() {
	r.frameCacheMu.Lock()
	defer r.frameCacheMu.Unlock()

	if r.frameCache != nil {
		r.frameCache.Clear()
	}
}
