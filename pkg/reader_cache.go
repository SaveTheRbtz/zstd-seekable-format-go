package seekable

import "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/framecache"

func defaultFrameCache() framecache.Cache {
	return framecache.NewFIFO(framecache.Limits{MaxFrames: 1})
}
