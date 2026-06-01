package seekable

import (
	"sort"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/env"
)

type frameIndex struct {
	entries []env.FrameOffsetEntry
	size    int64
}

func (i frameIndex) numFrames() int64 {
	return int64(len(i.entries))
}

func (i frameIndex) byDecompOffset(off uint64) (found *env.FrameOffsetEntry) {
	if off >= uint64(i.size) {
		return nil
	}

	// Find the first frame whose decompressed range contains off; this skips
	// zero-size entries that share an offset with a following non-empty frame.
	n := sort.Search(len(i.entries), func(n int) bool {
		return i.entries[n].DecompOffset+uint64(i.entries[n].DecompSize) > off
	})
	if n == len(i.entries) || i.entries[n].DecompOffset > off {
		return nil
	}
	return &i.entries[n]
}

func (i frameIndex) byID(id int64) (found *env.FrameOffsetEntry) {
	if id < 0 || id >= int64(len(i.entries)) {
		return nil
	}

	return &i.entries[int(id)]
}
