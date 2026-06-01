package seekable

import (
	"sort"
)

type frameIndex struct {
	entries []FrameOffsetEntry
	size    int64
}

func (i frameIndex) numFrames() int64 {
	return int64(len(i.entries))
}

func (i frameIndex) entryByDecompressedOffset(off uint64) (FrameOffsetEntry, bool) {
	if off >= uint64(i.size) {
		return FrameOffsetEntry{}, false
	}

	// Find the first frame whose decompressed range contains off; this skips
	// zero-size entries that share an offset with a following non-empty frame.
	n := sort.Search(len(i.entries), func(n int) bool {
		return i.entries[n].DecompOffset+uint64(i.entries[n].DecompSize) > off
	})
	if n == len(i.entries) || i.entries[n].DecompOffset > off {
		return FrameOffsetEntry{}, false
	}
	return i.entries[n], true
}

func (i frameIndex) entryByID(id int64) (FrameOffsetEntry, bool) {
	if id < 0 || id >= int64(len(i.entries)) {
		return FrameOffsetEntry{}, false
	}

	return i.entries[int(id)], true
}
