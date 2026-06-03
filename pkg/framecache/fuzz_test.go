//go:build go1.18
// +build go1.18

package framecache

import (
	"bytes"
	"fmt"
	"testing"
)

func FuzzFrameCacheAccessPatterns(f *testing.F) {
	// Seed tuple: rawStrategy, rawMaxFrames, rawMaxBytes, rawFrameCount, rawAccesses.
	f.Add(uint8(0), uint8(2), uint16(0), uint8(4), []byte{0, 1, 2, 1})
	f.Add(uint8(1), uint8(4), uint16(32), uint8(8), []byte{0, 0, 1, 2, 3})
	f.Add(uint8(2), uint8(8), uint16(64), uint8(16), []byte{15, 7, 3, 1, 0})

	f.Fuzz(func(t *testing.T, rawStrategy uint8, rawMaxFrames uint8, rawMaxBytes uint16, rawFrameCount uint8, rawAccesses []byte) {
		frameCount := int(rawFrameCount%32) + 1
		if len(rawAccesses) > 256 {
			rawAccesses = rawAccesses[:256]
		}
		if len(rawAccesses) == 0 {
			rawAccesses = []byte{0}
		}

		frames := testFrames(frameCount)
		limits := Limits{
			MaxFrames: int(rawMaxFrames%16) + 1,
			MaxBytes:  uint64(rawMaxBytes),
		}
		strategy, c := cacheByStrategy(rawStrategy, limits)

		for accessIndex, rawFrameID := range rawAccesses {
			frameID := int(rawFrameID) % frameCount
			want := frames[frameID]
			key := testKey(int64(frameID))
			got, ok := c.Get(key)
			if ok && !bytes.Equal(got, want) {
				t.Fatalf("Get(%d) = %q; want %q (strategy=%s limits=%+v frameCount=%d accessIndex=%d rawFrameID=%d)",
					frameID, got, want, strategy, limits, frameCount, accessIndex, rawFrameID)
			}
			c.Put(key, want)
			context := fmt.Sprintf("strategy=%s limits=%+v frameCount=%d accessIndex=%d rawFrameID=%d",
				strategy, limits, frameCount, accessIndex, rawFrameID)
			assertCacheInvariants(t, c, context)
		}
	})
}
