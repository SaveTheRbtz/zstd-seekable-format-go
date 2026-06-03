//go:build go1.18
// +build go1.18

package framecache

import (
	"bytes"
	"testing"
)

func FuzzFrameCacheAccessPatterns(f *testing.F) {
	f.Add(uint8(0), uint8(2), uint16(0), uint8(4), []byte{0, 1, 2, 1})
	f.Add(uint8(1), uint8(4), uint16(32), uint8(8), []byte{0, 0, 1, 2, 3})
	f.Add(uint8(2), uint8(8), uint16(64), uint8(16), []byte{15, 7, 3, 1, 0})

	f.Fuzz(func(t *testing.T, strategy uint8, maxFrames uint8, maxBytes uint16, frameCount uint8, accesses []byte) {
		n := int(frameCount%32) + 1
		if len(accesses) > 256 {
			accesses = accesses[:256]
		}
		if len(accesses) == 0 {
			accesses = []byte{0}
		}

		frames := testFrames(n)
		limits := Limits{
			MaxFrames: int(maxFrames%16) + 1,
			MaxBytes:  uint64(maxBytes),
		}
		_, c := cacheByStrategy(strategy, limits)

		for _, rawFrameID := range accesses {
			frameID := int(rawFrameID) % n
			want := frames[frameID]
			key := testKey(int64(frameID))
			got, ok := c.Get(key)
			if ok && !bytes.Equal(got, want) {
				t.Fatalf("Get(%d) = %q; want %q", frameID, got, want)
			}
			c.Put(key, want)
			assertCacheInvariants(t, c)
		}
	})
}
