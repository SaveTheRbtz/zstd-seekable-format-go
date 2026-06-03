//go:build go1.18
// +build go1.18

package framecache

import (
	"bytes"
	"math/rand"
	"testing"
)

func FuzzFrameCacheAccessPatterns(f *testing.F) {
	f.Add(uint8(0), int8(2), uint16(0), int64(1), uint8(4), uint8(0), []byte{0, 1, 2, 1})
	f.Add(uint8(1), int8(4), uint16(32), int64(2), uint8(8), uint8(1), []byte{0, 0, 1, 2, 3})
	f.Add(uint8(2), int8(8), uint16(64), int64(3), uint8(16), uint8(2), []byte{15, 7, 3, 1, 0})

	f.Fuzz(func(t *testing.T, strategy uint8, maxFrames int8, maxBytes uint16, seed int64, frameCount uint8, distribution uint8, accesses []byte) {
		n := int(frameCount%32) + 1
		if len(accesses) > 256 {
			accesses = accesses[:256]
		}
		if len(accesses) == 0 {
			accesses = []byte{0}
		}

		frames := fuzzFrames(n, seed)
		seq := fuzzAccesses(n, len(accesses), seed, distribution, accesses)
		limits := Limits{
			MaxFrames: int(maxFrames),
			MaxBytes:  uint64(maxBytes),
		}
		_, c := cacheByStrategy(strategy, limits)

		for _, frameID := range seq {
			want := frames[frameID]
			got, ok := c.Get(int64(frameID))
			if ok && !bytes.Equal(got, want) {
				t.Fatalf("Get(%d) = %q; want %q", frameID, got, want)
			}
			c.Put(int64(frameID), want)
			assertCacheInvariants(t, c, limits)
		}
	})
}

func fuzzFrames(n int, seed int64) [][]byte {
	rng := rand.New(rand.NewSource(seed))
	frames := make([][]byte, n)
	for i := range frames {
		size := rng.Intn(64) + 1
		frames[i] = bytes.Repeat([]byte{byte(i), byte(size)}, size)
	}
	return frames
}

func fuzzAccesses(frameCount, accessCount int, seed int64, distribution uint8, raw []byte) []int {
	switch distribution % 3 {
	case 0:
		return fuzzRawAccesses(frameCount, raw)
	case 1:
		return randomAccesses(frameCount, accessCount, seed)
	default:
		return zipfAccesses(frameCount, accessCount, seed)
	}
}

func fuzzRawAccesses(frameCount int, raw []byte) []int {
	seq := make([]int, len(raw))
	for i, b := range raw {
		seq[i] = int(b) % frameCount
	}
	return seq
}
