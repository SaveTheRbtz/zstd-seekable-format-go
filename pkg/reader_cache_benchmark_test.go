package seekable

import (
	"bytes"
	"math"
	"math/rand"
	"runtime"
	"testing"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/framecache"
	"github.com/klauspost/compress/zstd"
)

type benchmarkCountingCache struct {
	cache  framecache.Cache
	hits   uint64
	misses uint64
}

func (c *benchmarkCountingCache) Get(frameID int64) ([]byte, bool) {
	data, ok := c.cache.Get(frameID)
	if ok {
		c.hits++
	} else {
		c.misses++
	}
	return data, ok
}

func (c *benchmarkCountingCache) Put(frameID int64, data []byte) {
	c.cache.Put(frameID, data)
}

func (c *benchmarkCountingCache) Clear() {
	c.cache.Clear()
}

func (c *benchmarkCountingCache) HitRate() float64 {
	total := c.hits + c.misses
	if total == 0 {
		return 0
	}
	return float64(c.hits) / float64(total)
}

func BenchmarkReaderFrameCache(b *testing.B) {
	const (
		frameCount  = 256_000
		accessCount = 4_096_000
		cacheFrames = 10_000
	)

	compressed, frames, _ := cacheTestStream(b, frameCount)
	offsets := frameOffsets(frames)
	distributions := []struct {
		name   string
		newSeq func() []int
	}{
		{name: "Uniform", newSeq: func() []int {
			return readerCacheRandomAccesses(frameCount, accessCount, 1)
		}},
		{name: "Zipf_s=1.2_v=1", newSeq: func() []int {
			return readerCacheZipfAccesses(frameCount, accessCount, 2)
		}},
		{name: "Normal_mu=mid_sigma=n/6", newSeq: func() []int {
			return readerCacheNormalAccesses(frameCount, accessCount, 3)
		}},
	}
	caches := []struct {
		name string
		new  func() *benchmarkCountingCache
	}{
		{name: "FIFO_MaxFrames=10000", new: func() *benchmarkCountingCache {
			return &benchmarkCountingCache{cache: framecache.NewFIFO(framecache.Limits{MaxFrames: cacheFrames})}
		}},
		{name: "LRU_MaxFrames=10000", new: func() *benchmarkCountingCache {
			return &benchmarkCountingCache{cache: framecache.NewLRU(framecache.Limits{MaxFrames: cacheFrames})}
		}},
		{name: "Sieve_MaxFrames=10000", new: func() *benchmarkCountingCache {
			return &benchmarkCountingCache{cache: framecache.NewSieve(framecache.Limits{MaxFrames: cacheFrames})}
		}},
	}

	for _, dist := range distributions {
		seq := dist.newSeq()
		for _, cache := range caches {
			b.Run(dist.name+"/"+cache.name, func(b *testing.B) {
				dec, err := zstd.NewReader(nil)
				if err != nil {
					b.Fatal(err)
				}
				defer dec.Close()

				countingCache := cache.new()
				r, err := NewReader(bytes.NewReader(compressed), dec, WithReaderFrameCache(countingCache))
				if err != nil {
					b.Fatal(err)
				}
				defer func() {
					if err := r.Close(); err != nil {
						b.Fatal(err)
					}
				}()

				buf := make([]byte, maxFrameLen(frames))
				var total int
				var sink byte

				var i int
				for b.Loop() {
					frameID := seq[i%len(seq)]
					i++
					dst := buf[:len(frames[frameID])]
					n, err := r.ReadAt(dst, offsets[frameID])
					if err != nil {
						b.Fatal(err)
					}
					total += n
					if n > 0 {
						sink ^= dst[0]
					}
				}

				runtime.KeepAlive(total)
				runtime.KeepAlive(sink)
				b.ReportMetric(countingCache.HitRate()*100, "cache_hit_percent")
			})
		}
	}
}

func frameOffsets(frames [][]byte) []int64 {
	offsets := make([]int64, len(frames))
	var off int64
	for i, frame := range frames {
		offsets[i] = off
		off += int64(len(frame))
	}
	return offsets
}

func maxFrameLen(frames [][]byte) int {
	var max int
	for _, frame := range frames {
		if len(frame) > max {
			max = len(frame)
		}
	}
	return max
}

func readerCacheRandomAccesses(frameCount, accessCount int, seed int64) []int {
	rng := rand.New(rand.NewSource(seed))
	seq := make([]int, accessCount)
	for i := range seq {
		seq[i] = rng.Intn(frameCount)
	}
	return seq
}

func readerCacheZipfAccesses(frameCount, accessCount int, seed int64) []int {
	rng := rand.New(rand.NewSource(seed))
	zipf := rand.NewZipf(rng, 1.2, 1, uint64(frameCount-1))
	seq := make([]int, accessCount)
	for i := range seq {
		seq[i] = int(zipf.Uint64())
	}
	return seq
}

func readerCacheNormalAccesses(frameCount, accessCount int, seed int64) []int {
	rng := rand.New(rand.NewSource(seed))
	mean := float64(frameCount-1) / 2
	stddev := math.Max(1, float64(frameCount)/6)
	seq := make([]int, accessCount)
	for i := range seq {
		v := int(math.Round(rng.NormFloat64()*stddev + mean))
		if v < 0 {
			v = 0
		}
		if v >= frameCount {
			v = frameCount - 1
		}
		seq[i] = v
	}
	return seq
}
