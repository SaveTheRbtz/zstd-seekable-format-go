package seekable

import (
	"bytes"
	"math"
	"math/rand"
	"testing"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/framecache"
	"github.com/klauspost/compress/zstd"
)

var (
	benchmarkReaderCacheN    int
	benchmarkReaderCacheByte byte
	benchmarkReaderCacheErr  error
)

func BenchmarkReaderFrameCache(b *testing.B) {
	const (
		frameCount  = 256
		accessCount = 4096
	)

	compressed, frames, _ := cacheTestStream(b, frameCount)
	offsets := frameOffsets(frames)
	distributions := []struct {
		name string
		seq  []int
	}{
		{name: "Random", seq: readerCacheRandomAccesses(frameCount, accessCount, 1)},
		{name: "Zipf", seq: readerCacheZipfAccesses(frameCount, accessCount, 2)},
		{name: "Normal", seq: readerCacheNormalAccesses(frameCount, accessCount, 3)},
	}
	caches := []struct {
		name string
		new  func() framecache.Cache
	}{
		{name: "FIFO0", new: func() framecache.Cache { return framecache.NewFIFO(framecache.Limits{MaxFrames: 0}) }},
		{name: "FIFO1", new: func() framecache.Cache { return framecache.NewFIFO(framecache.Limits{MaxFrames: 1}) }},
		{name: "FIFO64", new: func() framecache.Cache { return framecache.NewFIFO(framecache.Limits{MaxFrames: 64}) }},
		{name: "LRU64", new: func() framecache.Cache { return framecache.NewLRU(framecache.Limits{MaxFrames: 64}) }},
		{name: "Sieve64", new: func() framecache.Cache { return framecache.NewSieve(framecache.Limits{MaxFrames: 64}) }},
	}

	for _, dist := range distributions {
		for _, cache := range caches {
			b.Run(dist.name+"/"+cache.name, func(b *testing.B) {
				dec, err := zstd.NewReader(nil)
				if err != nil {
					b.Fatal(err)
				}
				defer dec.Close()

				r, err := NewReader(bytes.NewReader(compressed), dec, WithReaderFrameCache(cache.new()))
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
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					frameID := dist.seq[i%len(dist.seq)]
					dst := buf[:len(frames[frameID])]
					n, err := r.ReadAt(dst, offsets[frameID])
					if err != nil {
						benchmarkReaderCacheErr = err
						b.Fatal(err)
					}
					total += n
					if n > 0 {
						sink ^= dst[0]
					}
				}
				benchmarkReaderCacheN = total
				benchmarkReaderCacheByte = sink
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
