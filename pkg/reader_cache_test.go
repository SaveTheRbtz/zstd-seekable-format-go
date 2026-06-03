package seekable

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/framecache"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type countingDecoder struct {
	dec ZSTDDecoder
	n   int
}

func (d *countingDecoder) DecodeAll(input, dst []byte) ([]byte, error) {
	d.n++
	return d.dec.DecodeAll(input, dst)
}

func (d *countingDecoder) Count() int {
	return d.n
}

type spyFrameCache struct {
	items  map[int64][]byte
	gets   int
	puts   int
	clears int
}

func newSpyFrameCache() *spyFrameCache {
	return &spyFrameCache{items: make(map[int64][]byte)}
}

func (c *spyFrameCache) Get(frameID int64) ([]byte, bool) {
	c.gets++
	data, ok := c.items[frameID]
	return data, ok
}

func (c *spyFrameCache) Put(frameID int64, data []byte) {
	c.puts++
	c.items[frameID] = data
}

func (c *spyFrameCache) Clear() {
	c.clears++
	clear(c.items)
}

func (c *spyFrameCache) Counts() (int, int) {
	return c.gets, c.puts
}

func TestReaderDefaultFrameCacheKeepsOneDecodedFrame(t *testing.T) {
	t.Parallel()

	compressed, frames, _ := cacheTestStream(t, 2)
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()
	counting := &countingDecoder{dec: dec}

	r, err := NewReader(bytes.NewReader(compressed), counting)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()

	assertReadAt(t, r, 0, frames[0])
	assert.Equal(t, 1, counting.Count())

	assertReadAt(t, r, 0, frames[0])
	assert.Equal(t, 1, counting.Count())

	assertReadAt(t, r, int64(len(frames[0])), frames[1])
	assert.Equal(t, 2, counting.Count())

	assertReadAt(t, r, 0, frames[0])
	assert.Equal(t, 3, counting.Count())
}

func TestReaderFrameCacheOptionUsesCallerCache(t *testing.T) {
	t.Parallel()

	compressed, frames, _ := cacheTestStream(t, 1)
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()
	counting := &countingDecoder{dec: dec}
	cache := newSpyFrameCache()
	cache.items[0] = []byte("stale frame")

	r, err := NewReader(bytes.NewReader(compressed), counting, WithReaderFrameCache(cache))
	require.NoError(t, err)
	assert.Equal(t, 1, cache.clears)

	assertReadAt(t, r, 0, frames[0])
	assertReadAt(t, r, 0, frames[0])
	assert.Equal(t, 1, counting.Count())

	gets, puts := cache.Counts()
	assert.Equal(t, 2, gets)
	assert.Equal(t, 1, puts)

	require.NoError(t, r.Close())
	gets, puts = cache.Counts()
	assert.Equal(t, 2, gets)
	assert.Equal(t, 1, puts)
	assert.Equal(t, 2, cache.clears)
}

func TestReaderNoStorageFrameCacheDisablesCaching(t *testing.T) {
	t.Parallel()

	compressed, frames, _ := cacheTestStream(t, 1)
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()
	counting := &countingDecoder{dec: dec}

	r, err := NewReader(bytes.NewReader(compressed), counting,
		WithReaderFrameCache(framecache.NewFIFO(framecache.Limits{MaxFrames: 0})))
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()

	for i := 1; i <= 2; i++ {
		assertReadAt(t, r, 0, frames[0])
		assert.Equal(t, i, counting.Count())
	}
}

func TestReaderFrameCacheConcurrentReadAt(t *testing.T) {
	t.Parallel()

	compressed, frames, source := cacheTestStream(t, 8)
	caches := []struct {
		name  string
		cache framecache.Cache
	}{
		{name: "FIFO", cache: framecache.NewFIFO(framecache.Limits{MaxFrames: 4})},
		{name: "LRU", cache: framecache.NewLRU(framecache.Limits{MaxFrames: 4})},
		{name: "Sieve", cache: framecache.NewSieve(framecache.Limits{MaxFrames: 4})},
	}

	for _, tc := range caches {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dec, err := zstd.NewReader(nil)
			require.NoError(t, err)
			defer dec.Close()

			r, err := NewReader(bytes.NewReader(compressed), dec, WithReaderFrameCache(tc.cache))
			require.NoError(t, err)
			defer func() { require.NoError(t, r.Close()) }()

			const workers = 64
			var g errgroup.Group
			for i := 0; i < workers; i++ {
				g.Go(func() error {
					off := i % len(source)
					size := len(frames[i%len(frames)])
					if off+size > len(source) {
						size = len(source) - off
					}
					got := make([]byte, size)
					n, err := r.ReadAt(got, int64(off))
					if err != nil && err != io.EOF {
						return err
					}
					if !bytes.Equal(got[:n], source[off:off+n]) {
						return fmt.Errorf("ReadAt(%d, %d) = %q, want %q", off, size, got[:n], source[off:off+n])
					}
					return nil
				})
			}
			require.NoError(t, g.Wait())
		})
	}
}

func assertReadAt(t testing.TB, r *Reader, off int64, want []byte) {
	t.Helper()

	got := make([]byte, len(want))
	n, err := r.ReadAt(got, off)
	require.NoError(t, err)
	require.Equal(t, len(want), n)
	require.Equal(t, want, got[:n])
}

func cacheTestStream(t testing.TB, frameCount int) ([]byte, [][]byte, []byte) {
	t.Helper()

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	require.NoError(t, err)
	defer func() { require.NoError(t, enc.Close()) }()

	var compressed bytes.Buffer
	w, err := NewWriter(&compressed, enc)
	require.NoError(t, err)

	frames := make([][]byte, frameCount)
	var source []byte
	for i := range frames {
		frame := bytes.Repeat([]byte{byte('a' + i%26)}, i%17+8)
		frames[i] = frame
		source = append(source, frame...)
		_, err := w.Write(frame)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	return compressed.Bytes(), frames, source
}
