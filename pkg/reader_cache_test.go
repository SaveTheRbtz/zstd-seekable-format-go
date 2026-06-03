package seekable

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/framecache"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingDecoder struct {
	dec ZSTDDecoder
	mu  sync.Mutex
	n   int
}

func (d *countingDecoder) DecodeAll(input, dst []byte) ([]byte, error) {
	d.mu.Lock()
	d.n++
	d.mu.Unlock()
	return d.dec.DecodeAll(input, dst)
}

func (d *countingDecoder) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.n
}

type spyFrameCache struct {
	mu     sync.Mutex
	items  map[int64][]byte
	gets   int
	puts   int
	clears int
}

func newSpyFrameCache() *spyFrameCache {
	return &spyFrameCache{items: make(map[int64][]byte)}
}

func (c *spyFrameCache) Get(frameID int64) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.gets++
	data, ok := c.items[frameID]
	return data, ok
}

func (c *spyFrameCache) Put(frameID int64, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.puts++
	c.items[frameID] = data
}

func (c *spyFrameCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.clears++
	clear(c.items)
}

func (c *spyFrameCache) Counts() (int, int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets, c.puts, c.clears
}

func TestReaderDefaultFrameCacheIsOneFrameFIFO(t *testing.T) {
	t.Parallel()

	compressed, frames, _ := cacheTestStream(t, 2)
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()
	counting := &countingDecoder{dec: dec}

	r, err := NewReader(bytes.NewReader(compressed), counting)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()

	buf := make([]byte, len(frames[0]))
	n, err := r.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, frames[0], buf)
	assert.Equal(t, 1, counting.Count())

	n, err = r.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, frames[0], buf)
	assert.Equal(t, 1, counting.Count())

	buf = make([]byte, len(frames[1]))
	n, err = r.ReadAt(buf, int64(len(frames[0])))
	require.NoError(t, err)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, frames[1], buf)
	assert.Equal(t, 2, counting.Count())

	buf = make([]byte, len(frames[0]))
	n, err = r.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, frames[0], buf)
	assert.Equal(t, 3, counting.Count())
}

func TestReaderFrameCacheOption(t *testing.T) {
	t.Parallel()

	compressed, frames, _ := cacheTestStream(t, 1)
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()
	counting := &countingDecoder{dec: dec}
	cache := newSpyFrameCache()

	r, err := NewReader(bytes.NewReader(compressed), counting, WithReaderFrameCache(cache))
	require.NoError(t, err)

	buf := make([]byte, len(frames[0]))
	n, err := r.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, frames[0], buf)

	n, err = r.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, frames[0], buf)
	assert.Equal(t, 1, counting.Count())

	gets, puts, clears := cache.Counts()
	assert.Equal(t, 2, gets)
	assert.Equal(t, 1, puts)
	assert.Equal(t, 0, clears)

	require.NoError(t, r.Close())
	_, _, clears = cache.Counts()
	assert.Equal(t, 1, clears)
}

func TestReaderFIFOZeroDisablesFrameCache(t *testing.T) {
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

	buf := make([]byte, len(frames[0]))
	for i := 1; i <= 2; i++ {
		n, err := r.ReadAt(buf, 0)
		require.NoError(t, err)
		assert.Equal(t, len(buf), n)
		assert.Equal(t, frames[0], buf)
		assert.Equal(t, i, counting.Count())
	}
}

func TestReaderFrameCacheConcurrent(t *testing.T) {
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dec, err := zstd.NewReader(nil)
			require.NoError(t, err)
			defer dec.Close()

			r, err := NewReader(bytes.NewReader(compressed), dec, WithReaderFrameCache(tc.cache))
			require.NoError(t, err)
			defer func() { require.NoError(t, r.Close()) }()

			const workers = 64
			var wg sync.WaitGroup
			errCh := make(chan error, workers)
			for i := 0; i < workers; i++ {
				i := i
				wg.Add(1)
				go func() {
					defer wg.Done()
					off := i % len(source)
					size := len(frames[i%len(frames)])
					if off+size > len(source) {
						size = len(source) - off
					}
					got := make([]byte, size)
					n, err := r.ReadAt(got, int64(off))
					if err != nil && err != io.EOF {
						errCh <- err
						return
					}
					if !bytes.Equal(got[:n], source[off:off+n]) {
						errCh <- fmt.Errorf("ReadAt(%d, %d) = %q, want %q", off, size, got[:n], source[off:off+n])
					}
				}()
			}
			wg.Wait()
			close(errCh)
			for err := range errCh {
				require.NoError(t, err)
			}
		})
	}
}

func cacheTestStream(t testing.TB, frameCount int) ([]byte, [][]byte, []byte) {
	t.Helper()

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	require.NoError(t, err)
	defer enc.Close()

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
