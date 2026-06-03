//go:build go1.18
// +build go1.18

package seekable

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/framecache"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func FuzzReaderFrameCacheConcurrent(f *testing.F) {
	f.Add(int64(1), uint8(4), uint8(16), uint8(0), uint8(2), []byte{0, 4, 4, 8, 8, 16})
	f.Add(int64(2), uint8(8), uint8(32), uint8(1), uint8(4), []byte{3, 5, 7, 11, 13, 17})
	f.Add(int64(3), uint8(16), uint8(64), uint8(2), uint8(0), []byte{1, 0, 2, 1, 3, 2})

	f.Fuzz(func(t *testing.T, seed int64, frameCount uint8, maxFrameSize uint8, strategy uint8, maxFrames uint8, rawOps []byte) {
		if len(rawOps) > 16 {
			rawOps = rawOps[:16]
		}
		if len(rawOps) < 2 {
			rawOps = []byte{0, 1}
		}

		compressed, source := fuzzReaderCacheStream(t, seed, int(frameCount%8)+1, int(maxFrameSize%32)+1)
		dec, err := zstd.NewReader(nil)
		require.NoError(t, err)
		defer dec.Close()

		r, err := NewReader(bytes.NewReader(compressed), dec,
			WithReaderFrameCache(fuzzReaderCache(strategy, int(maxFrames%4))))
		require.NoError(t, err)
		defer func() { require.NoError(t, r.Close()) }()

		var wg sync.WaitGroup
		errCh := make(chan error, len(rawOps)/2+1)
		for i := 0; i+1 < len(rawOps); i += 2 {
			off := int(rawOps[i]) % len(source)
			size := int(rawOps[i+1]) % 64
			wg.Add(1)
			go func() {
				defer wg.Done()
				got := make([]byte, size)
				n, err := r.ReadAt(got, int64(off))
				if err != nil && !errors.Is(err, io.EOF) {
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

func fuzzReaderCache(strategy uint8, maxFrames int) framecache.Cache {
	limits := framecache.Limits{MaxFrames: maxFrames}
	switch strategy % 3 {
	case 0:
		return framecache.NewFIFO(limits)
	case 1:
		return framecache.NewLRU(limits)
	default:
		return framecache.NewSieve(limits)
	}
}

func fuzzReaderCacheStream(t testing.TB, seed int64, frameCount, maxFrameSize int) ([]byte, []byte) {
	t.Helper()

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	require.NoError(t, err)
	defer func() { require.NoError(t, enc.Close()) }()

	var compressed bytes.Buffer
	w, err := NewWriter(&compressed, enc)
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(seed))
	var source []byte
	for i := 0; i < frameCount; i++ {
		frame := make([]byte, rng.Intn(maxFrameSize)+1)
		_, err := rng.Read(frame)
		require.NoError(t, err)
		source = append(source, frame...)
		_, err = w.Write(frame)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	return compressed.Bytes(), source
}
