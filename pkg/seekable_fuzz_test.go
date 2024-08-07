//go:build go1.18
// +build go1.18

package seekable

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math/rand"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func FuzzRoundTrip(f *testing.F) {
	dec, err := zstd.NewReader(nil)
	require.NoError(f, err)
	defer dec.Close()

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	require.NoError(f, err)
	defer func() { require.NoError(f, enc.Close()) }()

	f.Add(int64(1), uint8(0), int16(1), int8(io.SeekStart))
	f.Add(int64(10), uint8(1), int16(2), int8(io.SeekEnd))
	f.Add(int64(111), uint8(2), int16(3), int8(io.SeekCurrent))

	f.Fuzz(func(t *testing.T, seed int64, frames uint8, l int16, whence int8) {
		var b bytes.Buffer
		bufWriter := bufio.NewWriter(&b)

		w, err := NewWriter(bufWriter, enc)
		require.NoError(t, err)

		total := int16(0)
		rng := rand.New(rand.NewSource(seed))
		for i := 0; i < int(frames); i++ {
			sz := rng.Int63n(100)
			total += int16(sz)

			rndBuf := make([]byte, sz)

			_, err := rng.Read(rndBuf)
			require.NoError(t, err)

			_, err = w.Write(rndBuf)
			require.NoError(t, err)
		}
		err = w.Close()
		require.NoError(t, err)

		err = bufWriter.Flush()
		require.NoError(t, err)

		r, err := NewReader(bytes.NewReader(b.Bytes()), dec)
		require.NoError(t, err)
		defer func() { require.NoError(t, r.Close()) }()

		off := rng.Int63n(1+4*int64(total)) - 2*int64(total)
		i, err := r.Seek(off, int(whence))
		if err != nil {
			return
		}

		if l > total || l < 0 {
			l = total
		}
		buf1 := make([]byte, l)

		n, err := r.Read(buf1)
		if err != nil && !errors.Is(err, io.EOF) {
			return
		}

		buf2 := make([]byte, n)
		m, err := r.ReadAt(buf2, i)
		// t.Logf("off: %d, l: %d, whence: %d, i: %d, n: %d, m: %d", off, l, whence, i, n, m)

		if !errors.Is(err, io.EOF) {
			require.NoError(t, err)
		}

		assert.Equal(t, m, n)
		assert.Equal(t, buf1[:n], buf2)
	})
}
