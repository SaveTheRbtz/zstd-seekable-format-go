//go:build go1.18
// +build go1.18

package seekable

import (
	"errors"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func FuzzReader(f *testing.F) {
	f.Add(noChecksum, int64(0), uint8(1), io.SeekStart)
	f.Add(checksum, int64(-1), uint8(2), io.SeekEnd)
	f.Add(checksum, int64(1), uint8(0), io.SeekCurrent)

	f.Fuzz(func(t *testing.T, in []byte, off int64, l uint8, whence int) {
		dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(1<<24))
		require.NoError(t, err)
		defer dec.Close()

		sr := &seekableBufferReaderAt{buf: in}
		r, err := NewReader(sr, dec)
		if err != nil {
			return
		}
		defer func() { require.NoError(t, r.Close()) }()

		i, err := r.Seek(off, whence)
		if err != nil {
			return
		}

		buf1 := make([]byte, l)
		n, err := r.Read(buf1)
		if err != nil && !errors.Is(err, io.EOF) {
			return
		}

		buf2 := make([]byte, n)
		m, err := r.ReadAt(buf2, i)

		if !errors.Is(err, io.EOF) {
			require.NoError(t, err)
		}

		assert.Equal(t, n, m)
		assert.Equal(t, buf1[:n], buf2)
	})
}

func FuzzReaderConst(f *testing.F) {
	f.Add(int64(0), uint8(1), int8(io.SeekStart))

	f.Fuzz(func(t *testing.T, off int64, l uint8, whence int8) {
		dec, err := zstd.NewReader(nil)
		require.NoError(t, err)
		defer dec.Close()

		sr := &seekableBufferReaderAt{buf: checksum}
		r, err := NewReader(sr, dec)
		require.NoError(t, err)
		defer func() { require.NoError(t, r.Close()) }()

		i, err := r.Seek(off, int(whence))
		if err != nil {
			return
		}

		buf1 := make([]byte, l)
		n, err := r.Read(buf1)
		if err != nil && !errors.Is(err, io.EOF) {
			return
		}

		buf2 := make([]byte, n)
		m, err := r.ReadAt(buf2, i)

		if !errors.Is(err, io.EOF) {
			require.NoError(t, err)
		}

		assert.Equal(t, n, m)
		assert.Equal(t, buf1[:n], buf2)

		if n > 0 {
			assert.Equal(t, sourceString[i:i+int64(n)], string(buf2))
		}
	})
}
