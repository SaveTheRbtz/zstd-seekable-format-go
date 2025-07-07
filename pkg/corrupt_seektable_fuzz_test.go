//go:build go1.18
// +build go1.18

package seekable

import (
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func FuzzCorruptSeekTable(f *testing.F) {
	dec, err := zstd.NewReader(nil)
	require.NoError(f, err)
	defer dec.Close()

	base := noChecksum[35:]

	f.Add(base, uint8(0), int64(0))
	f.Add(base, uint8(1), int64(-1))
	f.Add(base, uint8(2), int64(1))
	f.Add(base, uint8(3), int64(8))

	f.Fuzz(func(t *testing.T, in []byte, mode uint8, off int64) {
		mutated := make([]byte, len(base))
		copy(mutated, base)

		if len(mutated) == 0 {
			return
		}

		switch mode % 4 {
		case 0:
			for i := 0; i < len(in) && i < len(mutated); i++ {
				mutated[i] = in[i]
			}
		case 1:
			for i := 0; i < len(in) && i < len(mutated); i++ {
				mutated[len(mutated)-1-i] = in[i]
			}
		case 2:
			mutated = append(mutated, in...)
		case 3:
			if len(in) > 0 {
				n := int(in[0]) % len(mutated)
				mutated = mutated[:n]
			}
		}

		d, err := NewDecoder(mutated, dec)
		if err != nil {
			return
		}
		defer func() { require.NoError(t, d.Close()) }()

		_ = d.Size()
		_ = d.NumFrames()
		_ = d.GetIndexByDecompOffset(uint64(off))
		_ = d.GetIndexByID(off)

		stream := append(append([]byte(nil), noChecksum[:35]...), mutated...)
		sr := &seekableBufferReaderAt{buf: stream}
		r, err := NewReader(sr, dec)
		if err != nil {
			return
		}
		defer func() { require.NoError(t, r.Close()) }()

		buf := make([]byte, mode%101)
		_, _ = r.ReadAt(buf, off)
		_, _ = r.Read(buf)
		_, _ = r.Seek(off, int(mode%3))
	})
}
