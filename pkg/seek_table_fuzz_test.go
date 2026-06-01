//go:build go1.18
// +build go1.18

package seekable

import (
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func FuzzCorruptSeekTable(f *testing.F) {
	const noChecksumSeekTableOffset = 35

	base := noChecksum[noChecksumSeekTableOffset:]

	for _, seed := range []struct {
		mode uint8
		off  int64
	}{
		{mode: 0, off: 0},
		{mode: 1, off: -1},
		{mode: 2, off: 1},
		{mode: 3, off: 8},
	} {
		f.Add(base, seed.mode, seed.off)
	}

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

		table, err := NewSeekTable(mutated)
		if err != nil {
			return
		}

		_ = table.Size()
		_ = table.NumFrames()
		_, _ = table.EntryByDecompressedOffset(uint64(off))
		_, _ = table.EntryByID(off)

		stream := append(append([]byte(nil), noChecksum[:noChecksumSeekTableOffset]...), mutated...)
		sr := &seekableBufferReaderAt{buf: stream}

		dec, err := zstd.NewReader(nil)
		require.NoError(t, err)
		defer dec.Close()

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
