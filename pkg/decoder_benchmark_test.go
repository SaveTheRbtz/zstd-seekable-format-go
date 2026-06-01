package seekable

import (
	"sync"
	"testing"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/env"
)

const benchmarkChunkIndexSize = 1 << 20

var (
	benchmarkLargeIndexOnce   sync.Once
	benchmarkLargeIndexReader *readerImpl
	benchmarkLargeIndexErr    error

	benchmarkDecoderSink Decoder
	benchmarkEntrySink   *env.FrameOffsetEntry
	benchmarkIntSink     int64
)

func benchmarkLargeSeekTable(b testing.TB) []byte {
	b.Helper()

	const entrySize = 12
	seekTable := make([]byte, benchmarkChunkIndexSize*entrySize+seekTableFooterOffset)
	entry := seekTableEntry{CompressedSize: 1, DecompressedSize: 1}
	for i := 0; i < benchmarkChunkIndexSize; i++ {
		entry.marshalBinaryInline(seekTable[i*entrySize : (i+1)*entrySize])
	}

	footer := seekTableFooter{
		NumberOfFrames: benchmarkChunkIndexSize,
		SeekTableDescriptor: seekTableDescriptor{
			ChecksumFlag: true,
		},
		SeekableMagicNumber: seekableMagicNumber,
	}
	footer.marshalBinaryInline(seekTable[benchmarkChunkIndexSize*entrySize:])

	frame, err := createSkippableFrame(seekableTag, seekTable)
	if err != nil {
		b.Fatal(err)
	}
	return frame
}

func benchmarkLargeReader(b *testing.B) *readerImpl {
	b.Helper()

	benchmarkLargeIndexOnce.Do(func() {
		seekTable := benchmarkLargeSeekTable(b)
		d, err := NewDecoder(seekTable, nil)
		benchmarkLargeIndexErr = err
		if err != nil {
			return
		}
		benchmarkLargeIndexReader = d.(*readerImpl)
	})
	if benchmarkLargeIndexErr != nil {
		b.Fatal(benchmarkLargeIndexErr)
	}
	return benchmarkLargeIndexReader
}

func BenchmarkDecoderLargeIndexBuild(b *testing.B) {
	seekTable := benchmarkLargeSeekTable(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d, err := NewDecoder(seekTable, nil)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkDecoderSink = d
		if err := d.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecoderLargeIndexGetIndexByDecompOffset(b *testing.B) {
	r := benchmarkLargeReader(b)

	cases := []struct {
		name string
		off  uint64
	}{
		{name: "First", off: 0},
		{name: "Middle", off: benchmarkChunkIndexSize / 2},
		{name: "Last", off: benchmarkChunkIndexSize - 1},
		{name: "MissPastEnd", off: benchmarkChunkIndexSize},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkEntrySink = r.GetIndexByDecompOffset(tc.off)
			}
		})
	}

	b.Run("Sequential", func(b *testing.B) {
		var ids int64
		mask := uint64(benchmarkChunkIndexSize - 1)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			index := r.GetIndexByDecompOffset(uint64(i) & mask)
			if index != nil {
				ids += index.ID
			}
		}
		benchmarkIntSink = ids
	})

	b.Run("PseudoRandom", func(b *testing.B) {
		var ids int64
		x := uint64(1)
		mask := uint64(benchmarkChunkIndexSize - 1)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			x = x*6364136223846793005 + 1
			index := r.GetIndexByDecompOffset(x & mask)
			if index != nil {
				ids += index.ID
			}
		}
		benchmarkIntSink = ids
	})
}

func BenchmarkDecoderLargeIndexGetIndexByID(b *testing.B) {
	r := benchmarkLargeReader(b)

	cases := []struct {
		name string
		id   int64
	}{
		{name: "First", id: 0},
		{name: "Middle", id: benchmarkChunkIndexSize / 2},
		{name: "Last", id: benchmarkChunkIndexSize - 1},
		{name: "MissNegative", id: -1},
		{name: "MissPastEnd", id: benchmarkChunkIndexSize},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkEntrySink = r.GetIndexByID(tc.id)
			}
		})
	}

	b.Run("Sequential", func(b *testing.B) {
		var ids int64
		mask := int64(benchmarkChunkIndexSize - 1)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			index := r.GetIndexByID(int64(i) & mask)
			if index != nil {
				ids += index.ID
			}
		}
		benchmarkIntSink = ids
	})

	b.Run("PseudoRandom", func(b *testing.B) {
		var ids int64
		x := uint64(1)
		mask := uint64(benchmarkChunkIndexSize - 1)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			x = x*6364136223846793005 + 1
			index := r.GetIndexByID(int64(x & mask))
			if index != nil {
				ids += index.ID
			}
		}
		benchmarkIntSink = ids
	})
}
