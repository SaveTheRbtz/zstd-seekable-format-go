package seekable

import (
	"testing"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/env"
)

var seekTableBenchmarkSizes = []struct {
	name string
	size int
}{
	{name: "16K", size: 16 << 10},
	{name: "128K", size: 128 << 10},
	{name: "1M", size: 1 << 20},
}

var (
	benchmarkSeekTableSink *seekTable
	benchmarkEntrySink     *env.FrameOffsetEntry
	benchmarkIntSink       int64
)

func benchmarkSeekTable(b testing.TB, size int) []byte {
	b.Helper()

	entrySize := int(seekTableEntrySize(true))
	seekTable := make([]byte, size*entrySize+seekTableFooterOffset)
	entry := seekTableEntry{CompressedSize: 1, DecompressedSize: 1}
	for i := 0; i < size; i++ {
		entry.marshalBinaryInline(seekTable[i*entrySize : (i+1)*entrySize])
	}

	footer := seekTableFooter{
		NumberOfFrames: uint32(size),
		SeekTableDescriptor: seekTableDescriptor{
			ChecksumFlag: true,
		},
		SeekableMagicNumber: seekableMagicNumber,
	}
	footer.marshalBinaryInline(seekTable[size*entrySize:])

	frame, err := createSkippableFrame(seekableTag, seekTable)
	if err != nil {
		b.Fatal(err)
	}
	return frame
}

func benchmarkParsedSeekTable(b *testing.B, size int) *seekTable {
	b.Helper()

	seekTable := benchmarkSeekTable(b, size)
	table, err := NewSeekTable(seekTable)
	if err != nil {
		b.Fatal(err)
	}
	return table
}

func BenchmarkSeekTableIndexBuild(b *testing.B) {
	for _, benchmarkSize := range seekTableBenchmarkSizes {
		b.Run(benchmarkSize.name, func(b *testing.B) {
			seekTable := benchmarkSeekTable(b, benchmarkSize.size)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				table, err := NewSeekTable(seekTable)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkSeekTableSink = table
			}
		})
	}
}

func BenchmarkSeekTableGetIndexByDecompOffset(b *testing.B) {
	for _, benchmarkSize := range seekTableBenchmarkSizes {
		b.Run(benchmarkSize.name, func(b *testing.B) {
			table := benchmarkParsedSeekTable(b, benchmarkSize.size)

			cases := []struct {
				name string
				off  uint64
			}{
				{name: "First", off: 0},
				{name: "Middle", off: uint64(benchmarkSize.size / 2)},
				{name: "Last", off: uint64(benchmarkSize.size - 1)},
				{name: "MissPastEnd", off: uint64(benchmarkSize.size)},
			}

			for _, tc := range cases {
				b.Run(tc.name, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						benchmarkEntrySink = table.GetIndexByDecompOffset(tc.off)
					}
				})
			}

			b.Run("Sequential", func(b *testing.B) {
				var ids int64
				mask := uint64(benchmarkSize.size - 1)

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					index := table.GetIndexByDecompOffset(uint64(i) & mask)
					if index != nil {
						ids += index.ID
					}
				}
				benchmarkIntSink = ids
			})

			b.Run("PseudoRandom", func(b *testing.B) {
				var ids int64
				x := uint64(1)
				mask := uint64(benchmarkSize.size - 1)

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					x = x*6364136223846793005 + 1
					index := table.GetIndexByDecompOffset(x & mask)
					if index != nil {
						ids += index.ID
					}
				}
				benchmarkIntSink = ids
			})
		})
	}
}

func BenchmarkSeekTableGetIndexByID(b *testing.B) {
	for _, benchmarkSize := range seekTableBenchmarkSizes {
		b.Run(benchmarkSize.name, func(b *testing.B) {
			table := benchmarkParsedSeekTable(b, benchmarkSize.size)

			cases := []struct {
				name string
				id   int64
			}{
				{name: "First", id: 0},
				{name: "Middle", id: int64(benchmarkSize.size / 2)},
				{name: "Last", id: int64(benchmarkSize.size - 1)},
				{name: "MissNegative", id: -1},
				{name: "MissPastEnd", id: int64(benchmarkSize.size)},
			}

			for _, tc := range cases {
				b.Run(tc.name, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						benchmarkEntrySink = table.GetIndexByID(tc.id)
					}
				})
			}

			b.Run("Sequential", func(b *testing.B) {
				var ids int64
				mask := int64(benchmarkSize.size - 1)

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					index := table.GetIndexByID(int64(i) & mask)
					if index != nil {
						ids += index.ID
					}
				}
				benchmarkIntSink = ids
			})

			b.Run("PseudoRandom", func(b *testing.B) {
				var ids int64
				x := uint64(1)
				mask := uint64(benchmarkSize.size - 1)

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					x = x*6364136223846793005 + 1
					index := table.GetIndexByID(int64(x & mask))
					if index != nil {
						ids += index.ID
					}
				}
				benchmarkIntSink = ids
			})
		})
	}
}
