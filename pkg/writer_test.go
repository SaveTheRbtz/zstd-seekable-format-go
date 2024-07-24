package seekable

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	t.Parallel()

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	assert.NoError(t, err)

	var b bytes.Buffer
	bw := io.Writer(&b)
	w, err := NewWriter(bw, enc)
	assert.NoError(t, err)

	bytes1 := []byte("test")
	bytesWritten1, err := w.Write(bytes1)
	assert.NoError(t, err)
	bytes2 := []byte("test2")
	bytesWritten2, err := w.Write(bytes2)
	assert.NoError(t, err)

	// test internals
	sw := w.(*writerImpl)
	assert.Equal(t, 2, len(sw.frameEntries))
	assert.Equal(t, uint32(len(bytes1)), sw.frameEntries[0].DecompressedSize)
	assert.Equal(t, bytesWritten1, len(bytes1))
	assert.Equal(t, uint32(len(bytes2)), sw.frameEntries[1].DecompressedSize)
	assert.Equal(t, uint32(bytesWritten2), sw.frameEntries[1].DecompressedSize)

	index1CompressedSize := sw.frameEntries[0].CompressedSize
	err = w.Close()
	assert.NoError(t, err)

	// verify buffer content
	buf := b.Bytes()
	// magic footer
	assert.Equal(t, buf[len(buf)-4:], []byte{0xb1, 0xea, 0x92, 0x8f})
	assert.Equal(t, uint32(2), binary.LittleEndian.Uint32(buf[len(buf)-9:len(buf)-5]))
	// index.1
	indexOffset := len(buf) - 4 - 1 - 4 - 2*12
	assert.Equal(t, index1CompressedSize, binary.LittleEndian.Uint32(buf[indexOffset:indexOffset+4]))
	assert.Equal(t, uint32(len(bytes1)), binary.LittleEndian.Uint32(buf[indexOffset+4:indexOffset+8]))
	// skipframe header
	frameOffset := indexOffset - 4 - 4
	assert.Equal(t, buf[frameOffset:frameOffset+4], []byte{0x5e, 0x2a, 0x4d, 0x18})
	assert.Equal(t, uint32(0x21), binary.LittleEndian.Uint32(buf[frameOffset+4:frameOffset+8]))

	// test decompression
	br := io.Reader(&b)
	dec, err := zstd.NewReader(br)
	assert.NoError(t, err)
	readBuf := make([]byte, 1024)
	n, err := dec.Read(readBuf)
	assert.Equal(t, err, io.EOF)
	concat := append(bytes1, bytes2...)
	assert.Equal(t, len(concat), n)
	assert.Equal(t, concat, readBuf[:n])
}

type fakeWriteEnvironment struct {
	bw io.Writer
}

func (s *fakeWriteEnvironment) WriteFrame(p []byte) (n int, err error) {
	return s.bw.Write(p)
}

func (s *fakeWriteEnvironment) WriteSeekTable(p []byte) (n int, err error) {
	return s.bw.Write(p)
}

func TestWriteEnvironment(t *testing.T) {
	t.Parallel()

	var b bytes.Buffer

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	assert.NoError(t, err)

	w, err := NewWriter(nil, enc, WithWEnvironment(&fakeWriteEnvironment{
		bw: io.Writer(&b),
	}))
	assert.NoError(t, err)

	bytes1 := []byte("test")
	_, err = w.Write(bytes1)
	assert.NoError(t, err)
	bytes2 := []byte("test2")
	_, err = w.Write(bytes2)
	assert.NoError(t, err)

	err = w.Close()
	assert.NoError(t, err)

	// test decompression
	br := io.Reader(&b)
	dec, err := zstd.NewReader(br)
	assert.NoError(t, err)
	readBuf := make([]byte, 1024)
	n, err := dec.Read(readBuf)
	assert.Equal(t, err, io.EOF)
	concat := append(bytes1, bytes2...)
	assert.Equal(t, len(concat), n)
	assert.Equal(t, concat, readBuf[:n])
}

func BenchmarkWrite(b *testing.B) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	assert.NoError(b, err)

	sizes := []int64{128, 4 * 1024, 16 * 1024, 64 * 1024, 1 * 1024 * 1024}
	for _, sz := range sizes {
		writeBuf := make([]byte, sz)
		_, err := rand.Read(writeBuf)
		assert.NoError(b, err)
		var buf bytes.Buffer
		bw := io.Writer(&buf)
		w, err := NewWriter(bw, enc)
		assert.NoError(b, err)

		b.Run(fmt.Sprintf("%d", sz), func(b *testing.B) {
			b.SetBytes(sz)
			b.ResetTimer()

			// TODO: Limit memory consumption.
			for i := 0; i < b.N; i++ {
				_, _ = w.Write(writeBuf)
			}
		})
		err = w.Close()
		assert.NoError(b, err)
	}
}
