package seekable

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriter(t *testing.T) {
	t.Parallel()

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	require.NoError(t, err)

	var b bytes.Buffer
	bw := io.Writer(&b)
	w, err := NewWriter(bw, enc)
	require.NoError(t, err)

	bytes1 := []byte("test")
	bytesWritten1, err := w.Write(bytes1)
	require.NoError(t, err)
	bytes2 := []byte("test2")
	bytesWritten2, err := w.Write(bytes2)
	require.NoError(t, err)

	// test internals
	sw := w.(*writerImpl)
	assert.Len(t, sw.frameEntries, 2)
	assert.Len(t, bytes1, int(sw.frameEntries[0].DecompressedSize))
	assert.Len(t, bytes1, bytesWritten1)
	assert.Equal(t, uint32(len(bytes2)), sw.frameEntries[1].DecompressedSize)
	assert.Equal(t, uint32(bytesWritten2), sw.frameEntries[1].DecompressedSize)

	index1CompressedSize := sw.frameEntries[0].CompressedSize
	err = w.Close()
	require.NoError(t, err)

	// verify buffer content
	buf := b.Bytes()
	// magic footer
	assert.Equal(t, []byte{0xb1, 0xea, 0x92, 0x8f}, buf[len(buf)-4:])
	assert.Equal(t, uint32(2), binary.LittleEndian.Uint32(buf[len(buf)-9:len(buf)-5]))
	// index.1
	indexOffset := len(buf) - 4 - 1 - 4 - 2*12
	assert.Equal(t, index1CompressedSize, binary.LittleEndian.Uint32(buf[indexOffset:indexOffset+4]))
	assert.Equal(t, uint32(len(bytes1)), binary.LittleEndian.Uint32(buf[indexOffset+4:indexOffset+8]))
	// skipframe header
	frameOffset := indexOffset - 4 - 4
	assert.Equal(t, []byte{0x5e, 0x2a, 0x4d, 0x18}, buf[frameOffset:frameOffset+4])
	assert.Equal(t, uint32(0x21), binary.LittleEndian.Uint32(buf[frameOffset+4:frameOffset+8]))

	// test decompression
	br := io.Reader(&b)
	dec, err := zstd.NewReader(br)
	require.NoError(t, err)
	readBuf := make([]byte, 1024)
	n, err := dec.Read(readBuf)
	require.ErrorIs(t, err, io.EOF)

	concat := append(bytes1, bytes2...)
	assert.Equal(t, len(concat), n)
	assert.Equal(t, concat, readBuf[:n])
}

func makeTestFrame(t *testing.T, idx int) []byte {
	var b bytes.Buffer
	for i := 0; i < 100; i++ {
		s := fmt.Sprintf("test%d", idx+i)
		_, err := b.WriteString(s)
		require.NoError(t, err)
	}
	return b.Bytes()
}

func makeTestFrameSource(frames [][]byte) FrameSource {
	idx := 0
	return func() ([]byte, error) {
		if idx >= len(frames) {
			return nil, nil
		}
		ret := frames[idx]
		idx++
		return ret, nil
	}
}

func TestConcurrentWriter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	require.NoError(t, err)

	// Setup test data
	const frameCount = 20
	var frames [][]byte
	var concat []byte
	for i := 0; i < frameCount; i++ {
		frame := makeTestFrame(t, i)
		frames = append(frames, frame)
		concat = append(concat, frame...)
	}

	// Write concurrently
	var b bytes.Buffer
	bw := io.Writer(&b)
	concurrentWriter, err := NewWriter(bw, enc)
	require.NoError(t, err)

	var totalWritten int
	err = concurrentWriter.WriteMany(ctx, makeTestFrameSource(frames), WithConcurrency(5),
		WithWriteCallback(func(size uint32) {
			totalWritten += int(size)
		}))
	require.NoError(t, err)
	require.Equal(t, len(concat), totalWritten)

	// Write one at a time
	var nb bytes.Buffer
	nbw := io.Writer(&nb)
	oneWriter, err := NewWriter(nbw, enc)
	require.NoError(t, err)

	for i := 0; i < frameCount; i++ {
		require.NoError(t, err)
		_, err = oneWriter.Write(frames[i])
		require.NoError(t, err)
	}

	// Output should be the same
	assert.Equal(t, b.Bytes(), nb.Bytes())

	concurrentImpl := concurrentWriter.(*writerImpl)
	oneImpl := oneWriter.(*writerImpl)
	assert.Equal(t, concurrentImpl.frameEntries, oneImpl.frameEntries)

	// test decompression
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	decoded, err := dec.DecodeAll(b.Bytes(), nil)
	require.NoError(t, err)
	assert.Equal(t, concat, decoded)
}

type failingWriteEnvironment struct {
	n   int
	err error
}

func (e failingWriteEnvironment) WriteFrame(p []byte) (n int, err error) {
	return e.n, e.err
}

func (e failingWriteEnvironment) WriteSeekTable(p []byte) (n int, err error) {
	return e.n, e.err
}

func TestConcurrentWriterErrors(t *testing.T) {
	t.Parallel()

	manyFrames := [][]byte{}
	for i := 0; i < 100; i++ {
		manyFrames = append(manyFrames, []byte(fmt.Sprintf("test%d", i)))
	}

	ctx := context.Background()
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	require.NoError(t, err)
	w, err := NewWriter(nil, enc)
	require.NoError(t, err)

	frameSource := makeTestFrameSource([][]byte{})
	err = w.WriteMany(ctx, frameSource, WithConcurrency(0))
	assert.ErrorContains(t, err, "concurrency must be positive")

	frameSource = func() ([]byte, error) {
		return nil, errors.New("test error")
	}
	err = w.WriteMany(ctx, frameSource)
	assert.ErrorContains(t, err, "frame source failed: test error")

	var b bytes.Buffer
	w, err = NewWriter(&b, enc,
		WithWEnvironment(failingWriteEnvironment{0, errors.New("test error")}))
	require.NoError(t, err)
	frameSource = makeTestFrameSource(manyFrames) // enough that we have to wait on ctx
	err = w.WriteMany(ctx, frameSource, WithConcurrency(1))
	assert.ErrorContains(t, err, "failed to write compressed data")

	w, err = NewWriter(&b, enc,
		WithWEnvironment(failingWriteEnvironment{1, nil}))
	require.NoError(t, err)
	err = w.WriteMany(ctx, frameSource, WithConcurrency(1))
	assert.ErrorContains(t, err, "partial write")
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
	require.NoError(t, err)

	w, err := NewWriter(nil, enc, WithWEnvironment(&fakeWriteEnvironment{
		bw: io.Writer(&b),
	}))
	require.NoError(t, err)

	bytes1 := []byte("test")
	_, err = w.Write(bytes1)
	require.NoError(t, err)
	bytes2 := []byte("test2")
	_, err = w.Write(bytes2)
	require.NoError(t, err)

	err = w.Close()
	require.NoError(t, err)

	// test decompression
	br := io.Reader(&b)
	dec, err := zstd.NewReader(br)
	require.NoError(t, err)
	readBuf := make([]byte, 1024)
	n, err := dec.Read(readBuf)
	require.ErrorIs(t, err, io.EOF)
	concat := append(bytes1, bytes2...)
	assert.Equal(t, len(concat), n)
	assert.Equal(t, concat, readBuf[:n])
}

func makeRepeatingFrameSource(frame []byte, count int) FrameSource {
	idx := 0
	return func() ([]byte, error) {
		if idx >= count {
			return nil, nil
		}
		idx++
		return frame, nil
	}
}

type nullWriter struct{}

func (nullWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func BenchmarkWrite(b *testing.B) {
	ctx := context.Background()

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	require.NoError(b, err)

	sizes := []int64{128, 4 * 1024, 16 * 1024, 64 * 1024, 1 * 1024 * 1024}
	for _, sz := range sizes {
		writeBuf := make([]byte, sz)
		_, err := rand.Read(writeBuf)
		require.NoError(b, err)

		w, err := NewWriter(nullWriter{}, enc)
		require.NoError(b, err)

		b.Run(fmt.Sprintf("%d", sz), func(b *testing.B) {
			b.SetBytes(sz)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _ = w.Write(writeBuf)
			}
		})
		b.Run(fmt.Sprintf("Parallel-%d", sz), func(b *testing.B) {
			b.SetBytes(sz)
			b.ResetTimer()

			err = w.WriteMany(ctx, makeRepeatingFrameSource(writeBuf, b.N))
			require.NoError(b, err)
		})

		err = w.Close()
		require.NoError(b, err)
	}
}
