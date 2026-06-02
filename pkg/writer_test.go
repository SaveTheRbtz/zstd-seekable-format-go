package seekable

import (
	"bytes"
	"context"
	"crypto/rand"
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

	assert.Len(t, bytes1, bytesWritten1)
	assert.Len(t, bytes2, bytesWritten2)

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
		_, err = oneWriter.Write(frames[i])
		require.NoError(t, err)
	}

	// Output should be the same
	assert.Equal(t, b.Bytes(), nb.Bytes())

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

type partialSecondFrameEnvironment struct {
	b           bytes.Buffer
	frameWrites int
}

func (e *partialSecondFrameEnvironment) WriteFrame(p []byte) (n int, err error) {
	e.frameWrites++
	if e.frameWrites == 2 {
		return e.b.Write(p[:1])
	}
	return e.b.Write(p)
}

func (e *partialSecondFrameEnvironment) WriteSeekTable(p []byte) (n int, err error) {
	return e.b.Write(p)
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
	err = w.WriteMany(ctx, makeTestFrameSource(manyFrames), WithConcurrency(1))
	assert.ErrorContains(t, err, "failed to write compressed data")
	_, err = w.Write([]byte("again"))
	assert.ErrorIs(t, err, errWriterFailed)

	w, err = NewWriter(&b, enc,
		WithWEnvironment(failingWriteEnvironment{1, nil}))
	require.NoError(t, err)
	err = w.WriteMany(ctx, makeTestFrameSource(manyFrames), WithConcurrency(1))
	assert.ErrorContains(t, err, "partial write")
	_, err = w.Write([]byte("again"))
	assert.ErrorIs(t, err, errWriterFailed)
}

func TestFrameWriteFailureAllowsClose(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		write func(t *testing.T, w ConcurrentWriter, frames [][]byte) error
	}{
		{
			name: "Write",
			write: func(t *testing.T, w ConcurrentWriter, frames [][]byte) error {
				t.Helper()

				_, err := w.Write(frames[0])
				require.NoError(t, err)
				_, err = w.Write(frames[1])
				return err
			},
		},
		{
			name: "WriteMany",
			write: func(t *testing.T, w ConcurrentWriter, frames [][]byte) error {
				t.Helper()

				return w.WriteMany(context.Background(), makeTestFrameSource(frames), WithConcurrency(1))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
			require.NoError(t, err)
			dec, err := zstd.NewReader(nil)
			require.NoError(t, err)
			defer dec.Close()

			env := &partialSecondFrameEnvironment{}
			w, err := NewWriter(nil, enc, WithWEnvironment(env))
			require.NoError(t, err)

			frames := [][]byte{[]byte("first"), []byte("second")}
			err = tc.write(t, w, frames)
			require.ErrorContains(t, err, "partial write")

			_, err = w.Write([]byte("again"))
			assert.ErrorIs(t, err, errWriterFailed)

			err = w.Close()
			require.NoError(t, err)

			table, err := readSeekTable(&readSeekerEnvImpl{rs: bytes.NewReader(env.b.Bytes())})
			require.NoError(t, err)
			assert.Equal(t, uint64(len(frames[0])), table.Size())
			assert.Equal(t, int64(1), table.NumFrames())

			r, err := NewReader(bytes.NewReader(env.b.Bytes()), dec)
			require.NoError(t, err)
			defer func() { require.NoError(t, r.Close()) }()

			got := make([]byte, len(frames[0]))
			n, err := r.ReadAt(got, 0)
			require.NoError(t, err)
			assert.Equal(t, len(got), n)
			assert.Equal(t, frames[0], got)
		})
	}
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

func TestZeroSizedFrameIgnored(t *testing.T) {
	t.Parallel()

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	require.NoError(t, err)

	var b bytes.Buffer
	w, err := NewWriter(&b, enc)
	require.NoError(t, err)

	var n int
	// Write two real frames with an empty frame in between.
	_, err = w.Write([]byte("foo"))
	require.NoError(t, err)
	n, err = w.Write([]byte{})
	require.NoError(t, err)
	assert.Equal(t, 0, n, "should not write anything for empty frame")
	_, err = w.Write([]byte("bar"))
	require.NoError(t, err)
	_, err = w.Write([]byte{})
	require.NoError(t, err)
	_, err = w.Write([]byte{})
	require.NoError(t, err)

	require.NoError(t, w.Close())

	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()

	r, err := NewReader(bytes.NewReader(b.Bytes()), dec)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()

	table, err := readSeekTable(&readSeekerEnvImpl{rs: bytes.NewReader(b.Bytes())})
	require.NoError(t, err)
	assert.Equal(t, uint64(len("foobar")), table.Size())
	assert.Equal(t, int64(2), table.NumFrames())
	_, ok := table.EntryByID(2)
	require.False(t, ok)

	expected := []byte("foobar")
	buf := make([]byte, len(expected))
	n, err = r.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, len(expected), n)
	assert.Equal(t, expected, buf)

	_, err = r.ReadAt(make([]byte, 1), int64(len(expected)))
	require.ErrorIs(t, err, io.EOF)
}

func TestCloseErrors(t *testing.T) {
	t.Parallel()

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	require.NoError(t, err)

	// environment returns error on WriteSeekTable
	w, err := NewWriter(nil, enc,
		WithWEnvironment(failingWriteEnvironment{0, errors.New("test error")}))
	require.NoError(t, err)
	err = w.Close()
	assert.ErrorContains(t, err, "test error")

	// environment reports partial write
	w, err = NewWriter(nil, enc, WithWEnvironment(failingWriteEnvironment{1, nil}))
	require.NoError(t, err)
	err = w.Close()
	assert.ErrorContains(t, err, "partial write")
}

func TestWriterCloseSemantics(t *testing.T) {
	t.Parallel()

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	require.NoError(t, err)

	var b bytes.Buffer
	w, err := NewWriter(&b, enc)
	require.NoError(t, err)
	sw := w.(*writerImpl)

	_, err = w.Write([]byte("foo"))
	require.NoError(t, err)
	require.NotEmpty(t, sw.frameEntries)

	require.NoError(t, w.Close())
	assert.Nil(t, sw.frameEntries)

	// double close should return an error
	err = w.Close()
	assert.ErrorIs(t, err, errWriterClosed)

	// write after close should return an error
	_, err = w.Write([]byte("bar"))
	assert.ErrorIs(t, err, errWriterClosed)

	// write many after close should return an error
	err = w.WriteMany(context.Background(), makeTestFrameSource([][]byte{[]byte("baz")}))
	assert.ErrorIs(t, err, errWriterClosed)
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
