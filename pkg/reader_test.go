package seekable

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/env"
)

const sourceString = "testtest2"

var checksum = []byte{
	// frame 1
	0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x00, 0x21, 0x00, 0x00,
	// "test"
	0x74, 0x65, 0x73, 0x74,
	0x39, 0x81, 0x67, 0xdb,
	// frame 2
	0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x00, 0x29, 0x00, 0x00,
	// "test2"
	0x74, 0x65, 0x73, 0x74, 0x32,
	0x87, 0xeb, 0x11, 0x71,
	// skippable frame
	0x5e, 0x2a, 0x4d, 0x18,
	0x21, 0x00, 0x00, 0x00,
	// index
	0x11, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x39, 0x81, 0x67, 0xdb,
	0x12, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x87, 0xeb, 0x11, 0x71,
	// footer
	0x02, 0x00, 0x00, 0x00,
	0x80,
	0xb1, 0xea, 0x92, 0x8f,
}

var noChecksum = []byte{
	// frame 1
	0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x00, 0x21, 0x00, 0x00,
	// "test"
	0x74, 0x65, 0x73, 0x74,
	0x39, 0x81, 0x67, 0xdb,
	// frame 2
	0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x00, 0x29, 0x00, 0x00,
	// "test2"
	0x74, 0x65, 0x73, 0x74, 0x32,
	0x87, 0xeb, 0x11, 0x71,
	// skippable frame
	0x5e, 0x2a, 0x4d, 0x18,
	0x19, 0x00, 0x00, 0x00,
	// index
	0x11, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
	0x12, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00,
	// footer
	0x02, 0x00, 0x00, 0x00,
	0x00,
	0xb1, 0xea, 0x92, 0x8f,
}

type seekableBufferReaderAt struct {
	buf    []byte
	offset int64
}

func (s *seekableBufferReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, fmt.Errorf("offset before the start of the file: %d", off)
	}

	size := uint64(len(s.buf)) - uint64(off)
	if size > uint64(len(p)) {
		size = uint64(len(p))
	}

	if off > int64(len(s.buf)) {
		return 0, io.EOF
	}

	copy(p, s.buf[off:uint64(off)+size])

	return int(size), nil
}

func (s *seekableBufferReaderAt) Read(p []byte) (n int, err error) {
	size := int64(len(s.buf)) - s.offset
	if size > int64(len(p)) {
		size = int64(len(p))
	}

	if s.offset > int64(len(s.buf)) {
		return 0, io.EOF
	}

	copy(p, s.buf[s.offset:s.offset+size])

	s.offset += size
	return int(size), nil
}

func (s *seekableBufferReaderAt) Seek(offset int64, whence int) (int64, error) {
	newOffset := s.offset
	switch whence {
	case io.SeekCurrent:
		newOffset += offset
	case io.SeekStart:
		newOffset = offset
	case io.SeekEnd:
		newOffset = int64(len(s.buf)) + offset
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("offset before the start of the file: %d (%d + %d)",
			newOffset, s.offset, offset)
	}

	s.offset = newOffset
	return s.offset, nil
}

type seekableBufferReader struct {
	sra seekableBufferReaderAt
}

func (s *seekableBufferReader) Read(p []byte) (n int, err error) {
	return s.sra.Read(p)
}

func (s *seekableBufferReader) Seek(offset int64, whence int) (int64, error) {
	return s.sra.Seek(offset, whence)
}

func TestReader(t *testing.T) {
	t.Parallel()

	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()

	for _, b := range [][]byte{checksum, noChecksum} {
		br := &seekableBufferReaderAt{buf: b}
		r, err := NewReader(br, dec)
		require.NoError(t, err)

		sr := r.(*readerImpl)
		assert.Equal(t, int64(9), sr.endOffset)
		assert.Equal(t, 2, sr.index.Len())
		assert.Equal(t, int64(0), sr.offset)

		bytes1 := []byte("test")
		bytes2 := []byte("test2")

		tmp := make([]byte, 4096)
		n, err := r.Read(tmp)
		require.NoError(t, err)
		assert.Equal(t, len(bytes1), n)
		assert.Equal(t, bytes1, tmp[:n])

		assert.Equal(t, int64(n), sr.offset)

		offset1, data1 := sr.cachedFrame.get()
		assert.Equal(t, uint64(0), offset1)
		assert.Equal(t, bytes1, data1)

		m, err := r.Read(tmp)
		require.NoError(t, err)
		assert.Equal(t, len(bytes2), m)
		assert.Equal(t, bytes2, tmp[:m])

		assert.Equal(t, int64(n)+int64(m), sr.offset)
		offset2, data2 := sr.cachedFrame.get()
		assert.Equal(t, uint64(len(bytes1)), offset2)
		assert.Equal(t, bytes2, data2)

		_, err = r.Read(tmp)
		require.ErrorIs(t, err, io.EOF)

		err = r.Close()
		require.NoError(t, err)

		// read after close
		_, err = r.Read(tmp)
		require.ErrorContains(t, err, "reader is closed")

		// double close
		err = r.Close()
		require.NoError(t, err)
	}
}

func TestReaderEdges(t *testing.T) {
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)

	source := []byte(sourceString)
	for i, b := range [][]byte{checksum, noChecksum} {
		i := i
		b := b
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()

			sr := &seekableBufferReaderAt{buf: b}
			r, err := NewReader(sr, dec)
			require.NoError(t, err)
			defer func() { require.NoError(t, r.Close()) }()

			for _, whence := range []int{io.SeekStart, io.SeekEnd} {
				for n := int64(-1); n <= int64(len(source))+1; n++ {
					for m := int64(0); m <= int64(len(source))+1; m++ {
						var j int64
						switch whence {
						case io.SeekStart:
							j, err = r.Seek(n, whence)
						case io.SeekEnd:
							j, err = r.Seek(int64(-len(source))+n, whence)
						}
						if n < 0 {
							require.Error(t, err)
							continue
						}
						require.NoError(t, err)
						assert.Equal(t, n, j)

						tmp := make([]byte, m)
						k, err := r.Read(tmp)
						if n >= int64(len(source)) {
							require.ErrorIsf(t, err, io.EOF,
								"%d: should return EOF at %d, len(source): %d, len(tmp): %d, k: %d, whence: %d",
								i, n, len(source), m, k, whence)
							continue
						}
						require.NoErrorf(t, err,
							"%d: should NOT return EOF at %d, len(source): %d, len(tmp): %d, k: %d, whence: %d",
							i, n, len(source), m, k, whence)

						assert.Equal(t, source[n:n+int64(k)], tmp[:k])
					}
				}
			}
		})
	}
}

// TestReaderAt verified the following ReaderAt asssumption:
//
// When ReadAt returns n < len(p), it returns a non-nil error explaining why more bytes were not returned.
// In this respect, ReadAt is stricter than Read.
func TestReaderAt(t *testing.T) {
	t.Parallel()

	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()

	for _, sr := range []io.ReadSeeker{
		&seekableBufferReader{seekableBufferReaderAt{buf: noChecksum}},
		&seekableBufferReaderAt{buf: noChecksum},
	} {
		sr := sr
		t.Run(fmt.Sprintf("%T", sr), func(t *testing.T) {
			r, err := NewReader(sr, dec)
			require.NoError(t, err)
			defer func() { require.NoError(t, r.Close()) }()

			oldOffset, err := r.Seek(0, io.SeekCurrent)
			require.NoError(t, err)
			assert.Equal(t, int64(0), oldOffset)

			tmp1 := make([]byte, 3)
			k1, err := r.ReadAt(tmp1, 3)
			require.NoError(t, err)
			assert.Equal(t, 3, k1)
			assert.Equal(t, []byte("tte"), tmp1)

			// If ReadAt is reading from an input source with a seek offset,
			// ReadAt should not affect nor be affected by the underlying seek offset.
			newOffset, err := r.Seek(0, io.SeekCurrent)
			require.NoError(t, err)
			assert.Equal(t, newOffset, oldOffset)

			tmp2 := make([]byte, 100)
			k2, err := r.ReadAt(tmp2, 3)
			require.ErrorIs(t, err, io.EOF)

			tmpLast := make([]byte, 1)
			kLast, err := r.ReadAt(tmpLast, 8)
			assert.Equal(t, 1, kLast)
			assert.Equal(t, []byte("2"), tmpLast)
			require.NoError(t, err)

			tmpOOB := make([]byte, 1)
			_, err = r.ReadAt(tmpOOB, 9)
			require.ErrorIs(t, err, io.EOF)

			assert.Equal(t, 6, k2)
			assert.Equal(t, []byte("ttest2"), tmp2[:k2])

			sectionReader := io.NewSectionReader(r, 3, 4)
			tmp3, err := io.ReadAll(sectionReader)
			require.NoError(t, err)
			assert.Len(t, tmp3, 4)
			assert.Equal(t, []byte("ttes"), tmp3)
		})
	}
}

func TestReaderEdgesParallel(t *testing.T) {
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)

	source := []byte(sourceString)
	for i, b := range [][]byte{checksum, noChecksum} {
		i := i
		b := b

		sr := &seekableBufferReaderAt{buf: b}
		r, err := NewReader(sr, dec)
		require.NoError(t, err)

		for n := int64(-1); n <= int64(len(source)); n++ {
			for m := int64(0); m <= int64(len(source)); m++ {
				n := n
				m := m
				t.Run(fmt.Sprintf("%d/len:%d/buf:%d", i, n, m), func(t *testing.T) {
					t.Parallel()

					tmp := make([]byte, m)
					k, err := r.ReadAt(tmp, n)
					if n < 0 && m != 0 {
						assert.Error(t, err,
							"%d: should return Error at %d: ret: %d, bytes: %+v",
							i, n, k, tmp)
						return
					}

					if m == 0 {
						require.NoError(t, err)
						assert.Equal(t, 0, k)
						assert.Equal(t, make([]byte, m), tmp)
						return
					}

					if n >= int64(len(source)) {
						require.ErrorIsf(t, err, io.EOF,
							"%d: should return EOF at %d, len(source): %d, len(tmp): %d, k: %d",
							i, n, len(source), m, k)
						assert.Equal(t, 0, k, "should not read anything at the end")
						return
					}
					if n+m <= int64(len(source)) {
						require.NoErrorf(t, err,
							"%d: should NOT return Err at %d, len(source): %d, len(tmp): %d, k: %d",
							i, n, len(source), m, k)
					} else {
						require.ErrorIsf(t, err, io.EOF,
							"%d: should return EOF at %d, len(source): %d, len(tmp): %d, k: %d",
							i, n, len(source), m, k)
					}
					assert.Equal(t, source[n:n+int64(k)], tmp[:k])
				})
			}
		}
	}
}

type fakeReadEnvironment struct{}

func (s *fakeReadEnvironment) GetFrameByIndex(index env.FrameOffsetEntry) ([]byte, error) {
	switch index.ID {
	case 0:
		return checksum[:17], nil
	case 1:
		return checksum[17 : 17+18], nil
	default:
		return nil, fmt.Errorf("unknown index: %d, %+v", index.ID, index)
	}
}

func (s *fakeReadEnvironment) ReadFooter() ([]byte, error) {
	return checksum[len(checksum)-10:], nil
}

func (s *fakeReadEnvironment) ReadSkipFrame(skippableFrameOffset int64) ([]byte, error) {
	return checksum[len(checksum)-41:], nil
}

func TestReadEnvironment(t *testing.T) {
	t.Parallel()
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()

	r, err := NewReader(nil, dec, WithREnvironment(&fakeReadEnvironment{}))
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()

	bytes1 := []byte("test")
	bytes2 := []byte("test2")

	tmp := make([]byte, 4096)
	n, err := r.Read(tmp)
	require.NoError(t, err)
	assert.Equal(t, len(bytes1), n)
	assert.Equal(t, bytes1, tmp[:n])

	m, err := r.Read(tmp)
	require.NoError(t, err)
	assert.Equal(t, len(bytes2), m)
	assert.Equal(t, bytes2, tmp[:m])

	_, err = r.Read(tmp)
	require.ErrorIs(t, err, io.EOF)
}

func TestNoReaderAt(t *testing.T) {
	t.Parallel()

	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()

	for _, sr := range []io.ReadSeeker{
		&seekableBufferReader{seekableBufferReaderAt{buf: checksum}},
		&seekableBufferReaderAt{buf: checksum},
	} {
		sr := sr
		t.Run(fmt.Sprintf("%T", sr), func(t *testing.T) {
			r, err := NewReader(sr, dec)
			require.NoError(t, err)
			defer func() { require.NoError(t, r.Close()) }()

			tmp := make([]byte, 3)
			n, err := r.ReadAt(tmp, 5)
			require.NoError(t, err)
			assert.Equal(t, 3, n)
			assert.Equal(t, tmp[:n], []byte("est"))

			// If ReadAt is reading from an input source with a seek offset,
			// ReadAt should not affect nor be affected by the underlying seek offset.
			m, err := r.Seek(0, io.SeekCurrent)
			require.NoError(t, err)
			assert.Equal(t, int64(0), m)

			tmp = make([]byte, 4096)
			n, err = r.Read(tmp)
			require.NoError(t, err)
			assert.Equal(t, 4, n)
			assert.Equal(t, tmp[:n], []byte("test"))

			m, err = r.Seek(1, io.SeekCurrent)
			require.NoError(t, err)
			assert.Equal(t, int64(5), m)

			n, err = r.Read(tmp)
			require.NoError(t, err)
			assert.Equal(t, 4, n)
			assert.Equal(t, tmp[:n], []byte("est2"))

			_, err = r.Seek(-1, io.SeekStart)
			require.ErrorContains(t, err, "offset before the start of the file")

			_, err = r.Seek(0, 9999)
			assert.Errorf(t, err, "unknown whence: %d", 9999)

			_, err = r.Seek(999, io.SeekStart)
			require.NoError(t, err)

			_, err = r.Read(tmp)
			require.ErrorIs(t, err, io.EOF)
		})
	}
}

func TestEmptyWriteRead(t *testing.T) {
	t.Parallel()

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	require.NoError(t, err)

	var b bytes.Buffer
	bw := io.Writer(&b)
	w, err := NewWriter(bw, enc)
	require.NoError(t, err)

	bytes1 := []byte("")
	bytesWritten1, err := w.Write(bytes1)
	require.NoError(t, err)
	assert.Equal(t, 0, bytesWritten1)

	err = w.Close()
	require.NoError(t, err)

	dec1, err := zstd.NewReader(nil)
	require.NoError(t, err)

	// test seekable decompression
	compressed := b.Bytes()

	sr := &seekableBufferReaderAt{buf: compressed}
	r, err := NewReader(sr, dec1)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()

	tmp1 := make([]byte, 1)
	n, err := r.Read(tmp1)
	require.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 0, n)

	// test native decompression
	dec2, err := zstd.NewReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	defer dec2.Close()

	tmp2 := make([]byte, 1)
	n, err = dec2.Read(tmp2)
	require.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 0, n)
}

func TestSeekTableParsing(t *testing.T) {
	var err error
	var stf seekTableFooter

	t.Parallel()

	// Checksum.
	err = stf.UnmarshalBinary([]byte{
		0x00, 0x00, 0x00, 0x00,
		1 << 7,
		0xb1, 0xea, 0x92, 0x8f,
	})
	require.NoError(t, err)

	// No checksum.
	err = stf.UnmarshalBinary([]byte{
		0x00, 0x00, 0x00, 0x00,
		0x00,
		0xb1, 0xea, 0x92, 0x8f,
	})
	require.NoError(t, err)

	// Unused bits.
	require.NoError(t, err)
	err = stf.UnmarshalBinary([]byte{
		0x00, 0x00, 0x00, 0x00,
		(1 << 7) + 0x01 + 0x2,
		0xb1, 0xea, 0x92, 0x8f,
	})
	require.NoError(t, err)

	// Reserved bits.
	err = stf.UnmarshalBinary([]byte{
		0x00, 0x00, 0x00, 0x00,
		0x84,
		0xb1, 0xea, 0x92, 0x8f,
	})
	require.ErrorContains(t, err, "footer reserved bits")
	err = stf.UnmarshalBinary([]byte{
		0x00, 0x00, 0x00, 0x00,
		0x80 + 0x40,
		0xb1, 0xea, 0x92, 0x8f,
	})
	require.ErrorContains(t, err, "footer reserved bits")

	// Size.
	err = stf.UnmarshalBinary([]byte{
		0xb1, 0xea, 0x92, 0x8f,
	})
	require.ErrorContains(t, err, "footer length mismatch")

	// Magic.
	err = stf.UnmarshalBinary([]byte{
		0x00, 0x00, 0x00, 0x00,
		0x80,
		0xea, 0x92, 0x8f, 0xb1,
	})
	require.ErrorContains(t, err, "footer magic mismatch")
}
func TestNilReaderNoEnvironment(t *testing.T) {
	t.Parallel()

	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()

	r, err := NewReader(nil, dec)
	require.Error(t, err)
	assert.Nil(t, r)
}
