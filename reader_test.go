package seekable

import (
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
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

type seekableBufferReader struct {
	buf    []byte
	offset int64
}

func (s *seekableBufferReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, fmt.Errorf("offset before the start of the file: %d", off)
	}

	size := uint64(len(s.buf)) - uint64(off)
	if size > uint64(len(p)) {
		size = uint64(len(p))
	}

	copy(p, s.buf[off:uint64(off)+size])

	return int(size), nil
}

func (s *seekableBufferReader) Read(p []byte) (n int, err error) {
	size := int64(len(s.buf)) - s.offset
	if size > int64(len(p)) {
		size = int64(len(p))
	}

	copy(p, s.buf[s.offset:s.offset+size])

	s.offset += size
	return int(size), nil
}

func (s *seekableBufferReader) Seek(offset int64, whence int) (int64, error) {
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

func TestReader(t *testing.T) {
	t.Parallel()

	for _, b := range [][]byte{checksum, noChecksum} {
		br := &seekableBufferReader{buf: b}
		r, err := NewReader(br)
		assert.NoError(t, err)

		sr := r.(*ReaderImpl)
		assert.Equal(t, int64(9), sr.endOffset)
		assert.Equal(t, 2, sr.index.Len())
		assert.Equal(t, int64(0), sr.offset)

		bytes1 := []byte("test")
		bytes2 := []byte("test2")

		tmp := make([]byte, 4096)
		n, err := r.Read(tmp)
		assert.NoError(t, err)
		assert.Equal(t, len(bytes1), n)
		assert.Equal(t, bytes1, tmp[:n])

		assert.Equal(t, int64(n), sr.offset)

		offset1, data1 := sr.cachedFrame.get()
		assert.Equal(t, uint64(0), offset1)
		assert.Equal(t, bytes1, data1)

		m, err := r.Read(tmp)
		assert.NoError(t, err)
		assert.Equal(t, len(bytes2), m)
		assert.Equal(t, bytes2, tmp[:m])

		assert.Equal(t, int64(n)+int64(m), sr.offset)
		offset2, data2 := sr.cachedFrame.get()
		assert.Equal(t, uint64(len(bytes1)), offset2)
		assert.Equal(t, bytes2, data2)

		_, err = r.Read(tmp)
		assert.Equal(t, err, io.EOF)

		assert.NoError(t, r.Close())
	}
}

func TestReaderEdges(t *testing.T) {
	t.Parallel()

	source := []byte(sourceString)
	for i, b := range [][]byte{checksum, noChecksum} {
		b := b
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()

			sr := &seekableBufferReader{buf: b}
			r, err := NewReader(sr)
			assert.NoError(t, err)
			defer r.Close()

			for _, whence := range []int{io.SeekStart, io.SeekEnd} {
				for n := int64(-1); n <= int64(len(source)); n++ {
					for m := int64(0); m <= int64(len(source)); m++ {
						var j int64
						switch whence {
						case io.SeekStart:
							j, err = r.Seek(n, whence)
						case io.SeekEnd:
							j, err = r.Seek(int64(-len(source))+n, whence)
						}
						if n < 0 {
							assert.Error(t, err)
							continue
						}
						assert.NoError(t, err)
						assert.Equal(t, n, j)

						tmp := make([]byte, m)
						k, err := r.Read(tmp)
						if n >= int64(len(source)) {
							assert.Equal(t, err, io.EOF,
								"%d: should return EOF at %d, len(source): %d, len(tmp): %d, k: %d, whence: %d",
								i, n, len(source), m, k, whence)
							continue
						} else {
							assert.NoError(t, err,
								"%d: should NOT return EOF at %d, len(source): %d, len(tmp): %d, k: %d, whence: %d",
								i, n, len(source), m, k, whence)
						}

						assert.Equal(t, source[n:n+int64(k)], tmp[:k])
					}
				}
			}
		})
	}
}

// TestReadeAt verified the following ReaderAt asssumption:
// 	When ReadAt returns n < len(p), it returns a non-nil error explaining why more bytes were not returned.
// 	In this respect, ReadAt is stricter than Read.
func TestReadeAt(t *testing.T) {
	t.Parallel()

	sr := &seekableBufferReader{buf: checksum}
	r, err := NewReader(sr)
	assert.NoError(t, err)
	defer r.Close()

	tmp1 := make([]byte, 3)
	k1, err := r.ReadAt(tmp1, 3)
	assert.NoError(t, err)

	assert.Equal(t, 3, k1)
	assert.Equal(t, []byte("tte"), tmp1)

	tmp2 := make([]byte, 100)
	k2, err := r.ReadAt(tmp2, 3)
	assert.Error(t, err, io.EOF)

	assert.Equal(t, 6, k2)
	assert.Equal(t, []byte("ttest2"), tmp2[:k2])
}

func TestReaderEdgesParallel(t *testing.T) {
	t.Parallel()

	source := []byte(sourceString)
	for i, b := range [][]byte{checksum, noChecksum} {
		b := b

		sr := &seekableBufferReader{buf: b}
		r, err := NewReader(sr)
		assert.NoError(t, err)
		defer r.Close()

		for n := int64(-1); n <= int64(len(source)); n++ {
			for m := int64(0); m <= int64(len(source)); m++ {
				t.Run(fmt.Sprintf("%d/%d/%d", i, n, m), func(t *testing.T) {
					t.Parallel()

					tmp := make([]byte, m)
					k, err := r.ReadAt(tmp, n)
					if n < 0 {
						assert.Error(t, err)
						return
					}

					if n >= int64(len(source)) {
						assert.Equal(t, err, io.EOF,
							"%d: should return EOF at %d, len(source): %d, len(tmp): %d, k: %d",
							i, n, len(source), m, k)
						return
					} else {
						assert.NoError(t, err,
							"%d: should NOT return EOF at %d, len(source): %d, len(tmp): %d, k: %d",
							i, n, len(source), m, k)
					}

					assert.Equal(t, source[n:n+int64(k)], tmp[:k])
				})
			}
		}
	}
}
