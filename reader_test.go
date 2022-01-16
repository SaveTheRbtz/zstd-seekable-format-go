package seekable

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
	offset uint64
}

func (s *seekableBufferReader) Read(p []byte) (n int, err error) {
	size := uint64(len(s.buf)) - s.offset
	if size > uint64(len(p)) {
		size = uint64(len(p))
	}

	copy(p, s.buf[s.offset:s.offset+size])

	s.offset += size
	return int(size), nil
}

func (s *seekableBufferReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		s.offset += uint64(offset)
	case io.SeekStart:
		s.offset = uint64(offset)
	case io.SeekEnd:
		s.offset = uint64(len(s.buf)) + uint64(offset)
	}
	return 0, nil
}

func TestReader(t *testing.T) {
	for _, b := range [][]byte{checksum, noChecksum} {
		br := &seekableBufferReader{buf: b}
		r, err := NewReader(br)
		assert.NoError(t, err)

		sr := r.(*seekableReaderImpl)
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
		assert.Equal(t, sr.cachedFrame, &cachedFrame{data: bytes1, offset: 0})

		m, err := r.Read(tmp)
		assert.NoError(t, err)
		assert.Equal(t, len(bytes2), m)
		assert.Equal(t, bytes2, tmp[:m])

		assert.Equal(t, int64(n)+int64(m), sr.offset)
		assert.Equal(t, sr.cachedFrame, &cachedFrame{data: bytes2, offset: uint64(len(bytes1))})

		_, err = r.Read(tmp)
		assert.Equal(t, err, io.EOF)

		assert.NoError(t, r.Close())
	}
}

func TestReaderEdges(t *testing.T) {
	source := []byte("testtest2")
	for _, b := range [][]byte{checksum, noChecksum} {
		sr := &seekableBufferReader{buf: b}
		r, err := NewReader(sr)
		assert.NoError(t, err)
		defer r.Close()

		for _, whence := range []int{io.SeekStart, io.SeekEnd} {
			for n := int64(0); n <= int64(len(source)); n += 1 {
				for m := int64(0); n <= int64(len(source)); n += 1 {
					var j int64
					switch whence {
					case io.SeekStart:
						j, err = r.Seek(n, whence)
					case io.SeekEnd:
						j, err = r.Seek(int64(-len(source))+n, whence)
					}
					assert.Equal(t, n, j)
					assert.NoError(t, err)

					tmp := make([]byte, m)
					k, err := r.Read(tmp)
					if n >= int64(len(source)) {
						assert.Equal(t, err, io.EOF, "should return EOF at %d, buf: %d, whence: %d",
							n, len(source), whence)
					} else {
						assert.NoError(t, err, "should not return err at %d, buf: %d, whence: %d",
							n, len(source), whence)
					}

					assert.Equal(t, source[n:n+int64(k)], tmp[:k])
				}
			}
		}
	}
}
