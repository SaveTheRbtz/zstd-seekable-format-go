package seekable

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
)

type bytesErr struct {
	tag           uint32
	input         []byte
	expectedBytes []byte
	expectedErr   error
}

func TestCreateSkippableFrame(t *testing.T) {
	t.Parallel()

	dec, err := zstd.NewReader(nil)
	assert.NoError(t, err)

	for i, tab := range []bytesErr{
		{
			tag:           0x00,
			input:         []byte{},
			expectedBytes: nil,
			expectedErr:   nil,
		}, {
			tag:           0x01,
			input:         []byte{'T'},
			expectedBytes: []byte{0x51, 0x2a, 0x4d, 0x18, 0x01, 0x00, 0x00, 0x00, 'T'},
			expectedErr:   nil,
		}, {
			tag:           0xff,
			input:         []byte{'T'},
			expectedBytes: nil,
			expectedErr:   fmt.Errorf("requested tag (255) > 0xf"),
		},
	} {
		tab := tab
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			actualBytes, err := createSkippableFrame(tab.tag, tab.input)
			assert.Equal(t, tab.expectedErr, err, "createSkippableFrame err does not match expected")
			if tab.expectedErr == nil && err == nil {
				assert.Equal(t, tab.expectedBytes, actualBytes, "createSkippableFrame output does not match expected")
				decodedeBytes, err := dec.DecodeAll(actualBytes, nil)
				assert.NoError(t, err)
				assert.Equal(t, []byte(nil), decodedeBytes)
			}
		})
	}
}

func TestIntercompat(t *testing.T) {
	t.Parallel()

	dec, err := zstd.NewReader(nil)
	assert.NoError(t, err)

	for _, fn := range []string{
		// t2sz README.md -l 22 -s 1024 -o testdata/intercompat-t2sz.zst
		"testdata/intercompat-t2sz.zst",
		// bazel run //cmd/zstdseek:zstdseek -- \
		//	-f $(realpath README.md) -o $(realpath testdata/intercompat-zstdseek_v0.zst) \
		//	-c 1:1 -t -q 13
		"testdata/intercompat-zstdseek_v0.zst",
	} {
		fn := fn
		t.Run(fn, func(t *testing.T) {
			t.Parallel()

			f, err := os.Open(fn)
			assert.NoError(t, err)
			defer f.Close()

			r, err := NewReader(f, dec)
			assert.NoError(t, err)
			defer func() { assert.NoError(t, r.Close()) }()

			buf := make([]byte, 4000)
			n, err := r.Read(buf)
			assert.NoError(t, err)
			assert.Equal(t, n, 1024)
			assert.Equal(t, []byte("  [![License]"), buf[:13])

			all, err := io.ReadAll(r)
			assert.NoError(t, err)
			assert.Greater(t, len(all), 1024)

			i, err := r.Seek(-47, io.SeekEnd)
			assert.NoError(t, err)
			assert.Greater(t, i, int64(1024))

			n, err = r.ReadAt(buf, i)
			assert.ErrorIs(t, err, io.EOF)
			assert.Equal(t, 47, n)
			assert.Equal(t, []byte("[license]: https://opensource.org/licenses/MIT\n"), buf[:n])
		})
	}
}
