package seekable

import (
	"io"
	"os"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type bytesErr struct {
	name          string
	tag           uint32
	input         []byte
	expectedBytes []byte
	expectedErr   string
}

func TestCreateSkippableFrame(t *testing.T) {
	t.Parallel()

	for _, tab := range []bytesErr{
		{
			name:          "Empty",
			tag:           0x00,
			input:         []byte{},
			expectedBytes: nil,
		}, {
			name:          "TagOne",
			tag:           0x01,
			input:         []byte{'T'},
			expectedBytes: []byte{0x51, 0x2a, 0x4d, 0x18, 0x01, 0x00, 0x00, 0x00, 'T'},
		}, {
			name:          "InvalidTag",
			tag:           0xff,
			input:         []byte{'T'},
			expectedBytes: nil,
			expectedErr:   "requested tag (255) > 0xf",
		},
	} {
		tab := tab
		t.Run(tab.name, func(t *testing.T) {
			t.Parallel()

			actualBytes, err := createSkippableFrame(tab.tag, tab.input)
			if tab.expectedErr != "" {
				require.ErrorContains(t, err, tab.expectedErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tab.expectedBytes, actualBytes, "createSkippableFrame output does not match expected")

			dec, err := zstd.NewReader(nil)
			require.NoError(t, err)
			defer dec.Close()

			decodedBytes, err := dec.DecodeAll(actualBytes, nil)
			require.NoError(t, err)
			assert.Empty(t, decodedBytes)
		})
	}
}

func TestIntercompat(t *testing.T) {
	t.Parallel()

	const firstFrameSize = 1024
	licensePrefix := []byte("  [![License]")
	licenseSuffix := []byte("[license]: https://opensource.org/licenses/MIT\n")

	for _, fn := range []string{
		// t2sz README.md -l 22 -s 1024 -o intercompat-t2sz.zst
		"intercompat-t2sz.zst",
		// go run ./cmd/zstdseek -- \
		//	-f $(realpath README.md) -o $(realpath intercompat-zstdseek_v0.zst) \
		//	-c 1:1 -t -q 13
		"intercompat-zstdseek_v0.zst",
	} {
		fn := fn
		t.Run(fn, func(t *testing.T) {
			t.Parallel()

			dec, err := zstd.NewReader(nil)
			require.NoError(t, err)
			defer dec.Close()

			f, err := os.Open("./testdata/" + fn)
			require.NoError(t, err)
			defer func() { require.NoError(t, f.Close()) }()

			r, err := NewReader(f, dec)
			require.NoError(t, err)
			defer func() { require.NoError(t, r.Close()) }()

			buf := make([]byte, 4000)
			n, err := r.Read(buf)
			require.NoError(t, err)
			assert.Equal(t, firstFrameSize, n)
			assert.Equal(t, licensePrefix, buf[:len(licensePrefix)])

			all, err := io.ReadAll(r)
			require.NoError(t, err)
			assert.Greater(t, len(all), firstFrameSize)

			i, err := r.Seek(-int64(len(licenseSuffix)), io.SeekEnd)
			require.NoError(t, err)
			assert.Greater(t, i, int64(firstFrameSize))

			n, err = r.ReadAt(buf, i)
			require.ErrorIs(t, err, io.EOF)
			assert.Equal(t, len(licenseSuffix), n)
			assert.Equal(t, licenseSuffix, buf[:n])
		})
	}
}
