package seekable_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go"
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
			actualBytes, err := seekable.CreateSkippableFrame(tab.tag, tab.input)
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
