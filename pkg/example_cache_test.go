package seekable_test

import (
	"bytes"
	"fmt"
	"log"

	"github.com/klauspost/compress/zstd"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/framecache"
)

func ExampleWithReaderFrameCache() {
	compressed := exampleSeekableStream()

	dec, err := zstd.NewReader(nil)
	if err != nil {
		log.Fatal(err)
	}
	defer dec.Close()

	// Keep up to two decoded frames, with a total limit of 1 MiB.
	cache := framecache.NewLRU(framecache.Limits{
		MaxFrames: 2,
		MaxBytes:  1 << 20,
	})
	r, err := seekable.NewReader(bytes.NewReader(compressed), dec,
		seekable.WithReaderFrameCache(cache),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = r.Close()
	}()

	// The last read reuses the first decoded frame.
	buf := make([]byte, 5)
	for _, offset := range []int64{0, 6, 0} {
		if _, err := r.ReadAt(buf, offset); err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(buf))
	}

	// Output:
	// Hello
	// World
	// Hello
}
