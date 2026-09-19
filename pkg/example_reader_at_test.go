package seekable_test

import (
	"bytes"
	"fmt"
	"io"
	"log"

	"github.com/klauspost/compress/zstd"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
)

func ExampleNewReader_readerAt() {
	compressed := exampleSeekableStream()
	var src io.ReaderAt = bytes.NewReader(compressed)
	compressedSize := int64(len(compressed))

	dec, err := zstd.NewReader(nil)
	if err != nil {
		log.Fatal(err)
	}
	defer dec.Close()

	r, err := seekable.NewReader(
		io.NewSectionReader(src, 0, compressedSize), dec,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	p := make([]byte, 5)
	if _, err := r.ReadAt(p, 6); err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(p))

	// Output:
	// World
}
