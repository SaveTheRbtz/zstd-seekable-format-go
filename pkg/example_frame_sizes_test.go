package seekable_test

import (
	"bytes"
	"fmt"
	"io"
	"log"

	"github.com/klauspost/compress/zstd"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
)

func ExampleWriter_Write_frameSizes() {
	var compressed bytes.Buffer

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := enc.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	w, err := seekable.NewWriter(&compressed, enc)
	if err != nil {
		log.Fatal(err)
	}

	src := bytes.NewReader([]byte("Hello World!"))
	chunk := make([]byte, 5)
	for {
		n, err := io.ReadFull(src, chunk)
		if n > 0 {
			if _, err := w.Write(chunk[:n]); err != nil {
				log.Fatal(err)
			}
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		log.Fatal(err)
	}
	defer dec.Close()

	r, err := seekable.NewReader(bytes.NewReader(compressed.Bytes()), dec)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = r.Close()
	}()

	table, err := r.SeekTable()
	if err != nil {
		log.Fatal(err)
	}
	for id := int64(0); id < table.NumFrames(); id++ {
		entry, ok := table.EntryByID(id)
		if !ok {
			log.Fatal("missing seek-table entry")
		}
		fmt.Printf("frame %d: %d bytes\n", id, entry.DecompressedSize)
	}

	all, err := io.ReadAll(r)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(all))

	// Output:
	// frame 0: 5 bytes
	// frame 1: 5 bytes
	// frame 2: 2 bytes
	// Hello World!
}
