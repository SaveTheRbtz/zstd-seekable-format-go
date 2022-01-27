package seekable_test

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/klauspost/compress/zstd"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go"
)

func Example() {
	f, err := os.CreateTemp("", "example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(f.Name())

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		log.Fatal(err)
	}
	defer enc.Close()

	w, err := seekable.NewWriter(f, enc)
	if err != nil {
		log.Fatal(err)
	}

	// Write data in chunks.
	for _, b := range [][]byte{[]byte("Hello"), []byte(" World!")} {
		_, err = w.Write(b)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Close and flush seek table.
	err = w.Close()
	if err != nil {
		log.Fatal(err)
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		log.Fatal(err)
	}

	r, err := seekable.NewReader(f, dec)
	if err != nil {
		log.Fatal(err)
	}

	ello := make([]byte, 4)
	// ReaderAt
	_, err = r.ReadAt(ello, 1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Offset: 1 from the start: %s\n", string(ello))

	world := make([]byte, 5)
	// Seeker
	_, err = r.Seek(-6, io.SeekEnd)
	if err != nil {
		log.Fatal(err)
	}
	// Reader
	_, err = r.Read(world)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Offset: -6 from the end: %s\n", string(world))

	_, _ = f.Seek(0, io.SeekStart)

	// Standard ZSTD Reader.
	dec, err = zstd.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}

	all, err := io.ReadAll(dec)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Whole string: %s\n", string(all))

	// Output:
	// Offset: 1 from the start: ello
	// Offset: -6 from the end: World
	// Whole string: Hello World!
}
