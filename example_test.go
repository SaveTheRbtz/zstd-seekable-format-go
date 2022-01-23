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

	w, err := seekable.NewWriter(f, seekable.WithZSTDEOptions(zstd.WithEncoderLevel(zstd.SpeedFastest)))
	if err != nil {
		log.Fatal(err)
	}

	helloWorld := []byte("Hello World!")
	// Writer
	_, err = w.Write(helloWorld)
	if err != nil {
		log.Fatal(err)
	}
	// Closer
	err = w.Close()
	if err != nil {
		log.Fatal(err)
	}

	r, err := seekable.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

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

	// Reset
	_, _ = f.Seek(0, io.SeekStart)

	// Standard ZSTD Reader
	dec, err := zstd.NewReader(f)
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
