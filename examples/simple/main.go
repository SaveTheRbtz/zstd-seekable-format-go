package main

import (
	"bytes"
	"io"
	"log"
	"os"

	"github.com/klauspost/compress/zstd"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go"
)

func main() {
	f, err := os.CreateTemp("", "example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(f.Name())

	w, err := seekable.NewWriter(f, seekable.WithZSTDWOptions(zstd.WithEncoderLevel(zstd.SpeedFastest)))
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
	r.ReadAt(ello, 1)
	if !bytes.Equal(ello, []byte("ello")) {
		log.Fatalf("%+v != ello", ello)
	}

	world := make([]byte, 5)
	// Seeker
	r.Seek(-6, io.SeekEnd)
	// Reader
	r.Read(world)
	if !bytes.Equal(world, []byte("World")) {
		log.Fatalf("%+v != World", world)
	}

	// Standard ZSTD Reader
	f.Seek(0, io.SeekStart)
	dec, err := zstd.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}

	all, err := io.ReadAll(dec)
	if err != nil {
		log.Fatal(err)
	}
	if !bytes.Equal(all, []byte("Hello World!")) {
		log.Fatalf("%+v != Hello World!", all)
	}
}
