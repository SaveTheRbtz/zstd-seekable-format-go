package seekable_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"

	"github.com/klauspost/compress/zstd"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
)

func exampleFrames() [][]byte {
	return [][]byte{[]byte("Hello"), []byte(" "), []byte("World!")}
}

func writeExampleFrames(w *seekable.Writer) {
	for _, frame := range exampleFrames() {
		if _, err := w.Write(frame); err != nil {
			log.Fatal(err)
		}
	}
}

func exampleSeekableStream() []byte {
	var buf bytes.Buffer

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := enc.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	w, err := seekable.NewWriter(&buf, enc)
	if err != nil {
		log.Fatal(err)
	}
	writeExampleFrames(w)
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	return buf.Bytes()
}

func ExampleNewWriter() {
	compressed := exampleSeekableStream()

	dec, err := zstd.NewReader(bytes.NewReader(compressed))
	if err != nil {
		log.Fatal(err)
	}
	defer dec.Close()

	all, err := io.ReadAll(dec)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(all))

	// Output:
	// Hello World!
}

func ExampleNewReader() {
	compressed := exampleSeekableStream()

	dec, err := zstd.NewReader(nil)
	if err != nil {
		log.Fatal(err)
	}
	defer dec.Close()

	r, err := seekable.NewReader(bytes.NewReader(compressed), dec)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = r.Close()
	}()

	ello := make([]byte, 4)
	if _, err := r.ReadAt(ello, 1); err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(ello))

	world := make([]byte, 5)
	if _, err := r.Seek(-6, io.SeekEnd); err != nil {
		log.Fatal(err)
	}
	if _, err := r.Read(world); err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(world))

	// Output:
	// ello
	// World
}

func ExampleNewSeekTable() {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := enc.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	e, err := seekable.NewEncoder(enc)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := e.Encode([]byte("Hello")); err != nil {
		log.Fatal(err)
	}
	if _, err := e.Encode([]byte(" World!")); err != nil {
		log.Fatal(err)
	}

	seekTableFrame, err := e.EndStream()
	if err != nil {
		log.Fatal(err)
	}
	table, err := seekable.NewSeekTable(seekTableFrame)
	if err != nil {
		log.Fatal(err)
	}
	entry, ok := table.EntryByDecompressedOffset(7)
	if !ok {
		log.Fatal("missing seek-table entry")
	}

	fmt.Printf("frames=%d size=%d checksums=%t\n", table.NumFrames(), table.Size(), table.HasChecksums())
	fmt.Printf("offset 7 is in frame %d\n", entry.ID)

	// Output:
	// frames=2 size=12 checksums=true
	// offset 7 is in frame 1
}

func ExampleWriter_WriteMany() {
	var buf bytes.Buffer

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := enc.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	w, err := seekable.NewWriter(&buf, enc)
	if err != nil {
		log.Fatal(err)
	}

	frames := exampleFrames()
	next := func() ([]byte, error) {
		if len(frames) == 0 {
			return nil, nil
		}
		frame := frames[0]
		frames = frames[1:]
		return frame, nil
	}

	err = w.WriteMany(context.Background(), next,
		seekable.WithConcurrency(2),
		seekable.WithWriteCallback(func(size uint32) {
			fmt.Println(size)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	// Output:
	// 5
	// 1
	// 6
}
