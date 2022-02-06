  [![License][license-img]][license] [![GoDoc][doc-img]][doc] [![Build Status][ci-img]][ci] [![Go Report][report-img]][report]
# ZSTD seekable compression format implementation in Go
[Seekable ZSTD compression format](https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md) implemented in Golang.

This library provides a random access reader (using uncompressed file offsets) for ZSTD-compressed streams.  This can be used for creating transparent compression layers.  Coupled with Content Defined Chunking (CDC) it can also be used as a robust de-duplication layer.
## Installation

`go get -u github.com/SaveTheRbtz/zstd-seekable-format-go`

## Using the seekable format

Writing is done through the `Writer` interface:
```go
import (
	"github.com/klauspost/compress/zstd"
	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go"
)

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
```
NB! Do not forget to call `Close` since it is responsible for flushing the seek table.

Reading can either be done through `ReaderAt` interface:

```go
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
r.ReadAt(ello, 1)
if !bytes.Equal(ello, []byte("ello")) {
	log.Fatalf("%+v != ello", ello)
}
```

Or through the `ReadSeeker`:
```go
world := make([]byte, 5)
// Seeker
r.Seek(-6, io.SeekEnd)
// Reader
r.Read(world)
if !bytes.Equal(world, []byte("World")) {
	log.Fatalf("%+v != World", world)
}
```

Seekable format utilizes [ZSTD skippable frames](https://github.com/facebook/zstd/blob/release/doc/zstd_compression_format.md#skippable-frames) so it is a valid ZSTD stream:

```go
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
```

[doc-img]: https://pkg.go.dev/badge/github.com/SaveTheRbtz/zstd-seekable-format-go
[doc]: https://pkg.go.dev/github.com/SaveTheRbtz/zstd-seekable-format-go
[ci-img]: https://github.com/SaveTheRbtz/zstd-seekable-format-go/actions/workflows/bazel.yml/badge.svg
[ci]: https://github.com/SaveTheRbtz/zstd-seekable-format-go/actions/workflows/bazel.yml
[report-img]: https://goreportcard.com/badge/SaveTheRbtz/zstd-seekable-format-go
[report]: https://goreportcard.com/report/SaveTheRbtz/zstd-seekable-format-go
[license-img]: https://img.shields.io/badge/License-MIT-blue.svg
[license]: https://opensource.org/licenses/MIT
