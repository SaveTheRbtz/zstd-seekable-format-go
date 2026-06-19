[![GoDoc][doc-img]][doc] [![License][license-img]][license] [![OpenSSF Scorecard][scorecard-img]][scorecard] [![Build Status][ci-img]][ci] [![Go Report][report-img]][report]

# zstd-seekable-format-go

Package `seekable` adds random-access reads to compressed Zstandard data.

It writes compressed streams with a seek table and reads them by decompressed
byte offset through `Reader`, `ReadAt`, and `Seek`. The resulting stream remains
valid Zstandard data, so standard Zstandard readers can still decode it
sequentially.

The wire format is the [Zstandard seekable format][format]: compressed frames
followed by a seek table in a skippable frame.

The package uses small encoder/decoder interfaces and is tested with
[`github.com/klauspost/compress/zstd`][klauspost-zstd].

## Install

```sh
go get github.com/SaveTheRbtz/zstd-seekable-format-go/pkg
```

```go
import seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
```

## Write

```go
func writeSeekable(dst io.Writer, chunks [][]byte) error {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return err
	}
	defer enc.Close()

	w, err := seekable.NewWriter(dst, enc)
	if err != nil {
		return err
	}
	for _, chunk := range chunks {
		if _, err := w.Write(chunk); err != nil {
			return err
		}
	}
	return w.Close()
}
```

`Writer.Close` writes the final seek table. Without it, `Reader` and
`NewSeekTable` cannot find the random-access metadata.

## Read

```go
func readAt(src io.ReadSeeker, off int64, p []byte) error {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return err
	}
	defer dec.Close()

	r, err := seekable.NewReader(src, dec)
	if err != nil {
		return err
	}
	defer r.Close()

	_, err = r.ReadAt(p, off)
	return err
}
```

Offsets are decompressed byte offsets. `Reader` implements `io.Reader`,
`io.ReaderAt`, `io.Seeker`, and `io.Closer`.

## Metadata

```go
func frameForOffset(r *seekable.Reader, off uint64) (seekable.FrameOffsetEntry, error) {
	table, err := r.SeekTable()
	if err != nil {
		return seekable.FrameOffsetEntry{}, err
	}
	entry, ok := table.EntryByDecompressedOffset(off)
	if !ok {
		return seekable.FrameOffsetEntry{}, io.EOF
	}
	return entry, nil
}
```

`SeekTable` exposes the decompressed size, frame count, checksum flag, and frame
lookup by id or decompressed offset.

## Compatibility

Seekable streams are valid Zstandard streams:

```go
func readSequential(src io.Reader) ([]byte, error) {
	zr, err := zstd.NewReader(src)
	if err != nil {
		return nil, err
	}
	defer zr.Close()

	return io.ReadAll(zr)
}
```

## Command Line

Install the `zstdseek` command with:

```sh
go install github.com/SaveTheRbtz/zstd-seekable-format-go/cmd/zstdseek@latest
```

Compress a file into a seekable Zstandard stream:

```sh
zstdseek -f input.dat -o input.dat.zst
```

Add `-t` to verify the compressed output after writing:

```sh
zstdseek -f input.dat -o input.dat.zst -t
```

Use `-` for stdin or stdout. Verification requires a named output file.

```sh
zstdseek -f - -o input.dat.zst < input.dat
zstdseek -f input.dat -o - > input.dat.zst
```

The main tuning flags are `-q` for Zstandard compression quality and `-c` for
content-defined chunk sizes in `min:avg:max` KiB form.

[format]: https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md
[klauspost-zstd]: https://pkg.go.dev/github.com/klauspost/compress/zstd

[doc-img]: https://pkg.go.dev/badge/github.com/SaveTheRbtz/zstd-seekable-format-go/pkg
[doc]: https://pkg.go.dev/github.com/SaveTheRbtz/zstd-seekable-format-go/pkg
[ci-img]: https://github.com/SaveTheRbtz/zstd-seekable-format-go/actions/workflows/go.yml/badge.svg
[ci]: https://github.com/SaveTheRbtz/zstd-seekable-format-go/actions/workflows/go.yml
[report-img]: https://goreportcard.com/badge/github.com/SaveTheRbtz/zstd-seekable-format-go/pkg
[report]: https://goreportcard.com/report/github.com/SaveTheRbtz/zstd-seekable-format-go/pkg
[license-img]: https://img.shields.io/badge/License-MIT-blue.svg
[license]: https://opensource.org/licenses/MIT
[scorecard-img]: https://api.scorecard.dev/projects/github.com/SaveTheRbtz/zstd-seekable-format-go/badge
[scorecard]: https://scorecard.dev/viewer/?uri=github.com/SaveTheRbtz/zstd-seekable-format-go
