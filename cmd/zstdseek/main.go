package main

import (
	"bytes"
	"context"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/SaveTheRbtz/fastcdc-go"
	"github.com/klauspost/compress/zstd"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/term"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
)

type readCloser struct {
	io.Reader
	io.Closer
}

func newLogger(verbose bool) *slog.Logger {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}

func main() {
	ctx := context.Background()

	var (
		inputFlag, chunkingFlag, outputFlag string
		qualityFlag                         int
		verifyFlag, verboseFlag             bool
	)

	flag.StringVar(&inputFlag, "f", "", "input filename")
	flag.StringVar(&outputFlag, "o", "", "output filename")
	flag.StringVar(&chunkingFlag, "c", "128:1024:8192", "min:avg:max chunking block size (in kb)")
	flag.BoolVar(&verifyFlag, "t", false, "test reading after the write")
	flag.IntVar(&qualityFlag, "q", 1, "compression quality (lower == faster)")
	flag.BoolVar(&verboseFlag, "v", false, "be verbose")

	flag.Parse()

	var err error
	logger := newLogger(verboseFlag)
	fatal := func(msg string, attrs ...slog.Attr) {
		logger.LogAttrs(ctx, slog.LevelError, msg, attrs...)
		os.Exit(1)
	}
	seekableLogger := logger.WithGroup("seekable")

	if inputFlag == "" || outputFlag == "" {
		fatal("both input and output files need to be defined")
	}
	if verifyFlag && outputFlag == "-" {
		fatal("verify can't be used with stdout output")
	}

	bar := progressbar.DefaultSilent(0, "")

	inputFile := os.Stdin
	if inputFlag != "-" {
		if inputFile, err = os.Open(inputFlag); err != nil {
			fatal("failed to open input", slog.Any("error", err))
		}

		if term.IsTerminal(int(os.Stdout.Fd())) {
			size := int64(-1)
			stat, err := inputFile.Stat()
			if err == nil {
				size = stat.Size()
			}

			bar = progressbar.DefaultBytes(
				size,
				"compressing",
			)
		}
	}

	var input io.ReadCloser = inputFile

	expected := sha512.New512_256()
	origDone := make(chan struct{})
	if verifyFlag {
		pr, pw := io.Pipe()

		tee := io.TeeReader(inputFile, pw)
		input = readCloser{tee, pw}

		go func() {
			defer close(origDone)

			m, err := io.CopyBuffer(expected, pr, make([]byte, 128<<10))
			if err != nil {
				fatal("failed to compute expected csum", slog.Int64("processed", m), slog.Any("error", err))
			}
		}()
	}

	output := os.Stdout
	if outputFlag != "-" {
		output, err = os.OpenFile(outputFlag, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0o644)
		if err != nil {
			fatal("failed to open output", slog.Any("error", err))
		}
		defer output.Close()
	}

	chunkParams := strings.Split(chunkingFlag, ":")
	if len(chunkParams) != 3 {
		fatal("failed parse chunker params. len() != 3", slog.Int("actual", len(chunkParams)))
	}
	mustConv := func(s string) int {
		n, err := strconv.Atoi(s)
		if err != nil {
			fatal("failed to parse int", slog.String("string", s), slog.Any("error", err))
		}
		return n
	}
	minChunkSize := mustConv(chunkParams[0]) * 1024
	avgChunkSize := mustConv(chunkParams[1]) * 1024
	maxChunkSize := mustConv(chunkParams[2]) * 1024

	var zstdOpts []zstd.EOption = []zstd.EOption{
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(qualityFlag)),
	}
	enc, err := zstd.NewWriter(nil, zstdOpts...)
	if err != nil {
		fatal("failed to create zstd encoder", slog.Any("error", err))
	}

	w, err := seekable.NewWriter(output, enc, seekable.WithWriterLogger(seekableLogger.WithGroup("writer")))
	if err != nil {
		fatal("failed to create compressed writer", slog.Any("error", err))
	}
	defer w.Close()

	// convert average chunk size to a number of bits
	logger.Debug("setting chunker params", slog.Int("min", minChunkSize), slog.Int("max", maxChunkSize))
	chunker, err := fastcdc.NewChunker(
		input,
		fastcdc.Options{
			MinSize:     minChunkSize,
			AverageSize: avgChunkSize,
			MaxSize:     maxChunkSize,
		},
	)
	if err != nil {
		fatal("failed to create chunker", slog.Any("error", err))
	}

	frameSource := func() ([]byte, error) {
		chunk, err := chunker.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, nil
			}
			return nil, err
		}
		// Chunker invalidates the data after calling Next, so we need to clone it
		return bytes.Clone(chunk.Data), nil
	}

	err = w.WriteMany(ctx, frameSource, seekable.WithWriteCallback(func(entry seekable.FrameOffsetEntry) {
		_ = bar.Add(int(entry.DecompressedSize))
	}))
	if err != nil {
		fatal("failed to write data", slog.Any("error", err))
	}

	_ = bar.Finish()
	input.Close()
	w.Close()

	if verifyFlag {
		logger.Info("verifying checksum")

		verify, err := os.Open(outputFlag)
		if err != nil {
			fatal("failed to open file for verification", slog.Any("error", err))
		}
		defer verify.Close()

		dec, err := zstd.NewReader(nil)
		if err != nil {
			fatal("failed to create zstd decompressor", slog.Any("error", err))
		}
		defer dec.Close()

		reader, err := seekable.NewReader(verify, dec, seekable.WithReaderLogger(seekableLogger.WithGroup("reader")))
		if err != nil {
			fatal("failed to create new seekable reader", slog.Any("error", err))
		}

		actual := sha512.New512_256()
		m, err := io.CopyBuffer(actual, reader, make([]byte, 128<<10))
		if err != nil {
			fatal("failed to compute actual csum", slog.Int64("processed", m), slog.Any("error", err))
		}
		<-origDone

		actualSum := actual.Sum(nil)
		expectedSum := expected.Sum(nil)
		if !bytes.Equal(actualSum, expectedSum) {
			fatal("checksum verification failed",
				slog.String("actual", hex.EncodeToString(actualSum)),
				slog.String("expected", hex.EncodeToString(expectedSum)))
		} else {
			logger.Info("checksum verification succeeded", slog.String("actual", hex.EncodeToString(actualSum)))
		}
	}
}
