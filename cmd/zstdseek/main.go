package main

import (
	"bytes"
	"context"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/SaveTheRbtz/fastcdc-go"
	"github.com/klauspost/compress/zstd"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/term"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
)

func newLogger(verbose bool) *slog.Logger {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}

func resolveInputOutput(args []string, outputFlag string, verify, stdoutIsTerminal bool) (inputName, outputName string, err error) {
	switch len(args) {
	case 0:
		inputName = "-"
	case 1:
		inputName = args[0]
	default:
		return "", "", errors.New("expected at most one input file")
	}
	if outputFlag == "" {
		outputFlag = "-"
	}
	if outputFlag == "-" {
		if verify {
			return "", "", errors.New("verify can't be used with stdout output")
		}
		if stdoutIsTerminal {
			return "", "", errors.New("refusing to write compressed data to terminal; use -o or redirect stdout")
		}
	}
	return inputName, outputFlag, nil
}

func main() {
	ctx := context.Background()

	var (
		outputFlag, levelFlag   string
		chunkSizeFlag           int
		threadsFlag             int
		verifyFlag, verboseFlag bool
	)

	flag.StringVar(&outputFlag, "o", "", "output filename (default: stdout)")
	flag.IntVar(&chunkSizeFlag, "chunk-size", 1024, "average chunk size in KiB (power of two, 1-4096)")
	flag.BoolVar(&verifyFlag, "verify", false, "verify output after writing (requires -o)")
	flag.StringVar(&levelFlag, "level", "fastest", "compression level: fastest, default, better, or best")
	flag.IntVar(&threadsFlag, "threads", 0, "number of compression workers (0 = automatic)")
	flag.BoolVar(&verboseFlag, "v", false, "be verbose")

	flag.Usage = func() {
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [options] [input]\n\nCompress input (or stdin) to a seekable Zstandard stream.\n\nOptions:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	var err error
	logger := newLogger(verboseFlag)
	fatal := func(msg string, attrs ...slog.Attr) {
		logger.LogAttrs(ctx, slog.LevelError, msg, attrs...)
		os.Exit(1)
	}
	seekableLogger := logger.WithGroup("seekable")

	inputName, outputName, err := resolveInputOutput(flag.Args(), outputFlag, verifyFlag, term.IsTerminal(int(os.Stdout.Fd())))
	if err != nil {
		fatal("invalid input/output options", slog.Any("error", err))
	}
	if threadsFlag < 0 {
		fatal("compression workers must be non-negative", slog.Int("threads", threadsFlag))
	}
	levelOK, level := zstd.EncoderLevelFromString(levelFlag)
	if !levelOK {
		fatal("invalid compression level", slog.String("level", levelFlag))
	}
	if chunkSizeFlag < 1 || chunkSizeFlag > 4096 || chunkSizeFlag&(chunkSizeFlag-1) != 0 {
		fatal("invalid average chunk size",
			slog.Int("chunk_size_kib", chunkSizeFlag),
			slog.String("expected", "power of two from 1 through 4096"))
	}
	avgChunkSize := chunkSizeFlag * 1024
	logger.Debug("setting chunker params", slog.Int("average", avgChunkSize))
	chunker, err := fastcdc.New(fastcdc.Config{AverageSize: avgChunkSize})
	if err != nil {
		fatal("failed to create chunker", slog.Any("error", err))
	}

	bar := progressbar.DefaultSilent(0, "")

	inputFile := os.Stdin
	if inputName != "-" {
		if inputFile, err = os.Open(inputName); err != nil {
			fatal("failed to open input", slog.Any("error", err))
		}

		if term.IsTerminal(int(os.Stderr.Fd())) {
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

	var input io.Reader = inputFile

	expected := sha512.New512_256()
	if verifyFlag {
		input = io.TeeReader(inputFile, expected)
	}

	output := os.Stdout
	if outputName != "-" {
		output, err = os.OpenFile(outputName, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0o644)
		if err != nil {
			fatal("failed to open output", slog.Any("error", err))
		}
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
	if err != nil {
		fatal("failed to create zstd encoder", slog.Any("error", err))
	}

	w, err := seekable.NewWriter(output, enc, seekable.WithWriterLogger(seekableLogger.WithGroup("writer")))
	if err != nil {
		fatal("failed to create compressed writer", slog.Any("error", err))
	}

	chunkReader := chunker.NewReader(input)

	frameSource := func() ([]byte, error) {
		chunk, err := chunkReader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, nil
			}
			return nil, err
		}
		// Reader invalidates the data after calling Next, so we need to clone it.
		return bytes.Clone(chunk), nil
	}

	writeOpts := []seekable.WriteManyOption{
		seekable.WithWriteCallback(func(entry seekable.FrameOffsetEntry) {
			_ = bar.Add(int(entry.DecompressedSize))
		}),
	}
	if threadsFlag > 0 {
		writeOpts = append(writeOpts, seekable.WithConcurrency(threadsFlag))
	}
	err = w.WriteMany(ctx, frameSource, writeOpts...)
	if err != nil {
		fatal("failed to write data", slog.Any("error", err))
	}

	_ = bar.Finish()
	if err := w.Close(); err != nil {
		fatal("failed to finalize compressed output", slog.Any("error", err))
	}
	if err := enc.Close(); err != nil {
		fatal("failed to close zstd encoder", slog.Any("error", err))
	}
	if outputName != "-" {
		if err := output.Close(); err != nil {
			fatal("failed to close output", slog.Any("error", err))
		}
	}
	if inputName != "-" {
		if err := inputFile.Close(); err != nil {
			fatal("failed to close input", slog.Any("error", err))
		}
	}

	if verifyFlag {
		logger.Info("verifying checksum")

		verify, err := os.Open(outputName)
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
