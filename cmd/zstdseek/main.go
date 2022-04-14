package main

import (
	"bytes"
	"crypto/sha512"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/reusee/fastcdc-go"
	"go.uber.org/zap"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go"
	"github.com/SaveTheRbtz/zstd-seekable-format-go/options"
)

type readCloser struct {
	io.Reader
	io.Closer
}

func main() {
	var (
		inputFlag, chunkingFlag, outputFlag string
		qualityFlag                         int
		verifyFlag, verboseFlag             bool
	)

	flag.StringVar(&inputFlag, "f", "", "input filename")
	flag.StringVar(&outputFlag, "o", "", "output filename")
	flag.StringVar(&chunkingFlag, "c", "16:64:128", "min:avg:max chunking block size (in kb)")
	flag.BoolVar(&verifyFlag, "t", false, "test reading after the write")
	flag.IntVar(&qualityFlag, "q", 1, "compression quality (lower == faster)")
	flag.BoolVar(&verboseFlag, "v", false, "be verbose")

	flag.Parse()

	var err error
	var logger *zap.Logger
	if verboseFlag {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}
	if err != nil {
		log.Fatal("failed to initialize logger", err)
	}
	defer func() {
		_ = logger.Sync()
	}()

	if inputFlag == "" || outputFlag == "" {
		logger.Fatal("both input and output files need to be defined")
	}
	if verifyFlag && outputFlag == "-" {
		logger.Fatal("verify can't be used with stdout output")
	}

	var input io.ReadCloser
	if inputFlag == "-" {
		input = os.Stdin
	} else {
		if input, err = os.Open(inputFlag); err != nil {
			logger.Fatal("failed to open input", zap.Error(err))
		}
	}

	expected := sha512.New512_256()
	origDone := make(chan struct{})
	if verifyFlag {
		pr, pw := io.Pipe()

		tee := io.TeeReader(input, pw)
		input = readCloser{tee, pw}

		go func() {
			defer close(origDone)

			m, err := io.CopyBuffer(expected, pr, make([]byte, 128<<10))
			if err != nil {
				logger.Fatal("failed to compute expected csum", zap.Int64("processed", m), zap.Error(err))
			}
		}()
	}

	var output *os.File
	if outputFlag == "-" {
		output = os.Stdout
	} else {
		output, err = os.OpenFile(outputFlag, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			logger.Fatal("failed to open output", zap.Error(err))
		}
		defer output.Close()
	}

	var zstdOpts []zstd.EOption
	zstdOpts = append(zstdOpts, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(qualityFlag)))
	enc, err := zstd.NewWriter(nil, zstdOpts...)
	if err != nil {
		logger.Fatal("failed to create zstd encoder", zap.Error(err))
	}

	w, err := seekable.NewWriter(output, enc, options.WithWLogger(logger))
	if err != nil {
		logger.Fatal("failed to create compressed writer", zap.Error(err))
	}
	defer w.Close()

	chunkParams := strings.SplitN(chunkingFlag, ":", 3)
	if len(chunkParams) != 3 {
		logger.Fatal("failed parse chunker params. len() != 3", zap.Int("actual", len(chunkParams)))
	}
	mustConv := func(s string) int {
		n, err := strconv.Atoi(s)
		if err != nil {
			logger.Fatal("failed to parse int", zap.String("int", s), zap.Error(err))
		}
		return n
	}
	opts := fastcdc.Options{
		MinSize:     mustConv(chunkParams[0]) * 1024,
		AverageSize: mustConv(chunkParams[1]) * 1024,
		MaxSize:     mustConv(chunkParams[2]) * 1024,
	}
	chunker, err := fastcdc.NewChunker(input, opts)
	if err != nil {
		logger.Fatal("failed to create chunker", zap.Error(err))
	}

	for {
		chunk, err := chunker.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			logger.Fatal("failed to read", zap.Error(err))
		}
		_, err = w.Write(chunk.Data)
		if err != nil {
			logger.Fatal("failed to write data", zap.Error(err))
		}
	}
	input.Close()
	w.Close()

	if verifyFlag {
		verify, err := os.Open(outputFlag)
		if err != nil {
			logger.Fatal("failed to open file for verification", zap.Error(err))
		}
		defer verify.Close()

		dec, err := zstd.NewReader(nil)
		if err != nil {
			logger.Fatal("failed to create zstd decompressor", zap.Error(err))
		}
		defer dec.Close()

		reader, err := seekable.NewReader(verify, dec, options.WithRLogger(logger))
		if err != nil {
			logger.Fatal("failed to create new seekable reader", zap.Error(err))
		}

		actual := sha512.New512_256()
		m, err := io.CopyBuffer(actual, reader, make([]byte, 128<<10))
		if err != nil {
			logger.Fatal("failed to compute actual csum", zap.Int64("processed", m), zap.Error(err))
		}
		<-origDone

		if !bytes.Equal(actual.Sum(nil), expected.Sum(nil)) {
			logger.Fatal("checksum verification failed",
				zap.Binary("actual", actual.Sum(nil)), zap.Binary("expected", expected.Sum(nil)))
		} else {
			logger.Info("checksum verification succeeded", zap.Binary("actual", actual.Sum(nil)))
		}
	}
}
