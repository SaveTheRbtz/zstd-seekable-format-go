package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	// TODO: move to a better fork, this one is pretty buggy
	"github.com/jotfs/fastcdc-go"
	"github.com/klauspost/compress/zstd"
	"github.com/zeebo/blake3"
	"go.uber.org/zap"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go"
)

func main() {
	var (
		inputFlag, chunkingFlag, outputFlag string
		qualityFlag                         int
		verifyFlag, verboseFlag             bool
	)

	flag.StringVar(&inputFlag, "f", "", "input filename")
	flag.StringVar(&outputFlag, "o", "", "output filename")
	flag.StringVar(&chunkingFlag, "c", "16:64:1024", "min:avg:max chunking block size (in kb)")
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

	var input *os.File
	if inputFlag == "-" {
		input = os.Stdin
	} else {
		if input, err = os.Open(inputFlag); err != nil {
			logger.Fatal("failed to open input", zap.Error(err))
		}
		defer input.Close()
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

	w, err := seekable.NewWriter(output, seekable.WithWLogger(logger), seekable.WithZSTDEOptions(zstdOpts...))
	if err != nil {
		logger.Fatal("failed to create compressed writer", zap.Error(err))
	}
	defer w.Close()

	ckunkParams := strings.SplitN(chunkingFlag, ":", 3)
	if len(ckunkParams) != 3 {
		logger.Fatal("failed parse chunker params. len() != 3", zap.Int("actual", len(ckunkParams)))
	}
	mustConv := func(s string) int {
		n, err := strconv.Atoi(s)
		if err != nil {
			logger.Fatal("failed to parse int", zap.String("int", s), zap.Error(err))
		}
		return n
	}
	opts := fastcdc.Options{
		MinSize:     mustConv(ckunkParams[0]) * 1024,
		AverageSize: mustConv(ckunkParams[1]) * 1024,
		MaxSize:     mustConv(ckunkParams[2]) * 1024,
	}
	chunker, err := fastcdc.NewChunker(input, opts)
	if err != nil {
		logger.Fatal("failed to create chunker", zap.Error(err))
	}

	expected := blake3.New()
	for {
		chunk, err := chunker.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			logger.Fatal("failed to read", zap.Error(err))
		}
		if verifyFlag {
			m, err := expected.Write(chunk.Data)
			if err != nil || m != chunk.Length {
				logger.Fatal("failed to update checksum", zap.Error(err))
			}
		}
		m, err := w.Write(chunk.Data)
		if err != nil || m != chunk.Length {
			logger.Fatal("failed to write data", zap.Error(err))
		}
	}
	w.Close()

	if verifyFlag {
		verify, err := os.Open(outputFlag)
		if err != nil {
			logger.Fatal("failed to open file for verification", zap.Error(err))
		}
		defer verify.Close()

		reader, err := seekable.NewReader(verify, seekable.WithRLogger(logger))
		if err != nil {
			logger.Fatal("failed to create new seekable reader", zap.Error(err))
		}
		defer reader.Close()

		chunk := make([]byte, 4096)
		actual := blake3.New()
		for {
			n, err := reader.Read(chunk)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				logger.Fatal("failed to read", zap.Error(err))
			}
			m, err := actual.Write(chunk[:n])
			if err != nil || m != n {
				logger.Fatal("failed to update checksum", zap.Error(err))
			}
		}

		if !bytes.Equal(actual.Sum(nil), expected.Sum(nil)) {
			logger.Fatal("checksum verification failed",
				zap.Binary("actual", actual.Sum(nil)), zap.Binary("expected", expected.Sum(nil)))
		} else {
			logger.Info("checksum verification succeeded", zap.Binary("actual", actual.Sum(nil)))
		}
	}
}
