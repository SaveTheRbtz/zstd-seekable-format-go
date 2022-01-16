package main

import (
	"bytes"
	"crypto/sha256"
	"flag"
	"io"
	"log"
	"os"

	"github.com/jotfs/fastcdc-go"
	"go.uber.org/zap"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go"
)

var inputFlag, outputFlag string
var verifyFlag bool

func init() {
	flag.StringVar(&inputFlag, "f", "", "input filename")
	flag.StringVar(&outputFlag, "o", "", "output filename")
	flag.BoolVar(&verifyFlag, "t", false, "test reading after the write")
}

func main() {
	flag.Parse()

	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal("failed to initialize logger", err)
	}
	defer logger.Sync()

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

	w, err := seekable.NewWriter(output)
	if err != nil {
		logger.Fatal("failed to create compressed writer", zap.Error(err))
	}
	defer w.Close()

	opts := fastcdc.Options{
		MinSize:     4 * 1024,
		AverageSize: 16 * 1024,
		MaxSize:     64 * 1024,
	}
	chunker, err := fastcdc.NewChunker(input, opts)
	if err != nil {
		logger.Fatal("failed to create chunker", zap.Error(err))
	}

	expected := sha256.New()
	for {
		chunk, err := chunker.Next()
		if err != nil {
			if err == io.EOF {
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
		w.Write(chunk.Data)
	}
	w.Close()

	if verifyFlag {
		verify, err := os.Open(outputFlag)
		if err != nil {
			logger.Fatal("failed to open file for verification", zap.Error(err))
		}
		reader, err := seekable.NewReader(verify)
		if err != nil {
			logger.Fatal("failed to create new seekable reader", zap.Error(err))
		}

		chunk := make([]byte, 4096)
		actual := sha256.New()
		for {
			n, err := reader.Read(chunk)
			if err != nil {
				if err == io.EOF {
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
		}
	}
}
