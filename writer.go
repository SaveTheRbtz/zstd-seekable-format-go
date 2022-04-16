package seekable

import (
	"fmt"
	"io"
	"sync"

	"go.uber.org/multierr"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/options"
)

// writerEnvImpl is the environment implementation of for the underlying WriteCloser.
type writerEnvImpl struct {
	w io.Writer
}

func (w *writerEnvImpl) WriteFrame(p []byte) (n int, err error) {
	return w.w.Write(p)
}

func (w *writerEnvImpl) WriteSeekTable(p []byte) (n int, err error) {
	return w.w.Write(p)
}

type writerImpl struct {
	enc          ZSTDEncoder
	frameEntries []seekTableEntry

	o options.WriterOptions

	once *sync.Once
}

var (
	_ io.Writer = (*writerImpl)(nil)
	_ io.Closer = (*writerImpl)(nil)
)

type Writer interface {
	// Write writes a chunk of data as a separate frame into the datastream.
	//
	// Note that Write does not do any coalescing nor splitting of data,
	// so each write will map to a separate ZSTD Frame.
	Write(src []byte) (int, error)

	// Close implement io.Closer interface.  It writes the seek table footer
	// and releases occupied memory.
	//
	// Caller is still responsible to Close the underlying writer.
	Close() (err error)
}

// ZSTDEncoder is the compressor.  Tested with github.com/klauspost/compress/zstd.
type ZSTDEncoder interface {
	EncodeAll(src, dst []byte) []byte
}

// NewWriter wraps the passed io.Writer and Encoder into and indexed ZSTD stream.
// Resulting stream then can be randomly accessed through the Reader and Decoder interfaces.
func NewWriter(w io.Writer, encoder ZSTDEncoder, opts ...options.WOption) (Writer, error) {
	sw := writerImpl{
		once: &sync.Once{},
		enc:  encoder,
	}

	sw.o.SetDefault()
	for _, o := range opts {
		err := o(&sw.o)
		if err != nil {
			return nil, err
		}
	}

	if sw.o.Env == nil {
		sw.o.Env = &writerEnvImpl{
			w: w,
		}
	}

	return &sw, nil
}

func (s *writerImpl) Write(src []byte) (int, error) {
	dst, err := s.Encode(src)
	if err != nil {
		return 0, err
	}

	n, err := s.o.Env.WriteFrame(dst)
	if err != nil {
		return 0, err
	}
	if n != len(dst) {
		return 0, fmt.Errorf("partial write: %d out of %d", n, len(dst))
	}

	return len(src), nil
}

func (s *writerImpl) Close() (err error) {
	s.once.Do(func() {
		err = multierr.Append(err, s.writeSeekTable())
	})
	return
}

func (s *writerImpl) writeSeekTable() error {
	seekTableBytes, err := s.EndStream()
	if err != nil {
		return err
	}

	_, err = s.o.Env.WriteSeekTable(seekTableBytes)
	return err
}
