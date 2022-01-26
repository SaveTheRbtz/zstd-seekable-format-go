package seekable

import (
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"

	"go.uber.org/multierr"
)

var (
	_ io.Writer = (*WriterImpl)(nil)
	_ io.Closer = (*WriterImpl)(nil)
)

// Environment can be used to inject a custom file reader that is different from normal ReadSeeker.
// This is useful when, for example there is a custom chunking code.
type WEnvironment interface {
	// WriteFrame is called each time frame is encoded and needs to be written upstream.
	WriteFrame(p []byte) (n int, err error)
	// WriteSeekTable is called on Close to flush the seek table.
	WriteSeekTable(p []byte) (n int, err error)
}

// writerEnvImpl is the environment implementation of for the underlying ReadSeeker.
type writerEnvImpl struct {
	w io.Writer
}

func (w *writerEnvImpl) WriteFrame(p []byte) (n int, err error) {
	return w.w.Write(p)
}

func (w *writerEnvImpl) WriteSeekTable(p []byte) (n int, err error) {
	return w.w.Write(p)
}

type WriterImpl struct {
	enc          *zstd.Encoder
	frameEntries []SeekTableEntry

	o writerOptions

	once *sync.Once
}

type ZSTDWriter interface {
	io.WriteCloser
}

// NewWriter wraps the passed writer into with an indexer and ZSTD encoder.
// Written data then can be randomly accessed through the NewReader's interface.
func NewWriter(w io.Writer, opts ...WOption) (ZSTDWriter, error) {
	sw := WriterImpl{
		once: &sync.Once{},
	}

	sw.o.setDefault()
	for _, o := range opts {
		err := o(&sw.o)
		if err != nil {
			return nil, err
		}
	}

	if sw.o.env == nil {
		sw.o.env = &writerEnvImpl{
			w: w,
		}
	}

	var err error
	sw.enc, err = zstd.NewWriter(nil, sw.o.zstdEOpts...)
	if err != nil {
		return nil, err
	}
	return &sw, nil
}

// Write writes a chunk of data as a separate frame into the datastream.
//
// Note that Write does not do any coalescing nor splitting of data,
// so each write will map to a separate ZSTD Frame.
func (s *WriterImpl) Write(src []byte) (int, error) {
	dst, err := s.Encode(src)
	if err != nil {
		return 0, err
	}

	n, err := s.o.env.WriteFrame(dst)
	if err != nil {
		return 0, err
	}
	if n != len(dst) {
		return 0, fmt.Errorf("partial write: %d out of %d", n, len(dst))
	}

	return len(src), nil
}

// Close implement io.Closer interface.  It writes the seek table footer
// and releases occupied memory.
//
// Caller is still responsible to Close the underlying writer.
func (s *WriterImpl) Close() (err error) {
	s.once.Do(func() {
		err = multierr.Append(err, s.writeSeekTable())
	})
	return
}

func (s *WriterImpl) writeSeekTable() error {
	seekTableBytes, err := s.EndStream()
	if err != nil {
		return err
	}

	_, err = s.o.env.WriteSeekTable(seekTableBytes)
	return err
}
