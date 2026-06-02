package seekable

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync"

	"golang.org/x/sync/errgroup"
)

var (
	errWriterFailed = errors.New("writer has failed")
)

// writerEnvImpl is the environment implementation for the underlying WriteCloser.
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

	logger *slog.Logger
	env    WriterEnvironment
	failed bool
	closed bool

	mu sync.Mutex
}

var (
	_ io.Writer = (*writerImpl)(nil)
	_ io.Closer = (*writerImpl)(nil)
)

// Writer writes a seekable Zstandard stream.
//
// Each non-empty Write call becomes one Zstandard frame in the output stream.
// Close must be called to write the final seek-table skippable frame; without
// it, Reader and NewSeekTable cannot find the random-access metadata.
// Close is idempotent. Write and WriteMany return ErrClosed after Close.
type Writer interface {
	// Write writes a chunk of data as a separate frame into the data stream.
	//
	// Note that Write does not do any coalescing nor splitting of data,
	// so each non-empty write will map to a separate Zstandard frame.
	// Empty writes do not add seek-table entries.
	//
	// If the underlying frame write fails or writes only part of the frame, the
	// writer stops accepting more frames. Close may still be called to write the
	// seek table for frames that were fully written. Bytes already accepted by
	// the underlying writer are not rolled back.
	Write(src []byte) (int, error)

	// Close implements io.Closer. It writes the seek table, releases the in-memory
	// frame index, and causes future Writer method calls to fail.
	//
	// The caller is still responsible for closing the underlying writer.
	Close() (err error)
}

// FrameSource returns one frame of data at a time.
//
// When there are no more frames, it returns nil, nil. A non-nil error stops the
// write. Empty frames are ignored by ConcurrentWriter.WriteMany.
type FrameSource func() ([]byte, error)

// ConcurrentWriter allows writing many frames concurrently.
type ConcurrentWriter interface {
	Writer

	// WriteMany writes many frames concurrently.
	//
	// It reads frames from frameSource sequentially, compresses up to the
	// configured concurrency in parallel, and writes compressed frames in the
	// same order returned by frameSource. Close must still be called after a
	// successful WriteMany call to write the final seek table. Frame write
	// failures have the same no-more-frames behavior as Writer.Write.
	WriteMany(ctx context.Context, frameSource FrameSource, options ...WriteManyOption) error
}

// ZSTDEncoder is the compressor.
//
// It is compatible with the EncodeAll method provided by
// github.com/klauspost/compress/zstd.
type ZSTDEncoder interface {
	EncodeAll(src, dst []byte) []byte
}

// NewWriter wraps w and encoder into an indexed Zstandard stream.
//
// w must be non-nil unless WithWriterEnvironment supplies a custom write
// environment. The caller remains responsible for closing w and encoder, if
// they require closing.
//
// The resulting stream can be randomly accessed through Reader or NewSeekTable.
func NewWriter(w io.Writer, encoder ZSTDEncoder, opts ...WriterOption) (ConcurrentWriter, error) {
	sw := writerImpl{
		enc: encoder,
	}

	sw.logger = discardLogger
	for _, o := range opts {
		err := o(&sw)
		if err != nil {
			return nil, err
		}
	}

	if sw.env == nil {
		sw.env = &writerEnvImpl{
			w: w,
		}
	}

	return &sw, nil
}

func (s *writerImpl) Write(src []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.env == nil || s.closed {
		return 0, ErrClosed
	}
	if s.failed {
		return 0, errWriterFailed
	}

	dst, entry, err := s.encodeOne(src)
	if err != nil {
		return 0, err
	}
	if len(src) == 0 {
		return 0, nil
	}

	n, err := s.env.WriteFrame(dst)
	if err != nil {
		s.failed = true
		return 0, err
	}
	if n != len(dst) {
		s.failed = true
		return 0, fmt.Errorf("partial write: %d out of %d", n, len(dst))
	}

	s.logger.Debug("appending frame", slog.Any("frame", &entry))
	s.frameEntries = append(s.frameEntries, entry)
	return len(src), nil
}

func (s *writerImpl) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.env == nil {
		return nil
	}

	err := s.writeSeekTableLocked()
	s.frameEntries = nil
	s.env = nil
	return err
}

type encodeResult struct {
	buf   []byte
	entry seekTableEntry
}

func (s *writerImpl) writeManyEncoder(ctx context.Context, ch chan<- encodeResult, frame []byte) func() error {
	return func() error {
		dst, entry, err := s.encodeOne(frame)
		if err != nil {
			return fmt.Errorf("failed to encode frame: %w", err)
		}

		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case ch <- encodeResult{dst, entry}:
			close(ch)
		}

		return nil
	}
}

func (s *writerImpl) writeManyProducer(ctx context.Context, frameSource FrameSource, g *errgroup.Group, queue chan<- chan encodeResult) func() error {
	return func() error {
		for {
			frame, err := frameSource()
			if err != nil {
				return fmt.Errorf("frame source failed: %w", err)
			}
			if frame == nil {
				close(queue)
				return nil
			}
			if len(frame) == 0 {
				// Skip empty frames entirely.
				continue
			}

			// Put a channel on the queue as a sort of promise.
			// This is a nice trick to keep our results ordered, even when compression
			// completes out-of-order.
			ch := make(chan encodeResult, 1)
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case queue <- ch:
			}

			g.Go(s.writeManyEncoder(ctx, ch, frame))
		}
	}
}

func (s *writerImpl) writeManyConsumer(ctx context.Context, callback func(uint32), queue <-chan chan encodeResult) func() error {
	return func() error {
		for {
			var ch <-chan encodeResult
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case ch = <-queue:
			}
			if ch == nil {
				return nil
			}

			// Wait for the block to be complete
			var result encodeResult
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case result = <-ch:
			}

			n, err := s.env.WriteFrame(result.buf)
			if err != nil {
				s.failed = true
				return fmt.Errorf("failed to write compressed data: %w", err)
			}
			if n != len(result.buf) {
				s.failed = true
				return fmt.Errorf("partial write: %d out of %d", n, len(result.buf))
			}
			s.frameEntries = append(s.frameEntries, result.entry)

			if callback != nil {
				callback(result.entry.DecompressedSize)
			}
		}
	}
}

func (s *writerImpl) WriteMany(ctx context.Context, frameSource FrameSource, options ...WriteManyOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.env == nil || s.closed {
		return ErrClosed
	}
	if s.failed {
		return errWriterFailed
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}

	opts := writeManyOptions{concurrency: runtime.GOMAXPROCS(0)}
	for _, o := range options {
		if err := o(&opts); err != nil {
			return err // no wrap, these should be user-comprehensible
		}
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(opts.concurrency + 2) // reader and writer
	// Add extra room in the queue, so we can keep throughput high even if blocks finish out of order
	queue := make(chan chan encodeResult, opts.concurrency*2)
	g.Go(s.writeManyProducer(gCtx, frameSource, g, queue))
	g.Go(s.writeManyConsumer(gCtx, opts.writeCallback, queue))
	return g.Wait()
}

func (s *writerImpl) writeSeekTableLocked() error {
	seekTableBytes, err := s.endStreamLocked()
	if err != nil {
		return err
	}

	n, err := s.env.WriteSeekTable(seekTableBytes)
	if err != nil {
		return err
	}
	if n != len(seekTableBytes) {
		return fmt.Errorf("partial write: %d out of %d", n, len(seekTableBytes))
	}
	return nil
}
