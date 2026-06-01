package seekable

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

type cachedFrame struct {
	m sync.Mutex

	offset uint64
	data   []byte
}

func (f *cachedFrame) replace(offset uint64, data []byte) {
	f.m.Lock()
	defer f.m.Unlock()

	f.offset = offset
	f.data = data
}

func (f *cachedFrame) get() (uint64, []byte) {
	f.m.Lock()
	defer f.m.Unlock()

	return f.offset, f.data
}

// readSeekerEnvImpl is the environment implementation for the io.ReadSeeker.
type readSeekerEnvImpl struct {
	rs io.ReadSeeker
	mu sync.Mutex
}

func (rs *readSeekerEnvImpl) GetFrameByIndex(index FrameOffsetEntry) ([]byte, error) {
	p := make([]byte, index.CompSize)
	off := int64(index.CompOffset)

	switch v := rs.rs.(type) {
	case io.ReaderAt:
		n, err := v.ReadAt(p, off)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if n != len(p) {
			return nil, io.ErrUnexpectedEOF
		}
	default:
		rs.mu.Lock()
		defer rs.mu.Unlock()

		_, err := v.Seek(off, io.SeekStart)
		if err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(rs.rs, p); err != nil {
			return nil, err
		}
	}

	return p, nil
}

func (rs *readSeekerEnvImpl) ReadFooter() ([]byte, error) {
	n, err := rs.rs.Seek(-seekTableFooterOffset, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to: %d: %w", -seekTableFooterOffset, err)
	}

	buf := make([]byte, seekTableFooterOffset)
	_, err = io.ReadFull(rs.rs, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read footer at: %d: %w", n, err)
	}

	return buf, nil
}

func (rs *readSeekerEnvImpl) ReadSkipFrame(skippableFrameOffset int64) ([]byte, error) {
	n, err := rs.rs.Seek(-skippableFrameOffset, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to: %d: %w", -skippableFrameOffset, err)
	}

	buf := make([]byte, skippableFrameOffset)
	_, err = io.ReadFull(rs.rs, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read skippable frame header at: %d: %w", n, err)
	}
	return buf, nil
}

type readerImpl struct {
	dec ZSTDDecoder
	seekTable

	offset int64

	logger *slog.Logger
	env    REnvironment

	closed atomic.Bool

	// TODO: Add simple LRU cache.
	cachedFrame cachedFrame
}

var (
	_ Reader      = (*readerImpl)(nil)
	_ io.Seeker   = (*readerImpl)(nil)
	_ io.Reader   = (*readerImpl)(nil)
	_ io.ReaderAt = (*readerImpl)(nil)
	_ io.Closer   = (*readerImpl)(nil)
)

type Reader interface {
	// EntryByDecompressedOffset returns the frame containing off in the decompressed stream.
	// It returns false if off is greater than or equal to Size().
	EntryByDecompressedOffset(off uint64) (FrameOffsetEntry, bool)

	// EntryByID returns the frame with id.
	// It returns false if id is greater than or equal to NumFrames() or less than 0.
	EntryByID(id int64) (FrameOffsetEntry, bool)

	// Size returns the size of the uncompressed stream.
	Size() int64

	// NumFrames returns number of frames in the compressed stream.
	NumFrames() int64

	// Seek implements io.Seeker interface to randomly access data.
	// This method is NOT goroutine-safe and CAN NOT be called
	// concurrently since it modifies the underlying offset.
	Seek(offset int64, whence int) (int64, error)

	// Read implements io.Reader interface to sequentially access data.
	// This method is NOT goroutine-safe and CAN NOT be called
	// concurrently since it modifies the underlying offset.
	Read(p []byte) (n int, err error)

	// ReadAt implements io.ReaderAt interface to randomly access data.
	// This method is goroutine-safe and can be called concurrently ONLY if
	// the underlying reader supports io.ReaderAt interface.
	ReadAt(p []byte, off int64) (n int, err error)

	// Close implements io.Closer interface freeing up any resources.
	Close() error
}

// ZSTDDecoder is the decompressor.  Tested with github.com/klauspost/compress/zstd.
type ZSTDDecoder interface {
	DecodeAll(input, dst []byte) ([]byte, error)
}

// NewReader returns ZSTD stream reader that can be randomly accessed using uncompressed data offset.
// Ideally, passed io.ReadSeeker should implement io.ReaderAt interface.
func NewReader(rs io.ReadSeeker, decoder ZSTDDecoder, opts ...rOption) (Reader, error) {
	sr := readerImpl{
		dec: decoder,
	}

	sr.logger = discardLogger
	for _, o := range opts {
		err := o(&sr)
		if err != nil {
			return nil, err
		}
	}

	if sr.env == nil {
		if rs == nil {
			return nil, fmt.Errorf("nil ReadSeeker and no custom environment supplied")
		}
		sr.env = &readSeekerEnvImpl{
			rs: rs,
		}
	}

	table, err := readSeekTable(sr.env)
	if err != nil {
		return nil, err
	}

	sr.seekTable = table

	return &sr, nil
}

func (r *readerImpl) ReadAt(p []byte, off int64) (n int, err error) {
	for m := 0; n < len(p) && err == nil; n += m {
		_, m, err = r.read(p[n:], off+int64(n))
	}
	return
}

func (r *readerImpl) Read(p []byte) (n int, err error) {
	offset, n, err := r.read(p, r.offset)
	if err != nil {
		if errors.Is(err, io.EOF) {
			r.offset = r.size
		}
		return
	}
	r.offset = offset
	return
}

func (r *readerImpl) Close() error {
	if r.closed.CompareAndSwap(false, true) {
		r.cachedFrame.replace(math.MaxUint64, nil)
		r.seekTable = seekTable{}
	}
	return nil
}

func (r *readerImpl) read(dst []byte, off int64) (int64, int, error) {
	if r.closed.Load() {
		return 0, 0, fmt.Errorf("reader is closed")
	}

	if off >= r.size {
		return 0, 0, io.EOF
	}
	if off < 0 {
		return 0, 0, fmt.Errorf("offset before the start of the file: %d", off)
	}

	index, ok := r.EntryByDecompressedOffset(uint64(off))
	if !ok {
		return 0, 0, fmt.Errorf("failed to get index by offset: %d", off)
	}
	if off < int64(index.DecompOffset) || off > int64(index.DecompOffset)+int64(index.DecompSize) {
		return 0, 0, fmt.Errorf("offset outside of index bounds: %d: min: %d, max: %d",
			off, int64(index.DecompOffset), int64(index.DecompOffset)+int64(index.DecompSize))
	}

	var decompressed []byte

	cachedOffset, cachedData := r.cachedFrame.get()
	if cachedOffset == index.DecompOffset && cachedData != nil {
		// fastpath
		decompressed = cachedData
	} else {
		// slowpath
		if index.CompSize > maxDecoderFrameSize {
			return 0, 0, fmt.Errorf("index.CompSize is too big: %d > %d",
				index.CompSize, maxDecoderFrameSize)
		}

		src, err := r.env.GetFrameByIndex(index)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to read compressed data at: %d, %w", index.CompOffset, err)
		}

		if len(src) != int(index.CompSize) {
			return 0, 0, fmt.Errorf("compressed size does not match index at: %d: expected: %d, index: %+v",
				off, len(src), index)
		}

		decompressed, err = r.dec.DecodeAll(src, nil)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to decompress data data at: %d, %w", index.CompOffset, err)
		}

		if r.checksums {
			checksum := uint32((xxhash.Sum64(decompressed) << 32) >> 32)
			if index.Checksum != checksum {
				return 0, 0, fmt.Errorf("checksum verification failed at: %d: expected: %d, actual: %d",
					index.CompOffset, index.Checksum, checksum)
			}
		}
		r.cachedFrame.replace(index.DecompOffset, decompressed)
	}

	if len(decompressed) != int(index.DecompSize) {
		return 0, 0, fmt.Errorf("index corruption: len: %d, expected: %d", len(decompressed), int(index.DecompSize))
	}

	offsetWithinFrame := uint64(off) - index.DecompOffset

	size := uint64(len(decompressed)) - offsetWithinFrame
	if size > uint64(len(dst)) {
		size = uint64(len(dst))
	}

	r.logger.Debug("decompressed", slog.Uint64("offsetWithinFrame", offsetWithinFrame), slog.Uint64("end", offsetWithinFrame+size),
		slog.Uint64("size", size), slog.Int("lenDecompressed", len(decompressed)), slog.Int("lenDst", len(dst)), slog.Any("index", index))
	copy(dst, decompressed[offsetWithinFrame:offsetWithinFrame+size])

	return off + int64(size), int(size), nil
}

func (r *readerImpl) Seek(offset int64, whence int) (int64, error) {
	newOffset := r.offset
	switch whence {
	case io.SeekCurrent:
		newOffset += offset
	case io.SeekStart:
		newOffset = offset
	case io.SeekEnd:
		newOffset = r.size + offset
	default:
		return 0, fmt.Errorf("unknown whence: %d", whence)
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("offset before the start of the file: %d (%d + %d)",
			newOffset, r.offset, offset)
	}

	r.offset = newOffset
	return r.offset, nil
}
