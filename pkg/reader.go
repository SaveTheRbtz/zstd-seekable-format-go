package seekable

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/framecache"
	"github.com/cespare/xxhash/v2"
)

const maxReaderOffset = uint64(1<<63 - 1)

// readSeekerEnvImpl is the environment implementation for the io.ReadSeeker.
type readSeekerEnvImpl struct {
	rs io.ReadSeeker
	mu sync.Mutex
}

func (rs *readSeekerEnvImpl) GetFrameByIndex(index FrameOffsetEntry) ([]byte, error) {
	p := make([]byte, index.CompressedSize)
	off := int64(index.CompressedOffset)

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

// Reader provides sequential and random access to a seekable Zstandard stream
// and exposes its parsed seek-table metadata.
//
// Offsets are expressed in the decompressed stream. Read and Seek use an
// internal current offset; ReadAt does not.
//
// Before Close, SeekTable may be called concurrently with other Reader methods.
// ReadAt may also be called concurrently if the supplied decoder, read
// environment support concurrent use. Read and Seek share the internal current
// offset and should be serialized by the caller. No Reader method should be
// called concurrently with Close.
type Reader struct {
	dec   ZSTDDecoder
	table SeekTable

	offset int64

	logger *slog.Logger
	env    ReaderEnvironment

	closed atomic.Bool

	frameCacheMu sync.Mutex
	frameCache   framecache.Cache
}

var (
	_ io.Seeker   = (*Reader)(nil)
	_ io.Reader   = (*Reader)(nil)
	_ io.ReaderAt = (*Reader)(nil)
	_ io.Closer   = (*Reader)(nil)
)

// ZSTDDecoder is the decompressor.
//
// It is compatible with the DecodeAll method provided by
// github.com/klauspost/compress/zstd.
//
// Reader may call DecodeAll concurrently from concurrent ReadAt calls. Decoders
// used that way must support concurrent DecodeAll calls.
type ZSTDDecoder interface {
	DecodeAll(input, dst []byte) ([]byte, error)
}

// NewReader returns a Zstandard stream reader that can be randomly accessed by
// uncompressed offset.
//
// The stream must contain a final seek-table skippable frame. rs must be
// non-nil unless WithReaderEnvironment supplies a custom read environment. When
// NewReader uses rs directly and rs also implements io.ReaderAt, frame reads do
// not move rs's current offset.
//
// decoder must be non-nil. NewReader reads and validates the seek table during
// construction. By default, Reader caches one decoded frame; use
// WithReaderFrameCache to change or disable caching.
//
// The caller remains responsible for closing rs and decoder, if they require
// closing.
func NewReader(rs io.ReadSeeker, decoder ZSTDDecoder, opts ...ReaderOption) (*Reader, error) {
	sr := Reader{
		dec: decoder,
	}

	sr.logger = discardLogger
	for _, o := range opts {
		err := o(&sr)
		if err != nil {
			return nil, err
		}
	}
	if sr.frameCache == nil {
		sr.frameCache = defaultFrameCache()
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
	if table.Size() > maxReaderOffset {
		return nil, fmt.Errorf("decompressed size is too large for Reader: %d > %d", table.Size(), maxReaderOffset)
	}

	sr.table = table
	sr.frameCache.Clear()

	return &sr, nil
}

// SeekTable returns the parsed seek table for this Reader.
//
// The returned SeekTable is immutable. SeekTable returns ErrClosed after Close.
// Before Close, SeekTable may be called concurrently with other Reader methods
// except Close.
func (r *Reader) SeekTable() (SeekTable, error) {
	if r.closed.Load() {
		return SeekTable{}, ErrClosed
	}
	return r.table, nil
}

// ReadAt reads len(p) decompressed bytes starting at off.
//
// ReadAt does not move the Reader's current offset.
//
// If off is at or beyond the decompressed stream size, ReadAt returns io.EOF. If
// p extends beyond the end of the stream, ReadAt returns the bytes available and
// io.EOF.
//
// Before Close, ReadAt may be called concurrently if the supplied decoder and
// read environment support concurrent use.
func (r *Reader) ReadAt(p []byte, off int64) (n int, err error) {
	if r.closed.Load() {
		return 0, ErrClosed
	}

	for m := 0; n < len(p) && err == nil; n += m {
		_, m, err = r.read(p[n:], off+int64(n))
	}
	return
}

// Read reads decompressed bytes from the Reader's current offset and advances
// the current offset by the bytes read.
func (r *Reader) Read(p []byte) (n int, err error) {
	offset, n, err := r.read(p, r.offset)
	if err != nil {
		return
	}
	r.offset = offset
	return
}

// Close releases resources held by the Reader.
//
// Close is idempotent. Read, ReadAt, Seek, and SeekTable return ErrClosed after
// Close. Close does not close the io.ReadSeeker, decoder, or custom read
// environment passed to NewReader. Close clears the Reader's frame cache.
func (r *Reader) Close() error {
	if !r.closed.Swap(true) {
		r.clearFrameCache()
		r.frameCache = nil
		r.table = SeekTable{}
	}
	return nil
}

func (r *Reader) read(dst []byte, off int64) (int64, int, error) {
	if r.closed.Load() {
		return 0, 0, ErrClosed
	}
	if len(dst) == 0 {
		return off, 0, nil
	}

	if off < 0 {
		return 0, 0, fmt.Errorf("offset before the start of the file: %d", off)
	}
	if uint64(off) >= r.table.Size() {
		return 0, 0, io.EOF
	}

	index, ok := r.table.EntryByDecompressedOffset(uint64(off))
	if !ok {
		return 0, 0, fmt.Errorf("failed to get index by offset: %d", off)
	}
	if off < int64(index.DecompressedOffset) || off > int64(index.DecompressedOffset)+int64(index.DecompressedSize) {
		return 0, 0, fmt.Errorf("offset outside of index bounds: %d: min: %d, max: %d",
			off, int64(index.DecompressedOffset), int64(index.DecompressedOffset)+int64(index.DecompressedSize))
	}

	var decompressed []byte

	cachedData, ok := r.getCachedFrame(index.ID)
	if ok {
		decompressed = cachedData
	} else {
		if index.CompressedSize > maxDecoderFrameSize {
			return 0, 0, fmt.Errorf("index.CompressedSize is too big: %d > %d",
				index.CompressedSize, maxDecoderFrameSize)
		}

		src, err := r.env.GetFrameByIndex(index)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to read compressed data at: %d, %w", index.CompressedOffset, err)
		}

		if len(src) != int(index.CompressedSize) {
			return 0, 0, fmt.Errorf("compressed size does not match index at: %d: expected: %d, index: %+v",
				off, len(src), index)
		}

		decompressed, err = r.dec.DecodeAll(src, nil)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to decompress data data at: %d, %w", index.CompressedOffset, err)
		}

		if r.table.HasChecksums() {
			checksum := uint32(xxhash.Sum64(decompressed))
			if index.Checksum != checksum {
				return 0, 0, fmt.Errorf("checksum verification failed at: %d: expected: %d, actual: %d",
					index.CompressedOffset, index.Checksum, checksum)
			}
		}
		r.putCachedFrame(index.ID, decompressed)
	}

	if len(decompressed) != int(index.DecompressedSize) {
		return 0, 0, fmt.Errorf("index corruption: len: %d, expected: %d", len(decompressed), int(index.DecompressedSize))
	}

	offsetWithinFrame := uint64(off) - index.DecompressedOffset

	size := uint64(len(decompressed)) - offsetWithinFrame
	if size > uint64(len(dst)) {
		size = uint64(len(dst))
	}

	r.logger.Debug("decompressed", slog.Uint64("offsetWithinFrame", offsetWithinFrame), slog.Uint64("end", offsetWithinFrame+size),
		slog.Uint64("size", size), slog.Int("lenDecompressed", len(decompressed)), slog.Int("lenDst", len(dst)), slog.Any("index", index))
	copy(dst, decompressed[offsetWithinFrame:offsetWithinFrame+size])

	return off + int64(size), int(size), nil
}

// Seek updates the Reader's current offset and returns the new offset.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	if r.closed.Load() {
		return 0, ErrClosed
	}

	newOffset := r.offset
	switch whence {
	case io.SeekCurrent:
		newOffset += offset
	case io.SeekStart:
		newOffset = offset
	case io.SeekEnd:
		newOffset = int64(r.table.Size()) + offset
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
