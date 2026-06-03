// Copyright (c) 2022, Alexey Ivanov
// All rights reserved.

// Package seekable writes and reads streams using the Zstandard seekable format.
//
// A seekable stream is a valid Zstandard stream made from one or more
// compressed frames followed by a final skippable frame containing a seek table.
// Standard Zstandard decoders can read the stream from the beginning, while
// Reader uses the seek table to serve Read, ReadAt, and Seek calls by
// uncompressed byte offset and exposes the parsed metadata through
// Reader.SeekTable.
//
// Writer and Encoder produce seekable streams by storing each non-empty input
// chunk as a separate Zstandard frame. Close or EndStream must be called to
// append or retrieve the final seek-table skippable frame.
// Close is idempotent; EndStream finalizes the Encoder and must be called once.
// Operations after Close or EndStream return ErrClosed.
//
// The package accepts small encoder and decoder interfaces and is tested with
// github.com/klauspost/compress/zstd.
//
// Reader caches one decoded frame by default. Use WithReaderFrameCache to
// select or disable the decoded-frame cache.
package seekable
