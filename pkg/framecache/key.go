package framecache

import (
	"encoding"
	"encoding/binary"
	"fmt"
)

const keyBinarySize = 16

var (
	_ encoding.BinaryAppender    = Key{}
	_ encoding.BinaryMarshaler   = Key{}
	_ encoding.BinaryUnmarshaler = (*Key)(nil)
)

// Key identifies one decoded frame in one cache namespace.
//
// seekable.Reader assigns process-local namespaces to Reader instances. Those
// namespaces prevent collisions in shared caches, but they are not persistent
// stream IDs and must not be used as durable cache identity.
type Key struct {
	namespace uint64
	frameID   int64
}

// NewKey returns a cache key for direct cache use.
//
// Most callers do not need this; seekable.Reader creates keys for configured
// caches. Use distinct namespace values when one cache may hold frames from
// different streams or Readers. namespace must not be treated as a stable stream
// identity.
func NewKey(namespace uint64, frameID int64) Key {
	return Key{namespace: namespace, frameID: frameID}
}

// ParseKey returns the key encoded by Key.MarshalBinary or Key.AppendBinary.
//
// data must contain exactly one key encoding. If AppendBinary was called with a
// non-empty dst, pass only the appended key bytes to ParseKey.
func ParseKey(data []byte) (Key, error) {
	var key Key
	if err := key.UnmarshalBinary(data); err != nil {
		return Key{}, err
	}
	return key, nil
}

// FrameID returns the seek-table frame ID embedded in k.
func (k Key) FrameID() int64 {
	return k.frameID
}

// AppendBinary appends k's opaque binary encoding to dst and returns the result.
//
// The encoding is useful for ephemeral external caches that need byte keys. It
// must not be used as persistent cache identity across Reader or process
// lifetimes.
func (k Key) AppendBinary(dst []byte) ([]byte, error) {
	var data [keyBinarySize]byte
	binary.BigEndian.PutUint64(data[:8], k.namespace)
	binary.BigEndian.PutUint64(data[8:], uint64(k.frameID))
	return append(dst, data[:]...), nil
}

// MarshalBinary returns k's opaque binary encoding.
//
// The encoding is useful for ephemeral external caches that need byte keys. It
// must not be used as persistent cache identity across Reader or process
// lifetimes.
func (k Key) MarshalBinary() ([]byte, error) {
	return k.AppendBinary(nil)
}

// UnmarshalBinary decodes k from the encoding produced by MarshalBinary.
func (k *Key) UnmarshalBinary(data []byte) error {
	if len(data) != keyBinarySize {
		return fmt.Errorf("framecache key length is %d, want %d", len(data), keyBinarySize)
	}
	k.namespace = binary.BigEndian.Uint64(data[:8])
	k.frameID = int64(binary.BigEndian.Uint64(data[8:keyBinarySize]))
	return nil
}
