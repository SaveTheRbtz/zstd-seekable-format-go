package framecache

import (
	"bytes"
	"testing"
)

func TestKeyBinaryEncoding(t *testing.T) {
	key := NewKey(42, -7)

	prefix := []byte{1, 2, 3}
	encoded, err := key.AppendBinary(prefix)
	if err != nil {
		t.Fatalf("AppendBinary: %v", err)
	}
	wantEncoded := []byte{
		1, 2, 3,
		0, 0, 0, 0, 0, 0, 0, 42,
		255, 255, 255, 255, 255, 255, 255, 249,
	}
	if !bytes.Equal(encoded, wantEncoded) {
		t.Fatalf("AppendBinary = %v, want %v", encoded, wantEncoded)
	}

	marshaled, err := key.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	if !bytes.Equal(encoded[len(prefix):], marshaled) {
		t.Fatalf("AppendBinary payload = %v, MarshalBinary = %v", encoded[len(prefix):], marshaled)
	}

	parsed, err := ParseKey(marshaled)
	if err != nil {
		t.Fatalf("ParseKey: %v", err)
	}
	if parsed != key {
		t.Fatalf("ParseKey = %+v, want %+v", parsed, key)
	}
	if parsed.FrameID() != -7 {
		t.Fatalf("FrameID = %d, want -7", parsed.FrameID())
	}

	var unmarshaled Key
	if err := unmarshaled.UnmarshalBinary(marshaled); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if unmarshaled != key {
		t.Fatalf("UnmarshalBinary = %+v, want %+v", unmarshaled, key)
	}
}

func TestParseKeyRejectsInvalidLength(t *testing.T) {
	for _, data := range [][]byte{
		nil,
		make([]byte, keyBinarySize-1),
		make([]byte, keyBinarySize+1),
	} {
		if _, err := ParseKey(data); err == nil {
			t.Fatalf("ParseKey(%d bytes) succeeded, want error", len(data))
		}
	}
}
