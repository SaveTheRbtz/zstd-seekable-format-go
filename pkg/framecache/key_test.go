package framecache

import (
	"bytes"
	"testing"
)

func TestKeyBinaryEncoding(t *testing.T) {
	key := NewKey(42, -7)

	prefix := []byte{1, 2, 3}
	encoded := key.AppendBinary(prefix)
	if !bytes.Equal(encoded[:len(prefix)], prefix) {
		t.Fatalf("AppendBinary prefix = %v, want %v", encoded[:len(prefix)], prefix)
	}
	if got, want := len(encoded)-len(prefix), keyBinarySize; got != want {
		t.Fatalf("AppendBinary appended %d bytes, want %d", got, want)
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

	values := map[Key][]byte{parsed: []byte("decoded")}
	if got := values[key]; !bytes.Equal(got, []byte("decoded")) {
		t.Fatalf("map lookup = %q, want decoded", got)
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
