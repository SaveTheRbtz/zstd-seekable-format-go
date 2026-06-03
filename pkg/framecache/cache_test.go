package framecache

import (
	"bytes"
	"container/list"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

type cacheFactory struct {
	name string
	new  func(Limits) testCache
}

type testCache interface {
	Cache
	Clearer
}

var cacheFactories = []cacheFactory{
	{name: "FIFO", new: func(l Limits) testCache { return NewFIFO(l) }},
	{name: "LRU", new: func(l Limits) testCache { return NewLRU(l) }},
	{name: "Sieve", new: func(l Limits) testCache { return NewSieve(l) }},
}

func testKey(frameID int64) Key {
	return NewKey(1, frameID)
}

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

func TestPolicyEviction(t *testing.T) {
	tests := []struct {
		name      string
		cache     Cache
		wantHit   []int64
		wantMiss  []int64
		primeFunc func(Cache)
	}{
		{
			name:  "FIFO",
			cache: NewFIFO(Limits{MaxFrames: 2}),
			primeFunc: func(c Cache) {
				c.Put(testKey(1), []byte("one"))
				c.Put(testKey(2), []byte("two"))
				_, _ = c.Get(testKey(1))
				c.Put(testKey(3), []byte("three"))
			},
			wantHit:  []int64{2, 3},
			wantMiss: []int64{1},
		},
		{
			name:  "LRU",
			cache: NewLRU(Limits{MaxFrames: 2}),
			primeFunc: func(c Cache) {
				c.Put(testKey(1), []byte("one"))
				c.Put(testKey(2), []byte("two"))
				_, _ = c.Get(testKey(1))
				c.Put(testKey(3), []byte("three"))
			},
			wantHit:  []int64{1, 3},
			wantMiss: []int64{2},
		},
		{
			name:  "Sieve",
			cache: NewSieve(Limits{MaxFrames: 2}),
			primeFunc: func(c Cache) {
				c.Put(testKey(1), []byte("one"))
				c.Put(testKey(2), []byte("two"))
				_, _ = c.Get(testKey(1))
				c.Put(testKey(3), []byte("three"))
			},
			wantHit:  []int64{1, 3},
			wantMiss: []int64{2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.primeFunc(tc.cache)
			for _, frameID := range tc.wantHit {
				if _, ok := tc.cache.Get(testKey(frameID)); !ok {
					t.Fatalf("Get(%d) missed", frameID)
				}
			}
			for _, frameID := range tc.wantMiss {
				if data, ok := tc.cache.Get(testKey(frameID)); ok {
					t.Fatalf("Get(%d) = %q, true; want miss", frameID, data)
				}
			}
		})
	}
}

func TestCacheLimitsReplacementAndClear(t *testing.T) {
	for _, factory := range cacheFactories {
		t.Run(factory.name, func(t *testing.T) {
			c := factory.new(Limits{MaxFrames: 2})
			c.Put(testKey(1), []byte("a"))
			c.Put(testKey(1), []byte("bb"))
			got, ok := c.Get(testKey(1))
			if !ok || !bytes.Equal(got, []byte("bb")) {
				t.Fatalf("replacement Get(1) = %q, %t; want %q, true", got, ok, "bb")
			}
			assertCacheInvariants(t, c, Limits{MaxFrames: 2})

			c = factory.new(Limits{MaxFrames: 2, MaxBytes: 2})
			c.Put(testKey(1), []byte("a"))
			c.Put(testKey(1), []byte("bbb"))
			if got, ok := c.Get(testKey(1)); ok {
				t.Fatalf("oversized replacement Get(1) = %q, true; want miss", got)
			}
			assertCacheInvariants(t, c, Limits{MaxFrames: 2, MaxBytes: 2})

			c = factory.new(Limits{MaxFrames: 3, MaxBytes: 5})
			c.Put(testKey(1), []byte("aa"))
			c.Put(testKey(2), []byte("bb"))
			c.Put(testKey(3), []byte("cc"))
			assertCacheInvariants(t, c, Limits{MaxFrames: 3, MaxBytes: 5})

			c = factory.new(Limits{MaxFrames: 0})
			c.Put(testKey(1), []byte("a"))
			if got, ok := c.Get(testKey(1)); ok {
				t.Fatalf("disabled cache Get(1) = %q, true; want miss", got)
			}
			assertCacheInvariants(t, c, Limits{MaxFrames: 0})

			c = factory.new(Limits{MaxFrames: 2})
			c.Put(testKey(1), []byte("a"))
			c.Put(testKey(2), []byte("b"))
			c.Clear()
			assertCacheInvariants(t, c, Limits{MaxFrames: 2})
			if got, ok := c.Get(testKey(1)); ok {
				t.Fatalf("Get(1) after Clear = %q, true; want miss", got)
			}
		})
	}
}

func TestCacheAccessDistributions(t *testing.T) {
	const (
		frameCount  = 32
		accessCount = 512
	)

	distributions := []struct {
		name string
		seq  []int
	}{
		{name: "Random", seq: randomAccesses(frameCount, accessCount, 1)},
		{name: "Zipf", seq: zipfAccesses(frameCount, accessCount, 2)},
		{name: "Normal", seq: normalAccesses(frameCount, accessCount, 3)},
	}

	frames := testFrames(frameCount)
	for _, factory := range cacheFactories {
		for _, dist := range distributions {
			t.Run(factory.name+"/"+dist.name, func(t *testing.T) {
				limits := Limits{MaxFrames: 8, MaxBytes: 64}
				c := factory.new(limits)
				for _, frameID := range dist.seq {
					want := frames[frameID]
					got, ok := c.Get(testKey(int64(frameID)))
					if ok && !bytes.Equal(got, want) {
						t.Fatalf("Get(%d) = %q; want %q", frameID, got, want)
					}
					c.Put(testKey(int64(frameID)), want)
					assertCacheInvariants(t, c, limits)
				}
			})
		}
	}
}

func testFrames(n int) [][]byte {
	frames := make([][]byte, n)
	for i := range frames {
		frames[i] = bytes.Repeat([]byte{byte(i)}, i%7+1)
	}
	return frames
}

func randomAccesses(frameCount, accessCount int, seed int64) []int {
	rng := rand.New(rand.NewSource(seed))
	seq := make([]int, accessCount)
	for i := range seq {
		seq[i] = rng.Intn(frameCount)
	}
	return seq
}

func zipfAccesses(frameCount, accessCount int, seed int64) []int {
	rng := rand.New(rand.NewSource(seed))
	zipf := rand.NewZipf(rng, 1.2, 1, uint64(frameCount-1))
	seq := make([]int, accessCount)
	for i := range seq {
		seq[i] = int(zipf.Uint64())
	}
	return seq
}

func normalAccesses(frameCount, accessCount int, seed int64) []int {
	rng := rand.New(rand.NewSource(seed))
	mean := float64(frameCount-1) / 2
	stddev := math.Max(1, float64(frameCount)/6)
	seq := make([]int, accessCount)
	for i := range seq {
		v := int(math.Round(rng.NormFloat64()*stddev + mean))
		if v < 0 {
			v = 0
		}
		if v >= frameCount {
			v = frameCount - 1
		}
		seq[i] = v
	}
	return seq
}

func assertCacheInvariants(t *testing.T, c Cache, limits Limits) {
	t.Helper()

	var (
		order *list.List
		items map[Key]*list.Element
		bytes uint64
		hand  *list.Element
	)

	switch v := c.(type) {
	case *FIFO:
		v.mu.Lock()
		defer v.mu.Unlock()
		order = &v.order
		items = v.items
		bytes = v.bytes
	case *LRU:
		v.mu.Lock()
		defer v.mu.Unlock()
		order = &v.order
		items = v.items
		bytes = v.bytes
	case *Sieve:
		v.mu.Lock()
		defer v.mu.Unlock()
		order = &v.order
		items = v.items
		bytes = v.bytes
		hand = v.hand
	default:
		t.Fatalf("unsupported cache type %T", c)
	}

	if len(items) != order.Len() {
		t.Fatalf("map length = %d, list length = %d", len(items), order.Len())
	}
	if limits.MaxFrames <= 0 && order.Len() != 0 {
		t.Fatalf("disabled cache holds %d frames", order.Len())
	}
	if limits.MaxFrames > 0 && order.Len() > limits.MaxFrames {
		t.Fatalf("cache holds %d frames, limit is %d", order.Len(), limits.MaxFrames)
	}
	if limits.MaxBytes > 0 && bytes > limits.MaxBytes {
		t.Fatalf("cache holds %d bytes, limit is %d", bytes, limits.MaxBytes)
	}
	if order.Len() == 0 && hand != nil {
		t.Fatalf("empty Sieve cache has non-nil hand")
	}

	var sum uint64
	seen := make(map[Key]bool, order.Len())
	for elem := order.Front(); elem != nil; elem = elem.Next() {
		entry, ok := elem.Value.(*cacheEntry)
		if !ok {
			t.Fatalf("list entry has type %T", elem.Value)
		}
		if seen[entry.key] {
			t.Fatalf("duplicate key %+v", entry.key)
		}
		seen[entry.key] = true
		if items[entry.key] != elem {
			t.Fatalf("map element mismatch for key %+v", entry.key)
		}
		if uint64(len(entry.data)) != entry.size {
			t.Fatalf("key %+v size = %d, len(data) = %d", entry.key, entry.size, len(entry.data))
		}
		sum += entry.size
	}
	if sum != bytes {
		t.Fatalf("byte accounting = %d, want %d", bytes, sum)
	}
}

func cacheByStrategy(strategy uint8, limits Limits) (string, Cache) {
	switch strategy % 3 {
	case 0:
		return "FIFO", NewFIFO(limits)
	case 1:
		return "LRU", NewLRU(limits)
	default:
		return "Sieve", NewSieve(limits)
	}
}

func TestCacheByStrategyCoversAllCaches(t *testing.T) {
	for strategy, name := range []string{"FIFO", "LRU", "Sieve"} {
		got, _ := cacheByStrategy(uint8(strategy), Limits{MaxFrames: 1})
		if got != name {
			t.Fatalf("strategy %d = %s, want %s", strategy, got, name)
		}
	}
}

func ExampleNewFIFO_noCache() {
	c := NewFIFO(Limits{MaxFrames: 0})
	c.Put(NewKey(1, 1), []byte("decoded frame"))
	_, ok := c.Get(NewKey(1, 1))
	fmt.Println(ok)

	// Output:
	// false
}
