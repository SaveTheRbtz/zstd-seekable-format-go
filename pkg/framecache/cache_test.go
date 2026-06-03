package framecache

import (
	"bytes"
	"container/list"
	"fmt"
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

func TestPolicyEviction(t *testing.T) {
	tests := []struct {
		name      string
		cache     Cache
		primeFunc func(Cache)
		want      []cacheResult
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
			want: []cacheResult{
				{frameID: 1},
				{frameID: 2, data: "two", ok: true},
				{frameID: 3, data: "three", ok: true},
			},
		},
		{
			name:  "FIFOReplacementByteLimit",
			cache: NewFIFO(Limits{MaxFrames: 2, MaxBytes: 5}),
			primeFunc: func(c Cache) {
				c.Put(testKey(1), []byte("aa"))
				c.Put(testKey(2), []byte("bbb"))
				c.Put(testKey(1), []byte("ccc"))
			},
			want: []cacheResult{
				{frameID: 1, data: "ccc", ok: true},
				{frameID: 2},
			},
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
			want: []cacheResult{
				{frameID: 1, data: "one", ok: true},
				{frameID: 2},
				{frameID: 3, data: "three", ok: true},
			},
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
			want: []cacheResult{
				{frameID: 1, data: "one", ok: true},
				{frameID: 2},
				{frameID: 3, data: "three", ok: true},
			},
		},
		{
			name:  "SieveAdmissionKeepsNewEntry",
			cache: NewSieve(Limits{MaxFrames: 1}),
			primeFunc: func(c Cache) {
				c.Put(testKey(1), []byte("one"))
				_, _ = c.Get(testKey(1))
				c.Put(testKey(2), []byte("two"))
			},
			want: []cacheResult{
				{frameID: 1},
				{frameID: 2, data: "two", ok: true},
			},
		},
		{
			name:  "SieveReplacementByteLimit",
			cache: NewSieve(Limits{MaxFrames: 2, MaxBytes: 2}),
			primeFunc: func(c Cache) {
				c.Put(testKey(1), []byte("a"))
				c.Put(testKey(2), []byte("b"))
				_, _ = c.Get(testKey(1))
				_, _ = c.Get(testKey(2))
				c.Put(testKey(1), []byte("aa"))
			},
			want: []cacheResult{
				{frameID: 1, data: "aa", ok: true},
				{frameID: 2},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.primeFunc(tc.cache)
			assertCacheResults(t, tc.cache, tc.want)
		})
	}
}

type cacheResult struct {
	frameID int64
	data    string
	ok      bool
}

func assertCacheResults(t *testing.T, c Cache, results []cacheResult) {
	t.Helper()

	for _, want := range results {
		data, ok := c.Get(testKey(want.frameID))
		if ok != want.ok {
			t.Fatalf("Get(%d) ok = %t, want %t", want.frameID, ok, want.ok)
		}
		if ok && string(data) != want.data {
			t.Fatalf("Get(%d) = %q, want %q", want.frameID, data, want.data)
		}
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

			c = factory.new(Limits{MaxFrames: 2, MaxBytes: 2})
			c.Put(testKey(1), []byte("a"))
			c.Put(testKey(1), []byte("bbb"))
			if got, ok := c.Get(testKey(1)); ok {
				t.Fatalf("oversized replacement Get(1) = %q, true; want miss", got)
			}

			c = factory.new(Limits{MaxFrames: 3, MaxBytes: 5})
			c.Put(testKey(1), []byte("aa"))
			c.Put(testKey(2), []byte("bb"))
			c.Put(testKey(3), []byte("cc"))
			assertCacheResults(t, c, []cacheResult{
				{frameID: 1},
				{frameID: 2, data: "bb", ok: true},
				{frameID: 3, data: "cc", ok: true},
			})

			c = factory.new(Limits{MaxFrames: 0})
			c.Put(testKey(1), []byte("a"))
			if got, ok := c.Get(testKey(1)); ok {
				t.Fatalf("disabled cache Get(1) = %q, true; want miss", got)
			}

			c = factory.new(Limits{MaxFrames: 2})
			c.Put(testKey(1), []byte("a"))
			c.Put(testKey(2), []byte("b"))
			c.Clear()
			if got, ok := c.Get(testKey(1)); ok {
				t.Fatalf("Get(1) after Clear = %q, true; want miss", got)
			}
		})
	}
}

func testFrames(n int) [][]byte {
	frames := make([][]byte, n)
	for i := range frames {
		frames[i] = bytes.Repeat([]byte{byte(i)}, i%7+1)
	}
	return frames
}

func assertCacheInvariants(t *testing.T, c Cache, context string) {
	t.Helper()
	fail := func(format string, args ...any) {
		t.Helper()
		if context != "" {
			format += " (" + context + ")"
		}
		t.Fatalf(format, args...)
	}

	var (
		order  *list.List
		items  map[Key]*list.Element
		bytes  uint64
		hand   *list.Element
		limits Limits
	)

	switch v := c.(type) {
	case *FIFO:
		v.mu.Lock()
		defer v.mu.Unlock()
		order = &v.order
		items = v.items
		bytes = v.bytes
		limits = v.limits
	case *LRU:
		v.mu.Lock()
		defer v.mu.Unlock()
		order = &v.order
		items = v.items
		bytes = v.bytes
		limits = v.limits
	case *Sieve:
		v.mu.Lock()
		defer v.mu.Unlock()
		order = &v.order
		items = v.items
		bytes = v.bytes
		hand = v.hand
		limits = v.limits
	default:
		fail("unsupported cache type %T", c)
	}

	if len(items) != order.Len() {
		fail("map length = %d, list length = %d", len(items), order.Len())
	}
	if limits.MaxFrames <= 0 && order.Len() != 0 {
		fail("disabled cache holds %d frames", order.Len())
	}
	if limits.MaxFrames > 0 && order.Len() > limits.MaxFrames {
		fail("cache holds %d frames, limit is %d", order.Len(), limits.MaxFrames)
	}
	if limits.MaxBytes > 0 && bytes > limits.MaxBytes {
		fail("cache holds %d bytes, limit is %d", bytes, limits.MaxBytes)
	}
	if order.Len() == 0 && hand != nil {
		fail("empty Sieve cache has non-nil hand")
	}

	var sum uint64
	seen := make(map[Key]bool, order.Len())
	for elem := order.Front(); elem != nil; elem = elem.Next() {
		key, data := cacheElement(t, elem, context)
		if seen[key] {
			fail("duplicate key %+v", key)
		}
		seen[key] = true
		if items[key] != elem {
			fail("map element mismatch for key %+v", key)
		}
		sum += uint64(len(data))
	}
	if sum != bytes {
		fail("byte accounting = %d, want %d", bytes, sum)
	}
}

func cacheElement(t *testing.T, elem *list.Element, context string) (Key, []byte) {
	t.Helper()

	switch entry := elem.Value.(type) {
	case *fifoEntry:
		return entry.key, entry.data
	case *lruEntry:
		return entry.key, entry.data
	case *sieveEntry:
		return entry.key, entry.data
	default:
		if context != "" {
			t.Fatalf("list entry has type %T (%s)", elem.Value, context)
		}
		t.Fatalf("list entry has type %T", elem.Value)
		return Key{}, nil
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
