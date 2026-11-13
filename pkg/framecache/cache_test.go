package framecache

import (
	"bytes"
	"testing"
)

type cacheFactory struct {
	name string
	new  func(Limits) testCache
}

type testCache interface {
	Cache
}

var cacheFactories = []cacheFactory{
	{name: "FIFO", new: func(l Limits) testCache { return NewFIFO(l) }},
	{name: "LRU", new: func(l Limits) testCache { return NewLRU(l) }},
	{name: "Sieve", new: func(l Limits) testCache { return NewSieve(l) }},
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
				c.Put(1, []byte("one"))
				c.Put(2, []byte("two"))
				_, _ = c.Get(1)
				c.Put(3, []byte("three"))
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
				c.Put(1, []byte("aa"))
				c.Put(2, []byte("bbb"))
				c.Put(1, []byte("ccc"))
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
				c.Put(1, []byte("one"))
				c.Put(2, []byte("two"))
				_, _ = c.Get(1)
				c.Put(3, []byte("three"))
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
				c.Put(1, []byte("one"))
				c.Put(2, []byte("two"))
				_, _ = c.Get(1)
				c.Put(3, []byte("three"))
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
				c.Put(1, []byte("one"))
				_, _ = c.Get(1)
				c.Put(2, []byte("two"))
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
				c.Put(1, []byte("a"))
				c.Put(2, []byte("b"))
				_, _ = c.Get(1)
				_, _ = c.Get(2)
				c.Put(1, []byte("aa"))
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
		data, ok := c.Get(want.frameID)
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
			c.Put(1, []byte("a"))
			c.Put(1, []byte("bb"))
			got, ok := c.Get(1)
			if !ok || !bytes.Equal(got, []byte("bb")) {
				t.Fatalf("replacement Get(1) = %q, %t; want %q, true", got, ok, "bb")
			}

			c = factory.new(Limits{MaxFrames: 2, MaxBytes: 2})
			c.Put(1, []byte("a"))
			c.Put(1, []byte("bbb"))
			if got, ok := c.Get(1); ok {
				t.Fatalf("oversized replacement Get(1) = %q, true; want miss", got)
			}

			c = factory.new(Limits{MaxFrames: 3, MaxBytes: 5})
			c.Put(1, []byte("aa"))
			c.Put(2, []byte("bb"))
			c.Put(3, []byte("cc"))
			assertCacheResults(t, c, []cacheResult{
				{frameID: 1},
				{frameID: 2, data: "bb", ok: true},
				{frameID: 3, data: "cc", ok: true},
			})

			c = factory.new(Limits{MaxFrames: 0})
			c.Put(1, []byte("a"))
			if got, ok := c.Get(1); ok {
				t.Fatalf("disabled cache Get(1) = %q, true; want miss", got)
			}

			c = factory.new(Limits{MaxFrames: 2})
			c.Put(1, []byte("a"))
			c.Put(2, []byte("b"))
			c.Clear()
			if got, ok := c.Get(1); ok {
				t.Fatalf("Get(1) after Clear = %q, true; want miss", got)
			}
		})
	}
}

func TestSieveReplacementPreservesPositionAndHand(t *testing.T) {
	c := NewSieve(Limits{MaxFrames: 3, MaxBytes: 10})
	c.Put(1, []byte("a"))
	c.Put(2, []byte("bb"))
	c.Put(3, []byte("c"))

	elem := c.items[2]
	hand := c.hand
	c.Put(2, []byte("ddd"))

	if c.items[2] != elem {
		t.Fatal("replacement moved frame 2")
	}
	if c.hand != hand {
		t.Fatal("replacement moved hand")
	}
	entry := elem
	if entry.count != 1 {
		t.Fatalf("replacement counter = %d, want 1", entry.count)
	}
	if !bytes.Equal(entry.data, []byte("ddd")) {
		t.Fatalf("replacement data = %q, want ddd", entry.data)
	}
	if got, want := c.bytes, uint64(5); got != want {
		t.Fatalf("bytes = %d, want %d", got, want)
	}
}

func TestSieveKCounterGivesMultipleChances(t *testing.T) {
	c := NewSieve(Limits{MaxFrames: 2})
	c.Put(1, []byte("one"))
	c.Put(2, []byte("two"))
	_, _ = c.Get(1)
	_, _ = c.Get(1)

	c.Put(3, []byte("three"))
	c.Put(4, []byte("four"))

	if _, ok := c.items[1]; !ok {
		t.Fatal("twice-used frame was evicted before its counter reached zero")
	}
	if _, ok := c.items[2]; ok {
		t.Fatal("unused frame 2 is still cached")
	}
	if _, ok := c.items[3]; ok {
		t.Fatal("unused frame 3 is still cached")
	}
	if _, ok := c.items[4]; !ok {
		t.Fatal("new frame 4 is not cached")
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

	var count int
	var sum uint64
	var bytes uint64
	var limits Limits

	switch v := c.(type) {
	case *FIFO:
		count, sum = walkCacheEntries(fail, v.items, &v.order, nil, fifoEntryInfo)
		bytes = v.bytes
		limits = v.limits
	case *LRU:
		count, sum = walkCacheEntries(fail, v.items, &v.order, nil, lruEntryInfo)
		bytes = v.bytes
		limits = v.limits
	case *Sieve:
		count, sum = walkCacheEntries(fail, v.items, &v.order, v.hand, sieveEntryInfo)
		bytes = v.bytes
		limits = v.limits
	default:
		fail("unsupported cache type %T", c)
	}

	if limits.MaxFrames <= 0 && count != 0 {
		fail("disabled cache holds %d frames", count)
	}
	if limits.MaxFrames > 0 && count > limits.MaxFrames {
		fail("cache holds %d frames, limit is %d", count, limits.MaxFrames)
	}
	if limits.MaxBytes > 0 && bytes > limits.MaxBytes {
		fail("cache holds %d bytes, limit is %d", bytes, limits.MaxBytes)
	}
	if sum != bytes {
		fail("byte accounting = %d, want %d", bytes, sum)
	}
}

type cacheEntryInfo struct {
	frameID int64
	data    []byte
}

func fifoEntryInfo(entry *fifoEntry) cacheEntryInfo {
	return cacheEntryInfo{frameID: entry.frameID, data: entry.data}
}

func lruEntryInfo(entry *lruEntry) cacheEntryInfo {
	return cacheEntryInfo{frameID: entry.frameID, data: entry.data}
}

func sieveEntryInfo(entry *sieveEntry) cacheEntryInfo {
	return cacheEntryInfo{frameID: entry.frameID, data: entry.data}
}

func walkCacheEntries[T intrusiveListEntry[T]](
	fail func(string, ...any),
	items map[int64]T,
	order *intrusiveList[T],
	hand T,
	info func(T) cacheEntryInfo,
) (int, uint64) {
	seen := make(map[int64]bool, len(items))
	var zero T
	handSeen := hand == zero
	var count int
	var sum uint64
	var prev T
	for entry := order.Front(); entry != zero; entry = entry.links().next {
		entryInfo := info(entry)
		if entry.links().prev != prev {
			fail("broken prev link for frame ID %d", entryInfo.frameID)
		}
		if seen[entryInfo.frameID] {
			fail("duplicate frame ID %d", entryInfo.frameID)
		}
		seen[entryInfo.frameID] = true
		if items[entryInfo.frameID] != entry {
			fail("map element mismatch for frame ID %d", entryInfo.frameID)
		}
		if entry == hand {
			handSeen = true
		}
		count++
		sum += uint64(len(entryInfo.data))
		prev = entry
	}
	if count != len(items) {
		fail("map length = %d, list length = %d", len(items), count)
	}
	if count != order.Len() {
		fail("list length field = %d, walked %d", order.Len(), count)
	}
	if prev != order.Back() {
		fail("tail mismatch")
	}
	if !handSeen {
		fail("Sieve hand is not in cache")
	}
	return count, sum
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
