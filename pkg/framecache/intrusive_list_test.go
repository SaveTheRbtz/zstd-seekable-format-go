package framecache

import (
	"container/list"
	"strconv"
	"testing"
)

type intrusiveListTestEntry struct {
	id   int
	list intrusiveLinks[*intrusiveListTestEntry]
}

func (entry *intrusiveListTestEntry) links() *intrusiveLinks[*intrusiveListTestEntry] {
	return &entry.list
}

func TestIntrusiveListPush(t *testing.T) {
	entries := newIntrusiveListTestEntries(3)
	var list intrusiveList[*intrusiveListTestEntry]

	assertIntrusiveList(t, &list)

	list.PushBack(entries[0])
	assertIntrusiveList(t, &list, entries[0])

	list.PushBack(entries[1])
	assertIntrusiveList(t, &list, entries[0], entries[1])

	list.PushFront(entries[2])
	assertIntrusiveList(t, &list, entries[2], entries[0], entries[1])
}

func TestIntrusiveListRemove(t *testing.T) {
	entries := newIntrusiveListTestEntries(4)
	var list intrusiveList[*intrusiveListTestEntry]
	for _, entry := range entries {
		list.PushBack(entry)
	}

	list.Remove(entries[1])
	assertDetachedIntrusiveListEntry(t, entries[1])
	assertIntrusiveList(t, &list, entries[0], entries[2], entries[3])

	list.Remove(entries[0])
	assertDetachedIntrusiveListEntry(t, entries[0])
	assertIntrusiveList(t, &list, entries[2], entries[3])

	list.Remove(entries[3])
	assertDetachedIntrusiveListEntry(t, entries[3])
	assertIntrusiveList(t, &list, entries[2])

	list.Remove(entries[2])
	assertDetachedIntrusiveListEntry(t, entries[2])
	assertIntrusiveList(t, &list)
}

func TestIntrusiveListMoveToFront(t *testing.T) {
	entries := newIntrusiveListTestEntries(4)
	var list intrusiveList[*intrusiveListTestEntry]
	for _, entry := range entries {
		list.PushBack(entry)
	}

	list.MoveToFront(entries[2])
	assertIntrusiveList(t, &list, entries[2], entries[0], entries[1], entries[3])

	list.MoveToFront(entries[3])
	assertIntrusiveList(t, &list, entries[3], entries[2], entries[0], entries[1])

	list.MoveToFront(entries[3])
	assertIntrusiveList(t, &list, entries[3], entries[2], entries[0], entries[1])
}

func TestIntrusiveListPrevCircular(t *testing.T) {
	entries := newIntrusiveListTestEntries(3)
	var list intrusiveList[*intrusiveListTestEntry]

	list.PushBack(entries[0])
	if got := list.PrevCircular(entries[0]); got != nil {
		t.Fatalf("PrevCircular(single entry) = %s, want nil", intrusiveListEntryName(got))
	}

	list.PushBack(entries[1])
	list.PushBack(entries[2])

	assertPrevCircular(t, &list, entries[0], entries[2])
	assertPrevCircular(t, &list, entries[1], entries[0])
	assertPrevCircular(t, &list, entries[2], entries[1])
}

func TestIntrusiveListInit(t *testing.T) {
	entries := newIntrusiveListTestEntries(2)
	var list intrusiveList[*intrusiveListTestEntry]
	list.PushBack(entries[0])
	list.PushBack(entries[1])

	list.Init()

	assertIntrusiveList(t, &list)
}

func FuzzIntrusiveListOperations(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{
		intrusiveListOp(opPushBack, 0),
		intrusiveListOp(opPushBack, 1),
		intrusiveListOp(opPushBack, 2),
	})
	f.Add([]byte{
		intrusiveListOp(opPushFront, 0),
		intrusiveListOp(opPushFront, 1),
		intrusiveListOp(opPushFront, 2),
	})
	f.Add([]byte{
		intrusiveListOp(opPushBack, 0),
		intrusiveListOp(opPushBack, 1),
		intrusiveListOp(opRemove, 1),
		intrusiveListOp(opRemove, 0),
	})
	f.Add([]byte{
		intrusiveListOp(opPushBack, 0),
		intrusiveListOp(opPushBack, 1),
		intrusiveListOp(opMoveToFront, 1),
		intrusiveListOp(opRemoveFront, 0),
	})

	f.Fuzz(func(t *testing.T, rawOps []byte) {
		const entryCount = 16
		if len(rawOps) > 512 {
			rawOps = rawOps[:512]
		}

		entries := newIntrusiveListTestEntries(entryCount)
		elements := make([]*list.Element, entryCount)
		var got intrusiveList[*intrusiveListTestEntry]
		var want list.List

		for opIndex, rawOp := range rawOps {
			op, entryIndex := decodeIntrusiveListOp(rawOp, entryCount)
			entry := entries[entryIndex]
			elem := elements[entryIndex]

			switch op {
			case opPushFront:
				if elem == nil {
					got.PushFront(entry)
					elements[entryIndex] = want.PushFront(entry)
				}
			case opPushBack:
				if elem == nil {
					got.PushBack(entry)
					elements[entryIndex] = want.PushBack(entry)
				}
			case opRemove:
				if elem != nil {
					got.Remove(entry)
					want.Remove(elem)
					elements[entryIndex] = nil
				}
			case opMoveToFront:
				if elem != nil {
					got.MoveToFront(entry)
					want.MoveToFront(elem)
				}
			case opRemoveFront:
				gotFront := got.Front()
				wantFront := listElementEntry(want.Front())
				if gotFront != wantFront {
					t.Fatalf("op %d: Front() before Remove = %s, want %s",
						opIndex, intrusiveListEntryName(gotFront), intrusiveListEntryName(wantFront))
				}
				if gotFront != nil {
					got.Remove(gotFront)
					want.Remove(want.Front())
					elements[gotFront.id] = nil
				}
			case opCheckPrevCircular:
				if elem != nil {
					gotPrev := got.PrevCircular(entry)
					wantPrev := prevCircularListElement(&want, elem)
					if gotPrev != wantPrev {
						t.Fatalf("op %d: PrevCircular(%d) = %s, want %s",
							opIndex, entry.id, intrusiveListEntryName(gotPrev), intrusiveListEntryName(wantPrev))
					}
				}
			}

			assertIntrusiveListMatchesContainer(t, &got, &want)
			assertDetachedIntrusiveListEntries(t, entries, elements)
		}
	})
}

const (
	opPushFront byte = iota
	opPushBack
	opRemove
	opMoveToFront
	opRemoveFront
	opCheckPrevCircular
)

func intrusiveListOp(op byte, entryIndex int) byte {
	return op&0x07 | byte(entryIndex<<3)
}

func decodeIntrusiveListOp(rawOp byte, entryCount int) (byte, int) {
	return rawOp & 0x07, int(rawOp>>3) % entryCount
}

func newIntrusiveListTestEntries(n int) []*intrusiveListTestEntry {
	entries := make([]*intrusiveListTestEntry, n)
	for i := range entries {
		entries[i] = &intrusiveListTestEntry{id: i}
	}
	return entries
}

func assertIntrusiveList(
	t *testing.T,
	list *intrusiveList[*intrusiveListTestEntry],
	want ...*intrusiveListTestEntry,
) {
	t.Helper()

	if got := list.Len(); got != len(want) {
		t.Fatalf("Len() = %d, want %d", got, len(want))
	}
	if got, want := list.Front(), firstIntrusiveListEntry(want); got != want {
		t.Fatalf("Front() = %s, want %s", intrusiveListEntryName(got), intrusiveListEntryName(want))
	}
	if got, want := list.Back(), lastIntrusiveListEntry(want); got != want {
		t.Fatalf("Back() = %s, want %s", intrusiveListEntryName(got), intrusiveListEntryName(want))
	}

	var prev *intrusiveListTestEntry
	count := 0
	for entry := list.Front(); entry != nil; entry = entry.list.next {
		if count >= len(want) {
			t.Fatalf("list has more than %d entries or contains a cycle", len(want))
		}
		if entry != want[count] {
			t.Fatalf("entry %d = %s, want %s",
				count, intrusiveListEntryName(entry), intrusiveListEntryName(want[count]))
		}
		if entry.list.prev != prev {
			t.Fatalf("entry %d prev = %s, want %s",
				entry.id, intrusiveListEntryName(entry.list.prev), intrusiveListEntryName(prev))
		}
		prev = entry
		count++
	}
	if count != len(want) {
		t.Fatalf("walked %d entries, want %d", count, len(want))
	}
}

func assertPrevCircular(
	t *testing.T,
	list *intrusiveList[*intrusiveListTestEntry],
	entry *intrusiveListTestEntry,
	want *intrusiveListTestEntry,
) {
	t.Helper()

	if got := list.PrevCircular(entry); got != want {
		t.Fatalf("PrevCircular(%d) = %s, want %s",
			entry.id, intrusiveListEntryName(got), intrusiveListEntryName(want))
	}
}

func assertDetachedIntrusiveListEntries(
	t *testing.T,
	entries []*intrusiveListTestEntry,
	elements []*list.Element,
) {
	t.Helper()

	for i, entry := range entries {
		if elements[i] == nil {
			assertDetachedIntrusiveListEntry(t, entry)
		}
	}
}

func assertDetachedIntrusiveListEntry(t *testing.T, entry *intrusiveListTestEntry) {
	t.Helper()

	if entry.list.prev != nil || entry.list.next != nil {
		t.Fatalf("entry %d links = prev %s next %s, want detached",
			entry.id, intrusiveListEntryName(entry.list.prev), intrusiveListEntryName(entry.list.next))
	}
}

func assertIntrusiveListMatchesContainer(
	t *testing.T,
	got *intrusiveList[*intrusiveListTestEntry],
	want *list.List,
) {
	t.Helper()

	entries := make([]*intrusiveListTestEntry, 0, want.Len())
	for elem := want.Front(); elem != nil; elem = elem.Next() {
		entries = append(entries, elem.Value.(*intrusiveListTestEntry))
	}
	assertIntrusiveList(t, got, entries...)
}

func prevCircularListElement(l *list.List, elem *list.Element) *intrusiveListTestEntry {
	if l.Len() <= 1 {
		return nil
	}
	if prev := elem.Prev(); prev != nil {
		return prev.Value.(*intrusiveListTestEntry)
	}
	return listElementEntry(l.Back())
}

func listElementEntry(elem *list.Element) *intrusiveListTestEntry {
	if elem == nil {
		return nil
	}
	return elem.Value.(*intrusiveListTestEntry)
}

func firstIntrusiveListEntry(entries []*intrusiveListTestEntry) *intrusiveListTestEntry {
	if len(entries) == 0 {
		return nil
	}
	return entries[0]
}

func lastIntrusiveListEntry(entries []*intrusiveListTestEntry) *intrusiveListTestEntry {
	if len(entries) == 0 {
		return nil
	}
	return entries[len(entries)-1]
}

func intrusiveListEntryName(entry *intrusiveListTestEntry) string {
	if entry == nil {
		return "nil"
	}
	return strconv.Itoa(entry.id)
}
