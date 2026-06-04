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

func TestIntrusiveListInit(t *testing.T) {
	entries := newIntrusiveListTestEntries(2)
	var got intrusiveList[*intrusiveListTestEntry]
	got.PushBack(entries[0])
	got.PushBack(entries[1])

	got.Init()

	if got.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", got.Len())
	}
	if got.Front() != nil {
		t.Fatalf("Front() = %s, want nil", intrusiveListEntryName(got.Front()))
	}
	if got.Back() != nil {
		t.Fatalf("Back() = %s, want nil", intrusiveListEntryName(got.Back()))
	}
}

func FuzzIntrusiveListOperations(f *testing.F) {
	f.Add([]byte{})
	f.Add(encodeIntrusiveListSteps(pushBack(0), pushBack(1), pushBack(2)))
	f.Add(encodeIntrusiveListSteps(pushFront(0), pushFront(1), pushFront(2)))
	f.Add(encodeIntrusiveListSteps(pushBack(0), pushBack(1), removeEntry(1), removeEntry(0)))
	f.Add(encodeIntrusiveListSteps(pushBack(0), pushBack(1), moveToFront(1), removeFront()))
	f.Add(encodeIntrusiveListSteps(
		pushBack(0),
		checkPrevCircular(0),
		pushBack(1),
		pushBack(2),
		checkPrevCircular(0),
		checkPrevCircular(1),
		checkPrevCircular(2),
	))

	f.Fuzz(func(t *testing.T, rawSteps []byte) {
		const entryCount = 16
		if len(rawSteps) > 512 {
			rawSteps = rawSteps[:512]
		}

		steps := make([]intrusiveListStep, len(rawSteps))
		for i, rawStep := range rawSteps {
			steps[i] = decodeIntrusiveListStep(rawStep, entryCount)
		}
		runIntrusiveListSteps(t, steps)
	})
}

type intrusiveListOp byte

const (
	opPushFront intrusiveListOp = iota
	opPushBack
	opRemove
	opMoveToFront
	opRemoveFront
	opCheckPrevCircular
)

type intrusiveListStep struct {
	op    intrusiveListOp
	entry int
}

func pushFront(entry int) intrusiveListStep {
	return intrusiveListStep{op: opPushFront, entry: entry}
}

func pushBack(entry int) intrusiveListStep {
	return intrusiveListStep{op: opPushBack, entry: entry}
}

func removeEntry(entry int) intrusiveListStep {
	return intrusiveListStep{op: opRemove, entry: entry}
}

func moveToFront(entry int) intrusiveListStep {
	return intrusiveListStep{op: opMoveToFront, entry: entry}
}

func removeFront() intrusiveListStep {
	return intrusiveListStep{op: opRemoveFront}
}

func checkPrevCircular(entry int) intrusiveListStep {
	return intrusiveListStep{op: opCheckPrevCircular, entry: entry}
}

func runIntrusiveListSteps(t *testing.T, steps []intrusiveListStep) {
	t.Helper()

	const entryCount = 16
	entries := newIntrusiveListTestEntries(entryCount)
	elements := make([]*list.Element, entryCount)
	var got intrusiveList[*intrusiveListTestEntry]
	var want list.List

	assertIntrusiveListMatchesContainer(t, &got, &want)
	assertRemovedIntrusiveListEntriesDetached(t, entries, elements)
	for stepIndex, step := range steps {
		entryIndex := step.entry % entryCount
		entry := entries[entryIndex]
		elem := elements[entryIndex]

		switch step.op {
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
				t.Fatalf("step %d: Front() before Remove = %s, want %s",
					stepIndex, intrusiveListEntryName(gotFront), intrusiveListEntryName(wantFront))
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
					t.Fatalf("step %d: PrevCircular(%d) = %s, want %s",
						stepIndex, entry.id, intrusiveListEntryName(gotPrev), intrusiveListEntryName(wantPrev))
				}
			}
		}

		assertIntrusiveListMatchesContainer(t, &got, &want)
		assertRemovedIntrusiveListEntriesDetached(t, entries, elements)
	}
}

func encodeIntrusiveListSteps(steps ...intrusiveListStep) []byte {
	encoded := make([]byte, len(steps))
	for i, step := range steps {
		encoded[i] = step.encode()
	}
	return encoded
}

func decodeIntrusiveListStep(encoded byte, entryCount int) intrusiveListStep {
	return intrusiveListStep{
		op:    intrusiveListOp(encoded & 0x07),
		entry: int(encoded>>3) % entryCount,
	}
}

func (step intrusiveListStep) encode() byte {
	return byte(step.op)&0x07 | byte(step.entry<<3)
}

func newIntrusiveListTestEntries(n int) []*intrusiveListTestEntry {
	entries := make([]*intrusiveListTestEntry, n)
	for i := range entries {
		entries[i] = &intrusiveListTestEntry{id: i}
	}
	return entries
}

func assertIntrusiveListMatchesContainer(
	t *testing.T,
	got *intrusiveList[*intrusiveListTestEntry],
	want *list.List,
) {
	t.Helper()

	if got.Len() != want.Len() {
		t.Fatalf("Len() = %d, want %d", got.Len(), want.Len())
	}
	if got.Front() != listElementEntry(want.Front()) {
		t.Fatalf("Front() = %s, want %s",
			intrusiveListEntryName(got.Front()), intrusiveListEntryName(listElementEntry(want.Front())))
	}
	if got.Back() != listElementEntry(want.Back()) {
		t.Fatalf("Back() = %s, want %s",
			intrusiveListEntryName(got.Back()), intrusiveListEntryName(listElementEntry(want.Back())))
	}

	prev := (*intrusiveListTestEntry)(nil)
	gotEntry := got.Front()
	for index, wantElem := 0, want.Front(); wantElem != nil; index, wantElem = index+1, wantElem.Next() {
		if gotEntry == nil {
			t.Fatalf("entry %d = nil, want %s", index, intrusiveListEntryName(listElementEntry(wantElem)))
		}
		if gotEntry != listElementEntry(wantElem) {
			t.Fatalf("entry %d = %s, want %s",
				index, intrusiveListEntryName(gotEntry), intrusiveListEntryName(listElementEntry(wantElem)))
		}
		if gotEntry.list.prev != prev {
			t.Fatalf("entry %d prev = %s, want %s",
				gotEntry.id, intrusiveListEntryName(gotEntry.list.prev), intrusiveListEntryName(prev))
		}

		prev = gotEntry
		gotEntry = gotEntry.list.next
	}
	if gotEntry != nil {
		t.Fatalf("list has trailing entry %s", intrusiveListEntryName(gotEntry))
	}
}

func assertRemovedIntrusiveListEntriesDetached(
	t *testing.T,
	entries []*intrusiveListTestEntry,
	elements []*list.Element,
) {
	t.Helper()

	for i, entry := range entries {
		if elements[i] != nil {
			continue
		}
		if entry.list.prev != nil || entry.list.next != nil {
			t.Fatalf("entry %d links = prev %s next %s, want detached",
				entry.id, intrusiveListEntryName(entry.list.prev), intrusiveListEntryName(entry.list.next))
		}
	}
}

func prevCircularListElement(l *list.List, elem *list.Element) *intrusiveListTestEntry {
	if l.Len() <= 1 {
		return nil
	}
	if prev := elem.Prev(); prev != nil {
		return listElementEntry(prev)
	}
	return listElementEntry(l.Back())
}

func listElementEntry(elem *list.Element) *intrusiveListTestEntry {
	if elem == nil {
		return nil
	}
	return elem.Value.(*intrusiveListTestEntry)
}

func intrusiveListEntryName(entry *intrusiveListTestEntry) string {
	if entry == nil {
		return "nil"
	}
	return strconv.Itoa(entry.id)
}
