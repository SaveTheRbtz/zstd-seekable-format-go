package framecache

type intrusiveListEntry[T any] interface {
	comparable
	links() *intrusiveLinks[T]
}

type intrusiveLinks[T any] struct {
	prev T
	next T
}

//nolint:unused // Used through method promotion from embedded intrusiveLinks fields.
func (links *intrusiveLinks[T]) links() *intrusiveLinks[T] {
	return links
}

type intrusiveList[T intrusiveListEntry[T]] struct {
	head T
	tail T
	len  int
}

func (l *intrusiveList[T]) Init() {
	*l = intrusiveList[T]{}
}

func (l *intrusiveList[T]) Len() int {
	return l.len
}

func (l *intrusiveList[T]) Front() T {
	return l.head
}

func (l *intrusiveList[T]) Back() T {
	return l.tail
}

func (l *intrusiveList[T]) PushFront(entry T) T {
	var zero T

	entryLinks := entry.links()
	entryLinks.prev = zero
	entryLinks.next = l.head

	if l.head != zero {
		l.head.links().prev = entry
	} else {
		l.tail = entry
	}
	l.head = entry
	l.len++
	return entry
}

func (l *intrusiveList[T]) PushBack(entry T) T {
	var zero T

	entryLinks := entry.links()
	entryLinks.prev = l.tail
	entryLinks.next = zero

	if l.tail != zero {
		l.tail.links().next = entry
	} else {
		l.head = entry
	}
	l.tail = entry
	l.len++
	return entry
}

func (l *intrusiveList[T]) Remove(entry T) {
	var zero T

	entryLinks := entry.links()
	if entryLinks.prev != zero {
		entryLinks.prev.links().next = entryLinks.next
	} else {
		l.head = entryLinks.next
	}
	if entryLinks.next != zero {
		entryLinks.next.links().prev = entryLinks.prev
	} else {
		l.tail = entryLinks.prev
	}
	entryLinks.prev = zero
	entryLinks.next = zero
	l.len--
}

func (l *intrusiveList[T]) MoveToFront(entry T) {
	if l.head == entry {
		return
	}
	l.Remove(entry)
	l.PushFront(entry)
}

func (l *intrusiveList[T]) PrevCircular(entry T) T {
	var zero T

	if l.len <= 1 {
		return zero
	}
	if prev := entry.links().prev; prev != zero {
		return prev
	}
	return l.tail
}
