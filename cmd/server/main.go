package main

import (
	"container/list"
	"fmt"
)

func main() {
	l := list.New()

	a := l.PushBack(1)    // [1]
	b := l.PushBack(2)    // [1 2]
	l.PushFront(0)        // [0 1 2]
	l.InsertBefore(99, b) // [0 1 99 2]
	l.InsertAfter(88, a)  // [0 1 88 99 2]

	l.MoveToFront(b) // [2 0 1 88 99]
	l.Remove(a)      // [2 0 88 99]

	for e := l.Front(); e != nil; e = e.Next() {
		fmt.Print(e.Value, " ") // 2 0 88 99
	}

	fmt.Println("\nlen:", l.Len()) // 4
}
