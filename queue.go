package blockstm

import (
	"container/heap"
	"sync"
)
type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]

	return x
}

type SafeQueue interface {
	Push(v int, d interface{})
	Pop() interface{}
	Len() int
}

type SafeFIFOQueue struct {
	c chan interface{}
}

func NewSafeFIFOQueue(capacity int) *SafeFIFOQueue {
	return &SafeFIFOQueue{
		c: make(chan interface{}, capacity),
	}
}

func (q *SafeFIFOQueue) Push(v int, d interface{}) {
	q.c <- d
}

func (q *SafeFIFOQueue) Pop() interface{} {
	return <-q.c
}

func (q *SafeFIFOQueue) Len() int {
	return len(q.c)
}

// A thread safe priority queue
type SafePriorityQueue struct {
	m     sync.Mutex
	queue *IntHeap
	data  map[int]interface{}
}

func NewSafePriorityQueue(capacity int) *SafePriorityQueue {
	q := make(IntHeap, 0, capacity)

	return &SafePriorityQueue{
		m:     sync.Mutex{},
		queue: &q,
		data:  make(map[int]interface{}, capacity),
	}
}

func (pq *SafePriorityQueue) Push(v int, d interface{}) {
	pq.m.Lock()

	heap.Push(pq.queue, v)
	pq.data[v] = d

	pq.m.Unlock()
}

func (pq *SafePriorityQueue) Pop() interface{} {
	pq.m.Lock()
	defer pq.m.Unlock()

	v := heap.Pop(pq.queue).(int)

	return pq.data[v]
}

func (pq *SafePriorityQueue) Len() int {
	return pq.queue.Len()
}
