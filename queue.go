package blockstm

import (
	"github.com/JustinTimperio/gpq"
	"github.com/JustinTimperio/gpq/schema"
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

type SafeQueue[d any] interface {
	Push(v int64, data d)
	Pop() d
	Len() uint64
}

type SafeFIFOQueue[d any] struct {
	c chan d
}

func NewSafeFIFOQueue[d any](capacity int) *SafeFIFOQueue[d] {
	return &SafeFIFOQueue[d]{
		c: make(chan d, capacity),
	}
}

func (q *SafeFIFOQueue[d]) Push(v int64, data d) {
	q.c <- data
}

func (q *SafeFIFOQueue[d]) Pop() d {
	return <-q.c
}

func (q *SafeFIFOQueue[d]) Len() uint64 {
	return uint64(len(q.c))
}

type SafePriorityQueue[d any] struct {
	queue *gpq.GPQ[d]
}

func NewSafePriorityQueue[d any](capacity int) *SafePriorityQueue[d] {
	_, queue, err := gpq.NewGPQ[d](schema.GPQOptions{
		NumberOfBuckets:      capacity,
		DiskCacheEnabled:     false,
		LazyDiskCacheEnabled: false,
	})
	if err != nil {
		panic(err)
	}
	return &SafePriorityQueue[d]{
		queue: queue,
	}
}

func (pq *SafePriorityQueue[d]) Push(v int64, data d) {
	pq.queue.EnQueue(data, v, schema.EnQueueOptions{})
}

func (pq *SafePriorityQueue[d]) Pop() d {
	//  pq.queue.DeQueue()
	_, data, err := pq.queue.DeQueue()
	if err != nil {
		panic(err)
	}
	return data
}

func (pq *SafePriorityQueue[d]) Len() uint64 {
	return pq.queue.Len()
}
