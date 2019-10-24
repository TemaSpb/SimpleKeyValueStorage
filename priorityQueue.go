package main

import (
	"container/heap"
	"log"
	"time"
)

// An Item is something we manage in a priority queue.
type Item struct {
	value      string
	expireDate time.Time
	index      int
}

// A PriorityQueue implements heap.Interface and holds Items
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].expireDate.Before(pq[j].expireDate)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) getMin() Item {
	log.Println(*pq)
	return *(*pq)[0]
}

func (pq *PriorityQueue) update(item *Item, expireDate time.Time) {
	item.expireDate = expireDate
	heap.Fix(pq, item.index)
}
