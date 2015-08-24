package hw2

import (
	"container/heap"
	"sort"
)

// An Item is something we manage in a priority queue.
type RecItem struct {
	Term string    // The priority of the item in the queue.
	Fname string
	Offset int64
	Length int64
}

type BlurbItem struct {
	Pos []int
	Index int
}

type Blurbs []*BlurbItem

func (b Blurbs) Less(i, j int) bool {
	return b[i].Pos[b[i].Index] < b[j].Pos[b[j].Index]
}

func (bs Blurbs) Len() int { return len(bs) }

// A PriorityQueue implements heap.Interface and holds Items.
type RecItems []*RecItem

func (items RecItems) Swap(i, j int) {
	items[i], items[j] = items[j], items[i]
}

type ByTerm struct {RecItems}

func (s ByTerm) Less(i, j int) bool { return s.RecItems[i].Term < s.RecItems[j].Term }

func (pq RecItems) Len() int { return len(pq) }

func (h *Blurbs) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*BlurbItem))
}

func (bs Blurbs) Swap(i, j int) { bs[i], bs[j] = bs[j], bs[i] }

func (h *Blurbs) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func Sort(keys interface{}) {
	switch k := keys.(type) {
		case []string:
			sort.Sort(sort.StringSlice(k))
		case []int:
			sort.Ints(k)
	}
}

func InitPQ(pq *RecItems) {
	sort.Sort(ByTerm{*pq})
}

func InitBlurbs(bs *Blurbs) {
	heap.Init(bs)
}