package ttlmap

type pqitem struct {
	key  string
	item *Item
}

type pqueue []*pqitem

func (pq pqueue) Len() int {
	return len(pq)
}

func (pq pqueue) Less(i, j int) bool {
	return pq[i].item.expiration.Before(pq[j].item.expiration)
}

func (pq pqueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].item.index = i
	pq[j].item.index = j
}

func (pq *pqueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*pqitem)
	item.item.index = n
	*pq = append(*pq, item)
}

func (pq *pqueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.item.index = -1
	*pq = old[0 : n-1]
	return item
}

func (pq pqueue) peek() (*pqitem, bool) {
	if pq.Len() == 0 {
		return nil, false
	}
	return pq[0], true
}
