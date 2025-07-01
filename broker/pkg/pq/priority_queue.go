package pq

type Item struct {
	prior int
	value any
}

type PriorityQueue struct {
	items    []Item
	cap, len int
}

func NewPriorityQueue() PriorityQueue {
	return PriorityQueue{
		items: make([]Item, 1),
		cap:   1,
		len:   0,
	}
}

func (q *PriorityQueue) swap(i, j int) {
	temp := q.items[i]
	q.items[i] = q.items[j]
	q.items[j] = temp
}

func (q *PriorityQueue) up(idx int) {
	for idx > 0 {
		pid := (idx - 1) >> 1
		if q.items[pid].prior <= q.items[idx].prior {
			return
		}
		q.swap(idx, pid)
		idx = pid
	}
}

func (q *PriorityQueue) Put(prior int, value any) {
	if q.cap == q.len {
		arr := make([]Item, q.cap*2)
		q.cap += q.cap
		copy(arr, q.items)
		q.items = arr
	}
	q.items[q.len].value = value
	q.items[q.len].prior = prior
	q.len++
	q.up(q.len - 1)
}

func (q *PriorityQueue) down(idx int) {
	for idx < q.len {
		left, right := idx<<1, idx<<1|1
		if left >= q.len {
			return
		}
		cidx := left
		if right < q.len && q.items[right].prior < q.items[left].prior {
			cidx = right
		}
		if q.items[idx].prior <= q.items[cidx].prior {
			return
		}
		q.swap(idx, cidx)
		idx = cidx
	}
}

func (q *PriorityQueue) Len() int {
	return q.len
}

func (q *PriorityQueue) IsEmpty() bool {
	return q.len == 0
}

func (q *PriorityQueue) Peek() (priority int, item any) {
	if q.len == 0 {
		return 0, nil
	}
	return q.items[0].prior, q.items[0].value
}

func (q *PriorityQueue) Pop() any {
	if q.len == 0 {
		return nil
	}
	ans := q.items[0].value
	q.items[0] = q.items[q.len-1]
	q.len--
	q.down(0)
	return ans
}

func (q *PriorityQueue) Empty() {
	q.items = make([]Item, 1)
	q.len = 0
	q.cap = 1
}

func (q *PriorityQueue) GetAt(index int) any {
	if index < 0 || index >= q.len {
		panic("out of range")
	}
	return q.items[index]
}
