package pq

type Item struct {
	prior int
	value any
}

type PriorityQueue struct {
	data     []Item
	cap, len int
}

func NewPriorityQueue() PriorityQueue {
	return PriorityQueue{
		data: make([]Item, 1),
		cap:  1,
		len:  0,
	}
}

func (q *PriorityQueue) swap(i, j int) {
	temp := q.data[i]
	q.data[i] = q.data[j]
	q.data[j] = temp
}

func (q *PriorityQueue) up(idx int) {
	for idx > 0 {
		pid := (idx - 1) >> 1
		if q.data[pid].prior <= q.data[idx].prior {
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
		q.data = arr
	}
	q.data[q.len].value = value
	q.data[q.len].prior = prior
	q.up(q.len)
	q.len++
}

func (q *PriorityQueue) down(idx int) {
	for idx < q.len {
		left, right := idx<<1, idx<<1|1
		if left >= q.len {
			return
		}
		cidx := left
		if right < q.len && q.data[right].prior < q.data[left].prior {
			cidx = right
		}
		if q.data[idx].prior <= q.data[cidx].prior {
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

func (q *PriorityQueue) Peek() (int, any) {
	if q.len == 0 {
		panic("empty queue")
	}
	return q.data[0].prior, q.data[0].value
}

func (q *PriorityQueue) Pop() any {
	if q.len == 0 {
		panic("empty queue")
	}
	ans := q.data[0].value
	q.data[0] = q.data[q.len-1]
	q.len--
	q.down(0)
	return ans
}

func (q *PriorityQueue) Empty() {
	q.data = make([]Item, 1)
	q.len = 0
	q.cap = 1
}

func (q *PriorityQueue) GetAt(index int) any {
	if index < 0 || index >= q.len {
		panic("out of range")
	}
	return q.data[index]
}
