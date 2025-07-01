package core

type InflightPQ struct {
	data     []*Message
	len, cap int
}

func calcGrowthCap(cap int) int {
	//fmt.Println("InflighQueue grow to", cap<<1)
	c := min(100000, cap<<1)
	return c
}

func (q *InflightPQ) swap(i, j int) {
	/* q.data[i].Index = j
	q.data[j].Index = i */

	m := q.data[i]
	q.data[i] = q.data[j]
	q.data[j] = m

	q.data[i].Index = i
	q.data[j].Index = j
}

func (q *InflightPQ) up(idx int) int {
	for idx > 0 {
		pidx := (idx - 1) >> 1
		if q.data[idx].Priority > q.data[pidx].Priority {
			return idx
		}
		q.swap(idx, pidx)
		idx = pidx
	}
	return idx
}

func (q *InflightPQ) Put(msg *Message) {
	if q.len == q.cap {
		q.cap = calcGrowthCap(q.cap)
		arr := make([]*Message, q.cap)
		copy(arr, q.data)
		q.data = arr
	}
	msg.Index = q.len
	q.data[q.len] = msg
	q.len++

	q.up(q.len - 1)
}

func (q *InflightPQ) down(idx int) {

	for idx < q.len {
		left, right := idx<<1, (idx<<1)|1
		if left >= q.len {
			return
		}
		cidx := left
		if right < q.len && q.data[right].Priority < q.data[left].Priority {
			cidx = right
		}
		//fmt.Println("cidx:", cidx)
		if q.data[cidx].Priority >= q.data[idx].Priority {
			return
		}
		q.swap(idx, cidx)
		idx = cidx
	}
}

func (q *InflightPQ) Len() int {
	return q.len
}

func (q *InflightPQ) IsEmpty() bool {
	return q.len == 0
}

func (q *InflightPQ) Peek() *Message {
	if q.len == 0 {
		return nil
	}
	return q.data[0]
}

func (q *InflightPQ) Pop() *Message {
	if q.len == 0 {
		return nil
	}
	ans := q.data[0]
	ans.Index = -1 // Index=-1 indicate that message is not in InflightPQ

	q.data[0] = q.data[q.len-1]
	q.data[q.len-1] = nil
	q.len--
	q.down(0)
	return ans
}

func (q *InflightPQ) Empty() {
	// TODO:
}
func (q *InflightPQ) GetAt(index int) *Message {
	return q.data[index]
}

func (q *InflightPQ) Remove(index int) (ans *Message) {
	if index < 0 || index >= q.len {
		return nil
	}
	ans = q.data[index]

	if index == q.len-1 {
		q.data[index] = nil
		q.len--
		ans.Index = -1
		return
	}
	q.swap(q.len-1, index)
	q.data[q.len-1] = nil

	q.len--
	idx := q.up(index)
	q.down(idx)
	ans.Index = -1
	return
}

func NewInflightPQ() InflightPQ {
	return InflightPQ{
		data: make([]*Message, 1),
		len:  0,
		cap:  1,
	}
}
