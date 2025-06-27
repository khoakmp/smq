package core

import (
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func prepare(n int) (map[MessageID]*Message, []*Message) {
	var messages map[MessageID]*Message = make(map[MessageID]*Message)
	var arr []*Message = make([]*Message, 0, n)
	for i := range n {
		id := MessageID(uuid.New())
		msg := genMessage(id, i, 0)
		arr = append(arr, msg)
		messages[id] = msg
	}
	rand.Shuffle(n, func(i, j int) {
		arr[i], arr[j] = arr[j], arr[i]
	})
	return messages, arr
}

func TestInflightPQ(t *testing.T) {
	t.Run("peek_pop_put", func(t *testing.T) {
		q := NewInflightPQ()
		n := 3
		messages, arr := prepare(n)
		cnt := 0
		for _, msg := range messages {
			msg.Priority = cnt
			cnt++
		}

		for i := range n {
			q.Put(arr[i])
		}

		assert.Equal(t, n, q.len)
		i := 0
		for !q.IsEmpty() {
			msg := q.Peek()
			assert.Equal(t, msg.Priority, i)
			assert.Equal(t, messages[msg.ID], msg)
			q.Pop()
			i++
		}
	})
	t.Run("remove", func(t *testing.T) {
		q := NewInflightPQ()
		n := 5
		_, arr := prepare(n)
		for i := range n {
			q.Put(arr[i])
		}
		msg := arr[3]
		msg1 := q.Remove(msg.Index)
		assert.Equal(t, msg1, msg)
		assert.Equal(t, n-1, q.Len())
		msg = arr[4]
		msg1 = q.Remove(msg.Index)
		assert.Equal(t, msg1, msg)
	})
}
