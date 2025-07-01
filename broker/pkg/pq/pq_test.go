package pq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPriorityQueue(t *testing.T) {
	q := NewPriorityQueue()
	q.Put(13, 233)
	q.Put(2, 22)
	q.Put(1, 31)
	prior, v := q.Peek()
	assert.Equal(t, 1, prior)
	assert.Equal(t, 31, v)
	q.Pop()
	assert.Equal(t, 2, q.Len())
	v = q.Pop()
	assert.Equal(t, 22, v)
	assert.Equal(t, 1, q.len)
	prior, v = q.Peek()
	assert.Equal(t, 13, prior)
	assert.Equal(t, 233, v)
}
