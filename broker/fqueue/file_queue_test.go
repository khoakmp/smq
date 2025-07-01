package fqueue

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFileQueue(t *testing.T) {

	q := NewFileQueue("testdata", "t1#tmp", FileQueueConfig{
		MaxBytesPerFile: 1024,
		SyncEveryStep:   5,
	})

	var msg []byte = []byte("hello")
	err := q.Put(msg)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, q.len)

	data := <-q.ReadChan()
	assert.Equal(t, []byte("hello"), data)
	time.Sleep(time.Millisecond)
	assert.Equal(t, 0, q.len)
	err = q.Close()

	assert.Equal(t, nil, err)
}

func TestReadFile(t *testing.T) {
	f, err := os.Create("hey")
	assert.Equal(t, nil, err)
	f.Write([]byte("abc"))
	rf, _ := os.OpenFile("hey", os.O_RDONLY, 0644)
	var buf [3]byte
	_, err = rf.Read(buf[:])
	assert.Equal(t, nil, err)

	f.Close()
	rf.Close()
}

func TestCreateTemp(t *testing.T) {
	f, err := os.CreateTemp("testdata", "ha")
	fmt.Println(err)
	f.Close()
}
