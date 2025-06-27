package bfpool

import (
	"bytes"
	"sync"
)

var bfpool sync.Pool

func GetBuffer() *bytes.Buffer {
	x := bfpool.Get()
	if x == nil {
		return bytes.NewBuffer(nil)
	}
	return x.(*bytes.Buffer)
}

func PutBuffer(buffer *bytes.Buffer) {
	buffer.Reset()
	bfpool.Put(buffer)
}
