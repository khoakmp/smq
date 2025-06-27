package core

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/khoakmp/smq/broker/bfpool"
)

var msgPool sync.Pool
var AllocMessageCount int64
var GetPooledMessageCount int64
var PutMessageToPoolCount int64

func PrintMessageAllocStats() {
	fmt.Println("Alloc Message Count:", atomic.LoadInt64(&AllocMessageCount))
	fmt.Println("Get In-Pool Message Count:", atomic.LoadInt64(&GetPooledMessageCount))
	fmt.Println("Put Message ToPool Count:", atomic.LoadInt64(&PutMessageToPoolCount))
}

func GetMessageFromPool() *Message {
	msg := msgPool.Get()
	if msg == nil {
		//atomic.AddInt64(&AllocMessageCount, 1)
		return new(Message)
	}
	//atomic.AddInt64(&GetPooledMessageCount, 1)
	return msg.(*Message)
}

func PutMessageToPool(msg *Message) {
	//fmt.Println("try to put msg.payload,len:", len(msg.Payload))
	bfpool.PutByteSlice(msg.Payload)
	msg.Reset()
	msgPool.Put(msg)
	//atomic.AddInt64(&PutMessageToPoolCount, 1)
}
