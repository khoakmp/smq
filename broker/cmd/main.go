package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khoakmp/smq/broker/core"
	"github.com/khoakmp/smq/broker/fqueue"
)

func genMessage(id core.MessageID, priority int, delay time.Duration) *core.Message {
	return &core.Message{
		ID:         id,
		ConsumerID: 1,
		Timestamp:  time.Now().Add(delay).Nanosecond(),
		Priority:   priority,
		Payload:    []byte("payload"),
	}
}

func runTCPClient() {
	ipAddr := os.Args[1]
	conn, err := net.Dial("tcp", ipAddr+":8080")
	if err != nil {
		fmt.Println(err)
		return
	}

	defer conn.Close()
	input := bufio.NewReader(os.Stdin)
	var buf [2048]byte

	for {
		line, _, err := input.ReadLine()
		if err != nil {
			fmt.Println(err)
			return
		}
		if len(line) == 0 {
			ts := time.Now().UnixNano()
			binary.BigEndian.PutUint64(buf[:8], uint64(ts))
			_, err := conn.Write(buf[:])

			if err != nil {
				fmt.Println(err)
				return
			}
		}
	}
}

func runTCPServer() {

	l, _ := net.Listen("tcp", ":8080")
	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}

	var buf [2048]byte
	for {
		_, err := io.ReadFull(conn, buf[:])
		if err != nil {
			fmt.Println("Failed to read data:", err)
			return
		}
		ts := int64(binary.BigEndian.Uint64(buf[:8]))
		delta := time.Duration(time.Now().UnixNano() - ts)
		fmt.Println("dur:", delta)
	}
}

func main() {

	/* ch := make(chan int, 10)
	n := 10000

	var s = 0
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			x, ok := <-ch
			if !ok {
				return
			}
			ans := 0
			for i := 2; i <= x; i++ {
				if x%i == 0 {
					ans++
				}
			}
			s += ans
		}

	}()
	st := time.Now()

	for range n {
		x := rand.Int() % 10000
		ch <- x
	}
	close(ch)
	wg.Wait()
	fmt.Println(time.Since(st)) */
	//testTCP()

	q := fqueue.NewFileQueue("data", "t1#tmp", fqueue.FileQueueConfig{
		MaxBytesPerFile: 2048,
		SyncEveryStep:   5,
	})
	var wg sync.WaitGroup

	pushFn := func(n int) {
		defer wg.Done()
		var msg []byte = []byte("hello")
		for range n {
			if err := q.Put(msg); err != nil {
				fmt.Println(err)
			}
		}
	}

	readFn := func(n int) {
		defer wg.Done()

		for range n {
			msg := <-q.ReadChan()

			if msg == nil {
				panic("msg is nil")
			}
		}
	}

	wg.Add(2)
	n := 3000

	go pushFn(n)
	go readFn(n)

	start := time.Now()
	wg.Wait()

	fmt.Println("total time:", time.Since(start))
	err := q.Delete()
	if err != nil {
		fmt.Println("failed to delete queue:", err)
	}

}

func cal() func() {
	var a = 10
	f := func() {
		a++
		fmt.Println(a)
	}
	a++
	return f
}

// take total 3ms to produce and push 10k message to topic channel
func testPushTopic() {
	topic := core.NewTopic("topic-1#tmp", nil, func(v any, persisted bool) {})
	startTime := time.Now()
	n := 10000
	recvDone := make(chan struct{})
	var messages []*core.Message = make([]*core.Message, 0, n)
	go func() {
		defer close(recvDone)
		cnt := 0
		for range n {
			msg := <-topic.MessageChan()
			messages = append(messages, msg)
			cnt++
		}
	}()
	for i := range n {
		var buf [16]byte
		binary.LittleEndian.PutUint64(buf[:8], uint64(i))
		msg := genMessage(core.MessageID(buf), 0, 0)
		msg.Index = i

		topic.PutMessage(msg)
	}
	fmt.Println("exec time:", time.Since(startTime))
	<-recvDone
	ans := 0
	for _, msg := range messages {
		ans += msg.Index
	}
	fmt.Println("ans:", ans)
}
func testPauseTopic() {
	topic := core.NewTopic("topic-1#tmp", nil, nil)
	topic.SetNotifyCb(func(v any, persisted bool) {})

	group1, _ := topic.FindConsumerGroup("group-1#tmp")
	group2, _ := topic.FindConsumerGroup("group-2#tmp")
	var cnt []int32 = make([]int32, 2)

	topic.Start()
	quitChan := make(chan struct{})

	recv := func(group *core.ConsumerGroup, groupIndex int) {

		for {
			select {
			case <-quitChan:
				return
			case <-group.MessageChan():
				atomic.AddInt32(&cnt[groupIndex], 1)
			}
		}
	}

	go func() {
		n := 10000

		for i := range n {
			var buf [16]byte
			binary.LittleEndian.PutUint64(buf[:8], uint64(i))
			msg := genMessage(core.MessageID(buf), 0, 0)
			topic.PutMessage(msg)
		}
	}()
	go recv(group1, 0)
	go recv(group2, 1)
	time.Sleep(time.Microsecond * 200)

	topic.Pause()
	time.Sleep(time.Microsecond * 100)
	n1 := atomic.LoadInt32(&cnt[0])
	fmt.Println("n1:", n1)
	n2 := atomic.LoadInt32(&cnt[1])
	fmt.Println("n2:", n2)
	time.Sleep(time.Millisecond)
	n11 := atomic.LoadInt32(&cnt[0])
	n21 := atomic.LoadInt32(&cnt[1])
	fmt.Println("n11:", n11)
	fmt.Println("n21:", n21)
	topic.Unpause()
	time.Sleep(time.Microsecond * 500)

	n11 = atomic.LoadInt32(&cnt[0])
	n21 = atomic.LoadInt32(&cnt[1])
	fmt.Println("n1:", n11)
	fmt.Println("n2:", n21)
}
