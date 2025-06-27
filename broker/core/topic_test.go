package core

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func createMessageList(n int) []*Message {
	var messages []*Message = make([]*Message, n)
	for i := range n {
		messages[i] = genMessage(MessageID(uuid.New()), 0, 0)
	}
	return messages
}

func TestTopicTemporary(t *testing.T) {
	notifyFn := func(v any, persisted bool) {
		fmt.Println("something change")
	}
	topic := NewTopic("topic-1#tmp", nil, notifyFn)

	assert.Equal(t, true, topic.temporary)
	assert.Equal(t, 0, len(topic.csGroups))

	//group1 := NewConsumerGroup("group1#tmp", topic.DeleteGroup)
	//group2 := NewConsumerGroup("group2#tmp", topic.DeleteGroup)
	group1, err := topic.FindConsumerGroup("group1#tmp")
	assert.Equal(t, nil, err)
	group2, err := topic.FindConsumerGroup("group2#tmp")
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(topic.csGroups))

	topic.Start()
	n := 100
	messages := createMessageList(n)
	var wg sync.WaitGroup
	wg.Add(2)

	doneChan := make(chan struct{})
	recv := func(group *ConsumerGroup) {
		defer wg.Done()
		cnt := 0
	LOOP:
		for {
			select {
			case <-doneChan:
				break LOOP
			case msg := <-group.messageChan:
				assert.Equal(t, messages[cnt].ID, msg.ID)
				cnt++
			}
		}
		fmt.Println(group.Name, "recv:", cnt)
		assert.Equal(t, n, cnt)
	}

	go recv(group1)
	go recv(group2)

	topic.PutMessage(messages[0])

	//group3, err := topic.FindCsGroup("group3#tmp")

	assert.Equal(t, nil, err)
	for i := 1; i < n; i++ {
		topic.PutMessage(messages[i])
	}
	time.Sleep(time.Millisecond)

	close(doneChan)
	wg.Wait()
}

func TestAddGroupAtRuntime(t *testing.T) {
	notifyFn := func(v any, persisted bool) {
		fmt.Println("something change")
	}
	topic := NewTopic("topic-1#tmp", nil, notifyFn)

	topic.FindConsumerGroup("group-1#tmp")
	topic.FindConsumerGroup("group-2#tmp")

	topic.Start()

	n := 100
	messages := createMessageList(n)
	for i := 0; i < 10; i++ {
		messages[i].Index = i
		topic.PutMessage(messages[i])
	}
	group3, err := topic.FindConsumerGroup("group-3#tmp")
	assert.Equal(t, nil, err)
	cnt := 0
LOOP:
	for {
		select {
		case msg := <-group3.messageChan:
			fmt.Println("index:", msg.Index)
			cnt++
		default:
			break LOOP
		}
	}
	fmt.Println("group-3 recv:", cnt)
}
func TestRemoveGroup(t *testing.T) {
	var topicDeletted bool = false
	var groupDeletteds map[string]bool = make(map[string]bool)
	notifyFn := func(v any, persisted bool) {
		switch v := v.(type) {
		case *Topic:
			topicDeletted = true
		case *ConsumerGroup:
			if v.Exiting() {
				groupDeletteds[v.Name] = true
			}
		}
	}
	topic := NewTopic("topic-1#tmp", nil, notifyFn)

	topic.deleteCb = func(topicName string) error {
		notifyFn(topic, false)
		return nil
	}

	topic.notify = notifyFn
	group1Name := "group-1#tmp"
	groupDeletteds[group1Name] = false
	group1, _ := topic.FindConsumerGroup(group1Name)
	err := topic.DeleteGroup(group1Name)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, group1.Exiting())
	time.Sleep(time.Millisecond)

	assert.Equal(t, true, topicDeletted)
	assert.Equal(t, true, groupDeletteds[group1Name])

}
func TestTopicDelete(t *testing.T) {
	var groups map[string]bool = make(map[string]bool)
	var lock sync.Mutex
	notifyCb := func(v any, persisted bool) {
		switch v := v.(type) {
		case *ConsumerGroup:
			fmt.Println("hey, hre")
			fmt.Println("exiting:", v.Exiting())
			lock.Lock()
			groups[v.Name] = v.Exiting()
			lock.Unlock()
		}
	}
	topic := NewTopic("topic-1#tmp", nil, notifyCb)

	// when call topic.DeleteResource directly, it does not trigger topic.deleteCallback
	// topic.deletecall will actual call topic.DeleteResource

	topic.FindConsumerGroup("group-1#tmp")
	_, err := topic.FindConsumerGroup("group-1#tmp")
	assert.Equal(t, nil, err)
	topic.DeleteResource()
	assert.Equal(t, true, topic.Exiting())
	for groupName, exiting := range groups {
		fmt.Println(groupName, ":", exiting)
	}
}

func TestTopicPause(t *testing.T) {
	notifyFn := func(v any, persisted bool) {}
	topic := NewTopic("topic-1#tmp", nil, notifyFn)
	//topic.notify = func(v any, persisted bool) {}

	group1, _ := topic.FindConsumerGroup("group-1#tmp")
	group2, _ := topic.FindConsumerGroup("group-2#tmp")
	var cnt []int32 = make([]int32, 2)

	topic.Start()
	quitChan := make(chan struct{})

	recv := func(group *ConsumerGroup, groupIndex int) {

		for {
			select {
			case <-quitChan:
				return
			case <-group.messageChan:
				atomic.AddInt32(&cnt[groupIndex], 1)
			}
		}
	}

	go func() {
		n := 10000

		for i := range n {
			var buf [16]byte
			binary.LittleEndian.PutUint64(buf[:8], uint64(i))
			msg := genMessage(MessageID(buf), 0, 0)
			topic.PutMessage(msg)
		}
	}()
	go recv(group1, 0)
	go recv(group2, 1)
	time.Sleep(time.Microsecond * 200)
	// no se co van de t
	topic.Pause()
	time.Sleep(time.Microsecond * 100)
	n1 := atomic.LoadInt32(&cnt[0])
	fmt.Println("n1:", n1)
	n2 := atomic.LoadInt32(&cnt[1])
	fmt.Println("n2:", n2)
	time.Sleep(time.Millisecond * 10)
	n11 := atomic.LoadInt32(&cnt[0])
	n21 := atomic.LoadInt32(&cnt[1])

	assert.Equal(t, n1, n11)
	assert.Equal(t, n2, n21)
	topic.Unpause()
	time.Sleep(time.Microsecond * 500)

	n11 = atomic.LoadInt32(&cnt[0])
	n21 = atomic.LoadInt32(&cnt[1])
	fmt.Println("n1:", n11)
	fmt.Println("n2:", n21)
}
func PushMessageToTopic() []*Message {
	topic := NewTopic("topic-1#tmp", nil, func(v any, persisted bool) {})
	//startTime := time.Now()
	n := 10000
	recvDone := make(chan struct{})
	var messages []*Message = make([]*Message, 0, n)

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
		msg := genMessage(MessageID(buf), 0, 0)
		msg.Index = i

		topic.PutMessage(msg)
	}
	//fmt.Println("exec time:", time.Since(startTime))
	<-recvDone

	return messages
}

func BenchmarkPushTopic(b *testing.B) {
	for i := 0; i < b.N; i++ {
		PushMessageToTopic()
	}
}
