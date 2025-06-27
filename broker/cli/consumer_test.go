package cli

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/khoakmp/smq/broker/core"
	"github.com/stretchr/testify/assert"
)

func randomUniqueInts(n, count int) (ans []int, remains []int) {
	if count > n {
		panic("count cannot be greater than n")
	}

	// Create a slice with numbers 0 to n-1
	nums := make([]int, n)
	for i := range n {
		nums[i] = i
	}

	rand.Shuffle(n, func(i, j int) {
		nums[i], nums[j] = nums[j], nums[i]
	})

	// Return the first `count` elements
	return nums[:count], nums[count:]
}
func genMessage(id core.MessageID, priority int, delay time.Duration) *core.Message {
	return &core.Message{
		ID:         id,
		ConsumerID: 1,
		Timestamp:  time.Now().Add(delay).Nanosecond(),
		Priority:   priority,
		Payload:    []byte("payload"),
	}
}

type MockProtocol struct {
	sentMessages map[core.MessageID]bool
	mutex        sync.Mutex
}

func (m *MockProtocol) IOLoop(client *Client, state *core.BrokerBase) {}
func (m *MockProtocol) SendMessage(client *Client, msg *core.Message) error {
	m.mutex.Lock()
	m.sentMessages[msg.ID] = true
	m.mutex.Unlock()
	client.writeLock.Lock()
	buf := bytes.NewBuffer(nil)
	msg.WriteTo(buf)
	client.writer.Write(buf.Bytes())
	client.writeLock.Unlock()
	return nil
}

func TestPushMessages(t *testing.T) {
	writeBuffer := bytes.NewBuffer(nil)
	client := &Client{
		ClientID:        1,
		netConn:         nil,
		reader:          nil,
		writer:          bufio.NewWriter(writeBuffer),
		closeChan:       make(chan struct{}),
		updateStateChan: make(chan struct{}, 1),
		opts:            defaultClientOptions(),
	}
	client.TraceID = "1"

	protocol := &MockProtocol{
		sentMessages: make(map[core.MessageID]bool),
	}
	notifyChan := make(chan any)
	go func() {
		for {
			<-notifyChan
		}
	}()
	state := core.CreateBrokerBaseForTest(notifyChan)
	// use one topic , one group

	topicName, groupName := "topic-1#tmp", "group-1#tmp"

	consumer, err := HandleSubscribe(protocol, client, state, topicName, groupName)
	assert.Equal(t, nil, err)
	topic := state.FindTopic(topicName)

	n := 10
	var messages []*core.Message = make([]*core.Message, n)

	for i := range n {
		messages[i] = genMessage(core.MessageID(uuid.New()), 0, 0)
		topic.PutMessage(messages[i])
	}

	time.Sleep(time.Millisecond)
	assert.Equal(t, int32(n), consumer.InflightMsgCount)

	for i := range n {
		msg := messages[i]
		assert.Equal(t, i, msg.Index)
		assert.Equal(t, true, protocol.sentMessages[msg.ID])
	}
	ackNum := 5
	ackIndexs, remainIndexs := randomUniqueInts(n, ackNum)

	for i := range len(ackIndexs) {
		HandleACK(consumer, messages[ackIndexs[i]].ID)
	}
	inflightCnt := n - ackNum
	assert.Equal(t, int32(inflightCnt), atomic.LoadInt32(&consumer.InflightMsgCount))
	assert.Equal(t, inflightCnt, consumer.group.InflightCount())
	time.Sleep(consumer.opts.FlushMessagesInterval + time.Millisecond)
	assert.Equal(t, true, writeBuffer.Len() > 0)

	consumer.group.Pause()
	msg := genMessage(core.MessageID(uuid.New()), 0, 0)
	fmt.Println("try to put message")
	topic.PutMessage(msg)

	time.Sleep(time.Millisecond)
	assert.Equal(t, int32(inflightCnt), consumer.InflightMsgCount)
	consumer.group.Unpause()
	time.Sleep(time.Millisecond)
	assert.Equal(t, int32(inflightCnt+1), consumer.InflightMsgCount)

	for i := range len(remainIndexs) {
		msg := messages[remainIndexs[i]]
		HandleACK(consumer, msg.ID)
	}

	assert.Equal(t, int32(1), consumer.InflightMsgCount)
	assert.Equal(t, 1, consumer.group.InflightCount())

}
