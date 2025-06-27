package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type MockConsumer struct {
	id     ClientID
	closed bool
}

type IntClientIDGenerator struct {
	counter int
}

func (g *IntClientIDGenerator) NextID() ClientID {
	g.counter++
	return ClientID(g.counter)
}

func (m *MockConsumer) Close()              { m.closed = true }
func (m *MockConsumer) ID() ClientID        { return m.id }
func (m *MockConsumer) TriggerUpdateState() {}

func TestConsumerGroupTemporary(t *testing.T) {
	group := newConsumerGroup("group-1#tmp", "topic-1#tmp", nil)

	assert.Equal(t, true, group.temporary)
	assert.Equal(t, false, group.Pausing())
	assert.Equal(t, false, group.Exiting())
	idGenerator := IntClientIDGenerator{}

	t.Run("putMessage", func(t *testing.T) {
		msg := genMessage(MessageID(uuid.New()), 10, 0)
		err := group.PutMessage(msg)
		assert.Equal(t, nil, err)
		gotMsg := <-group.messageChan
		assert.Equal(t, msg, gotMsg)
	})

	t.Run("delayqueue", func(t *testing.T) {
		msg := genMessage(MessageID(uuid.New()), 22, time.Second)
		group.AddDelayMessage(msg)
		gotMsg := group.delayQueue.Pop().(*Message)
		assert.Equal(t, msg, gotMsg)
		assert.Equal(t, 0, group.delayQueue.Len())
	})

	t.Run("delete", func(t *testing.T) {
		msg := genMessage(MessageID(uuid.New()), 22, 0)
		group.AddDelayMessage(msg)
		msg = genMessage(MessageID(uuid.New()), 12, 0)
		group.PutMessage(msg)

		group.Delete()
		assert.Equal(t, true, group.delayQueue.IsEmpty())
		assert.Equal(t, true, group.Exiting())
		msg = genMessage(MessageID(uuid.New()), 10, 0)
		err := group.PutMessage(msg)
		assert.Equal(t, ErrExiting, err)
	})

	t.Run("delete_concurent", func(t *testing.T) {
		group := newConsumerGroup("group-2#tmp", "topic-1#tmp", nil)
		msg := genMessage(MessageID(uuid.New()), 22, 0)
		group.AddDelayMessage(msg)
		var successCnt int32 = 0
		var wg sync.WaitGroup
		n := 5
		wg.Add(n)

		for range n {
			go func() {
				defer wg.Done()
				err := group.Delete()
				if err == nil {
					atomic.AddInt32(&successCnt, 1)
				} else {
					assert.Equal(t, ErrExiting, err)
				}
			}()
		}
		wg.Wait()
		assert.Equal(t, int32(1), successCnt)
	})
	t.Run("consumer", func(t *testing.T) {
		t.Run("add_duplicate_consumer_id", func(t *testing.T) {
			group := newConsumerGroup("group-2#tmp", "topic-1#tmp", nil)
			n := 5
			var wg sync.WaitGroup
			wg.Add(n)
			id := idGenerator.NextID()
			for range n {
				go func() {
					defer wg.Done()
					c := &MockConsumer{
						id: id,
					}
					err := group.AddMember(c)
					if err != nil {
						assert.Equal(t, ErrConsumerGroupExisted, err)
					}
				}()
			}
			wg.Wait()
			assert.Equal(t, 1, len(group.members))
		})

		t.Run("close_group_check_consumer_state", func(t *testing.T) {
			group := newConsumerGroup("group-2#tmp", "topic-1#tmp", nil)
			n := 5
			var wg sync.WaitGroup
			wg.Add(n)
			for range n {
				go func() {
					defer wg.Done()
					c := &MockConsumer{
						id: idGenerator.NextID(),
					}
					err := group.AddMember(c)
					if err != nil {
						assert.Equal(t, ErrConsumerGroupExisted, err)
					}
				}()
			}
			wg.Wait()
			group.Close()
			for _, c := range group.members {
				assert.Equal(t, true, c.(*MockConsumer).closed)
			}
		})
	})

	t.Run("delete_consumer", func(t *testing.T) {
		var deletted bool = false
		group := newConsumerGroup("group3#tmp", "topic-1#tmp", nil)
		ctx, cancel := context.WithCancel(context.Background())
		group.deleteCb = func(groupName string) error {
			group.Delete()
			deletted = true
			cancel()
			return nil
		}
		assert.Equal(t, true, group.temporary)
		var n int = 5
		var wg sync.WaitGroup
		wg.Add(n)
		for range n {
			go func() {
				defer wg.Done()
				id := idGenerator.NextID()
				c := &MockConsumer{
					id: id,
				}
				group.AddMember(c)
				//v := rand.Int() % 10
				time.Sleep(time.Millisecond)
				group.DeleteConsumer(id)
				assert.Equal(t, true, c.closed)
			}()
		}
		wg.Wait()
		<-ctx.Done()
		assert.Equal(t, true, group.Exiting())
		assert.Equal(t, true, deletted)
	})

	t.Run("inflight_acknowledge", func(t *testing.T) {
		group := newConsumerGroup("group-2#tmp", "topic-1#tmp", nil)
		n := 100
		var wg sync.WaitGroup
		wg.Add(n)
		var messages []*Message = make([]*Message, n)

		for i := range n {
			msg := genMessage(MessageID(uuid.New()), 0, 0)
			//msg.ConsumerID = 1
			messages[i] = msg

			go func() {
				defer wg.Done()
				group.StartInflight(msg, 1, time.Second)
			}()
		}
		wg.Wait()
		for i := range n {
			msg := messages[i]
			err := group.FinishMessage(msg.ID, 1)
			assert.Equal(t, nil, err)
		}
		assert.Equal(t, 0, group.inflightPQ.len)
		assert.Equal(t, 0, len(group.inflightMessages))
	})

	t.Run("inflight2", func(t *testing.T) {
		group := newConsumerGroup("group2#tmp", "topic-1#tmp", nil)
		msg := genMessage(MessageID(uuid.New()), 0, 0)
		group.StartInflight(msg, 1, time.Second)
		gotMsg := group.inflightPQ.Peek()
		deadline := time.Now().Add(time.Second).UnixNano()
		delta := deadline - int64(gotMsg.Priority)
		fmt.Println(delta < int64(time.Nanosecond)*3000)
	})

	t.Run("empty_message", func(t *testing.T) {
		group := newConsumerGroup("group3#tmp", "topic-1#tmp", nil)
		numWorker := 10
		numMsgPerWorker := 1000
		var wg sync.WaitGroup
		wg.Add(numWorker)
		for range numWorker {
			go func() {
				defer wg.Done()
				for range numMsgPerWorker {
					msg := genMessage(MessageID(uuid.New()), 0, 0)
					group.PutMessage(msg)
				}
			}()
		}
		wg.Wait()
		group.EmptyMessages(true)
		var cnt = 0
		select {
		case <-group.messageChan:
			cnt++
		default:
		}
		assert.Equal(t, 0, cnt)
		assert.Equal(t, 0, group.inflightPQ.Len())
		assert.Equal(t, 0, len(group.inflightMessages))
		assert.Equal(t, 0, group.delayQueue.Len())
	})

}
