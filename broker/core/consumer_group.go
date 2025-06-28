package core

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/khoakmp/smq/broker/pkg/pq"
)

type ClientID int64

type ConsumerGroup struct {
	Name           string
	TopicName      string
	temporary      bool
	messageChan    chan *Message
	peristentQueue PersistentQueue

	lock    sync.RWMutex // lock guards for consumers only, not message queues
	members map[ClientID]ConsumerGroupMember

	delayLock        sync.Mutex
	delayQueue       pq.PriorityQueue
	inflightLock     sync.Mutex
	inflightPQ       InflightPQ
	inflightMessages map[MessageID]*Message

	exitLock sync.RWMutex
	exiting  int32
	pausing  int32
	deleter  sync.Once
	deleteCb func(groupName string) error
	notify   func(v any, peristed bool)

	// adding counter to count number of messages in queue()
	ReadyMsgCount    int64 //
	InflightMsgCount int64
	Measurer         *Measurer
}

func (c *ConsumerGroup) RLock()   { c.lock.RLock() }
func (c *ConsumerGroup) RUnlock() { c.lock.RUnlock() }
func (c *ConsumerGroup) Lock()    { c.lock.Lock() }
func (c *ConsumerGroup) Unlock()  { c.lock.Unlock() }

func NewConsumerGroup(name, topicName string, deleteCb func(groupName string) error, notify func(v any, persisted bool)) *ConsumerGroup {
	cg := newConsumerGroup(name, topicName, deleteCb)
	cg.notify = notify

	notify(cg, !cg.temporary)

	return cg
}

func (cg *ConsumerGroup) SetMeasurer(m *Measurer) {
	cg.Measurer = m
}

func defaultNotify(v any, persisted bool) {}
func newConsumerGroup(name, topicName string, deleteCb func(groupName string) error) *ConsumerGroup {
	cg := &ConsumerGroup{
		Name:             name,
		TopicName:        topicName,
		temporary:        strings.HasSuffix(name, "#tmp"),
		messageChan:      make(chan *Message, opts.InMemQueueSize),
		members:          make(map[ClientID]ConsumerGroupMember),
		delayQueue:       pq.NewPriorityQueue(),
		inflightPQ:       NewInflightPQ(),
		inflightMessages: make(map[MessageID]*Message),
		exiting:          0,
		pausing:          0,
		deleteCb:         deleteCb,
		notify:           defaultNotify,
		Measurer:         NewMeasurer(),
	}
	if cg.temporary {
		cg.peristentQueue = &MockPersistentQueue{}
	} else {
		// TODO: replace by diskqueue
		cg.peristentQueue = &MockPersistentQueue{}
	}
	return cg
}

func (cg *ConsumerGroup) MessageChan() chan *Message {
	return cg.messageChan
}

// 1. store it directly in this one dung
// van de se can lam gi de ma consider this one dung

func (cg *ConsumerGroup) PersistentQueue() PersistentQueue {
	return cg.peristentQueue
}

func (cg *ConsumerGroup) Exiting() bool {
	return atomic.LoadInt32(&cg.exiting) == 1
}

func (cg *ConsumerGroup) Pausing() bool {
	return atomic.LoadInt32(&cg.pausing) == 1
}

func (cg *ConsumerGroup) Pause() {
	if atomic.CompareAndSwapInt32(&cg.pausing, 0, 1) {
		for _, member := range cg.members {
			//fmt.Println("call trigger update ")
			member.TriggerUpdateState()
		}
	}
}

func (cg *ConsumerGroup) Unpause() {
	if atomic.CompareAndSwapInt32(&cg.pausing, 1, 0) {
		for _, member := range cg.members {
			member.TriggerUpdateState()
		}
	}
}

func (cg *ConsumerGroup) AddMember(c ConsumerGroupMember) error {
	cg.exitLock.RLock()
	defer cg.exitLock.RUnlock()

	if atomic.LoadInt32(&cg.exiting) == 1 {
		return ErrExiting
	}
	cg.Lock()
	if _, ok := cg.members[c.ID()]; ok {
		cg.Unlock()
		return ErrConsumerGroupExisted
	}
	log.Printf("Client %d join group %s\n", c.ID(), cg.Name)
	cg.members[c.ID()] = c
	cg.Unlock()
	return nil
}

func (cg *ConsumerGroup) DeleteConsumer(consumerID ClientID) error {
	cg.exitLock.RLock()
	defer cg.exitLock.RUnlock()

	if cg.Exiting() {
		return ErrExiting
	}
	cg.RLock()
	c, ok := cg.members[consumerID]
	if !ok {
		cg.RUnlock()
		return ErrConsumerGroupNotExisted
	}
	cg.RUnlock()
	c.Close()

	var l int
	cg.Lock()
	delete(cg.members, consumerID)
	l = len(cg.members)
	cg.Unlock()

	if l == 0 && cg.temporary {
		fmt.Printf("Delete consumer %d num(consumer) = 0\n", c.ID())

		// must ensure there is only one time call delete group successfull
		// call another goroutine to not block the caller wait for deleting group
		cg.deleter.Do(func() {
			go cg.deleteCb(cg.Name)
		})
	}
	return nil
}

func (c *ConsumerGroup) Delete() error {
	return c.shutdown(true)
}

func (c *ConsumerGroup) Close() error {
	return c.shutdown(false)
}

func (c *ConsumerGroup) shutdown(delete bool) error {
	c.exitLock.Lock()
	defer c.exitLock.Unlock()

	if !atomic.CompareAndSwapInt32(&c.exiting, 0, 1) {
		return ErrExiting
	}

	c.RLock()
	for _, member := range c.members {
		member.Close()
	}
	c.RUnlock()
	/* if c.notify == nil {
		fmt.Println("c.notify is nil")
	} */
	//fmt.Println("before notify")
	c.notify(c, !c.temporary)
	//fmt.Println("after notify")
	if delete {
		c.EmptyMessages(false)
		return c.peristentQueue.Delete()
	}
	c.SaveMessages()
	return c.peristentQueue.Close()
}

func (c *ConsumerGroup) EmptyMessages(emptyPeristent bool) {
	// does not need to acquire resource lock
LOOP:
	for {
		select {
		case <-c.messageChan:
		default:
			break LOOP
		}
	}

	c.inflightLock.Lock()
	c.inflightPQ = NewInflightPQ()
	c.inflightMessages = make(map[MessageID]*Message)
	c.inflightLock.Unlock()

	c.delayLock.Lock()
	c.delayQueue = pq.NewPriorityQueue()
	c.delayLock.Unlock()

	if emptyPeristent {
		c.peristentQueue.Empty()
	}
}

// SaveMessages is only called when consumerGroup exitting
func (c *ConsumerGroup) SaveMessages() {
LOOP:
	for {
		select {
		case msg := <-c.messageChan:
			persistMessage(c.peristentQueue, msg)
		default:
			break LOOP
		}
	}

	c.delayLock.Lock()
	for i := range c.delayQueue.Len() {
		msg := c.delayQueue.GetAt(i).(*Message)
		persistMessage(c.peristentQueue, msg)
	}
	c.delayLock.Unlock()

	c.inflightLock.Lock()
	for i := range c.inflightPQ.Len() {
		msg := c.inflightPQ.GetAt(i)
		persistMessage(c.peristentQueue, msg)
	}
	c.inflightLock.Unlock()
}

func (c *ConsumerGroup) PutMessage(msg *Message) error {
	c.exitLock.RLock()
	defer c.exitLock.RUnlock()

	if atomic.LoadInt32(&c.exiting) == 1 {
		return ErrExiting
	}
	// When putting message, it ensures that group is active, not exiting
	// so the diskqueue is active and can recv message
	return c.put(msg)
}

func (c *ConsumerGroup) put(msg *Message) error {
	select {
	case c.messageChan <- msg:
	default:
		c.Measurer.IncrWritePQueueGroup()
		buffer := bytes.NewBuffer(nil)
		msg.WriteTo(buffer)
		buf := buffer.Bytes()

		if err := c.peristentQueue.Put(buf); err != nil {
			//log.Printf("[ConsumerGroup %s] Failed to persist message, caused by %s\n", c.Name, err.Error())
			return err
		}
	}
	// when go here, message is successfully put to queue
	atomic.AddInt64(&c.ReadyMsgCount, 1)
	return nil
}

func (c *ConsumerGroup) AddDelayMessage(msg *Message) {
	c.exitLock.RLock()
	defer c.exitLock.RUnlock()
	if atomic.LoadInt32(&c.exiting) == 1 {
		return
	}
	// When adding delay message, it ensures that group is active, not exiting
	// so the delayqueue is active and can be put message
	c.delayLock.Lock()
	c.delayQueue.Put(msg.Timestamp, msg)
	c.delayLock.Unlock()
}

func (c *ConsumerGroup) StartInflight(msg *Message, clientID ClientID, timeout time.Duration) {
	//TODO: check exitlock, consider later
	msg.Priority = int(time.Now().Add(timeout).UnixNano())
	msg.ConsumerID = clientID

	/* c.inflightLock.Lock()
	c.inflightPQ.Put(msg)
	c.inflightMessages[msg.ID] = msg
	c.inflightLock.Unlock()

	atomic.AddInt64(&c.InflightMsgCount, 1)
	atomic.AddInt64(&c.ReadyMsgCount, -1) */
	c.putMessageInflightPQ(msg)
}

func (c *ConsumerGroup) putMessageInflightPQ(msg *Message) {
	c.inflightLock.Lock()
	c.inflightPQ.Put(msg)
	c.inflightMessages[msg.ID] = msg
	atomic.AddInt64(&c.InflightMsgCount, 1)
	c.inflightLock.Unlock()

	atomic.AddInt64(&c.ReadyMsgCount, -1)
}

func (c *ConsumerGroup) FinishMessage(msgID MessageID, clientID ClientID) error {
	// TODO: may check exitLock
	c.inflightLock.Lock()
	defer c.inflightLock.Unlock()

	msg, ok := c.inflightMessages[msgID]
	if !ok {
		return errors.New("message not found")
	}

	if msg.ConsumerID != clientID {
		id, _ := uuid.FromBytes(msgID[:])
		log.Printf("%c Failed to acknowledge msg, Msg %s is not owned by client %d \n", '\u274c', id.String(), clientID)
		return nil
	}

	c.inflightPQ.Remove(msg.Index)
	delete(c.inflightMessages, msgID)
	atomic.AddInt64(&c.InflightMsgCount, -1)

	c.Measurer.CountAndRecordFinish()
	// only in finish message function, we can putMessageToPool to reuse
	PutMessageToPool(msg)
	return nil
}

func (c *ConsumerGroup) RequeueMessage(msgID MessageID, clientID ClientID) error {
	// TODO: check exitlock
	c.inflightLock.Lock()
	msg, ok := c.inflightMessages[msgID]
	if !ok {
		c.inflightLock.Unlock()
		return errors.New("message not found")
	}
	if msg.ConsumerID != clientID {
		c.inflightLock.Unlock()
		return errors.New("message is not owned by this client")
	}
	c.inflightPQ.Remove(msg.Index)
	delete(c.inflightMessages, msgID)
	c.inflightLock.Unlock()
	atomic.AddInt64(&c.InflightMsgCount, -1)

	return c.put(msg)
}

// debug purpose
func (c *ConsumerGroup) InflightCount() int {
	// must ensure inflightPQ.Len() == len(inflightMessages)
	return c.inflightPQ.Len()
}

func (c *ConsumerGroup) ProcessInflightPQ(ts int) (positive bool) {
	c.exitLock.RLock()
	defer c.exitLock.RUnlock()

	if c.Exiting() {
		return
	}

	for {
		c.inflightLock.Lock()
		msg := c.inflightPQ.Peek()

		if msg == nil || msg.Priority > ts {
			c.inflightLock.Unlock()
			return
		}
		c.inflightPQ.Pop()
		delete(c.inflightMessages, msg.ID)
		c.inflightLock.Unlock()
		positive = true

		//mid, _ := uuid.FromBytes(msg.ID[:])
		//fmt.Println("Deleted MSG:", mid.String(), "from inflight Queue")

		atomic.AddInt64(&c.InflightMsgCount, -1)
		msg.ConsumerID = -1 // indicate that this messages is not owned by any client
		c.put(msg)          //  not acquire exitlock more
	}
}

// TODO: add process delay queue + test
