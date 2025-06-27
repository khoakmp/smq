package core

import (
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type Topic struct {
	// Topic attributes: Name + persisted: unchange through life time of topic
	Name      string
	temporary bool

	pausing int32
	exiting int32

	mutex           sync.RWMutex
	messageChan     chan *Message
	persistentQueue PersistentQueue

	csGroups map[string]*ConsumerGroup
	deleteCb func(topicName string) error
	deleter  sync.Once
	notify   func(v any, persisted bool)

	updateCsGroupsChan chan struct{}
	pauseChan          chan struct{}
	startChan          chan struct{}
	exitChan           chan struct{}
	doneChan           chan struct{}
	Measurer           *Measurer // for testing
}

func (t *Topic) lock()    { t.mutex.Lock() }
func (t *Topic) unlock()  { t.mutex.Unlock() }
func (t *Topic) rlock()   { t.mutex.RLock() }
func (t *Topic) runlock() { t.mutex.RUnlock() }

func NewTopic(name string, deleteCb func(topicName string) error, notify func(v any, persisted bool)) *Topic {
	topic := &Topic{
		Name:               name,
		temporary:          strings.HasSuffix(name, "#tmp"),
		messageChan:        make(chan *Message, opts.InMemQueueSize),
		csGroups:           make(map[string]*ConsumerGroup),
		pausing:            0,
		exiting:            0,
		deleteCb:           deleteCb,
		notify:             notify,
		mutex:              sync.RWMutex{},
		updateCsGroupsChan: make(chan struct{}),
		pauseChan:          make(chan struct{}),
		startChan:          make(chan struct{}),
		exitChan:           make(chan struct{}),
		doneChan:           make(chan struct{}),
		Measurer:           NewMeasurer(), // used for testing
	}

	if topic.temporary {
		topic.persistentQueue = &MockPersistentQueue{}
	} else {
		topic.persistentQueue = &MockPersistentQueue{}
		// TODO: replace mockPeristentQueue by Diskqueue
	}
	notify(topic, !topic.temporary)
	go topic.forwardMessages()
	return topic
}

func (t *Topic) SetMeasurer(m *Measurer) {
	t.Measurer = m
}

func (t *Topic) NextMessageID() MessageID {
	return MessageID(uuid.New())
}

func (t *Topic) Start() {
	close(t.startChan)
}

func (t *Topic) Pause() {
	if atomic.CompareAndSwapInt32(&t.pausing, 0, 1) {
		// pause state is rechecked for each iteration of forwarding message loop
		// so it does not need to block on pushing pauseChan
		select {
		case t.pauseChan <- struct{}{}:
		default:
		}
	}
}

func (t *Topic) Unpause() {
	if atomic.CompareAndSwapInt32(&t.pausing, 1, 0) {
		select {
		case t.pauseChan <- struct{}{}:
		default:
		}
	}
}

func (t *Topic) Pausing() bool {
	return atomic.LoadInt32(&t.pausing) == 1
}

func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exiting) == 1
}

func (t *Topic) FindConsumerGroup(groupName string) (*ConsumerGroup, error) {
	if t.Exiting() {
		return nil, ErrExiting
	}
	t.rlock()
	cg, ok := t.csGroups[groupName]
	if ok {
		t.runlock()
		//fmt.Println("found existed consumer group:", groupName)
		return cg, nil
	}
	t.runlock()

	return t.createConsumerGroup(groupName)
}

func (t *Topic) createConsumerGroup(groupName string) (cg *ConsumerGroup, err error) {
	// there is only one point call func NewConsumerGroup and it's always called by topic
	cg = NewConsumerGroup(groupName, t.Name, t.DeleteGroup, t.notify)

	t.lock()
	if group, ok := t.csGroups[groupName]; ok {
		// if another goroutine had created this group, discard the created consumer group
		t.unlock()
		return group, nil
	}
	t.csGroups[groupName] = cg

	t.unlock()

	cg.SetMeasurer(t.Measurer) // TODO: remove later

	// TODO: should remove log when topic/group is created/deleted frequently
	log.Printf("ConsumerGroup %s of topic %s is created\n", groupName, t.Name)

	t.triggerUpdateGroups()
	//	t.notify(cg, !cg.temporary), let consumerGroup notify itself

	return
}
func (t *Topic) triggerUpdateGroups() {
	select {
	case t.updateCsGroupsChan <- struct{}{}:
	case <-t.exitChan:
	}
}

func (t *Topic) DeleteGroup(csgName string) error {
	if t.Exiting() {
		return ErrExiting
	}
	t.rlock()
	cg, ok := t.csGroups[csgName]
	if !ok {
		t.runlock()
		return ErrConsumerGroupNotExisted
	}
	t.runlock()

	cg.Delete()

	t.lock()
	delete(t.csGroups, csgName)
	var sz int = len(t.csGroups)
	t.unlock()
	t.triggerUpdateGroups()
	//t.notify(cg, !cg.temporary)

	if sz == 0 && t.temporary {
		t.deleter.Do(func() {
			go t.deleteCb(t.Name)
		})
	}
	return nil
}

func (t *Topic) PutMessage(msg *Message) error {
	if t.Exiting() {
		return ErrExiting
	}
	select {
	case t.messageChan <- msg:
		return nil
	default:
		t.Measurer.IncrWritePQueueTopic()
		return persistMessage(t.persistentQueue, msg)
	}
}

func (t *Topic) forwardMessages() {
	defer close(t.doneChan)
	for {
		select {
		case <-t.exitChan:
			return
		case <-t.pauseChan:
			continue
		case <-t.updateCsGroupsChan:
			continue
		case <-t.startChan:
			goto START
		}
	}
START:

	var messageChan chan *Message = t.messageChan
	var persistentQueueChan <-chan []byte = t.persistentQueue.ReadChan()

	var groups []*ConsumerGroup = make([]*ConsumerGroup, 0, len(t.csGroups))

	updateGroups := func() {
		t.rlock()
		for _, g := range t.csGroups {
			groups = append(groups, g)
		}
		t.runlock()

		if len(groups) == 0 {
			messageChan = nil
			persistentQueueChan = nil
		}
	}
	updateGroups()

	for {
		// should recheck pause state for each iteration

		if t.Pausing() || len(groups) == 0 {
			messageChan = nil
			persistentQueueChan = nil
		} else {
			messageChan = t.messageChan
			persistentQueueChan = t.persistentQueue.ReadChan()
		}
		var msg *Message
		select {
		case <-t.exitChan:
			return
		case <-t.pauseChan:
			// next iteration to update the latest pause state
			continue
		case <-t.updateCsGroupsChan:
			// avoid leak memory
			for i := range len(groups) {
				groups[i] = nil
			}
			groups = groups[:0]
			updateGroups()
			continue
		case msg = <-messageChan:
		case buf := <-persistentQueueChan:
			msg = new(Message)
			msg.Decode(buf)
		}

		if msg.Timestamp > int(time.Now().UnixNano()) {
			groups[0].AddDelayMessage(msg)
			for i := 1; i < len(groups); i++ {
				msg := msg.Clone()

				groups[i].AddDelayMessage(msg)
			}
			continue
		}

		groups[0].PutMessage(msg)

		for i := 1; i < len(groups); i++ {
			msg := msg.Clone()
			groups[i].PutMessage(msg)
		}
	}
}

func (t *Topic) DeleteResource() {
	t.shutdown(true)
}

func (t *Topic) CloseResource() {
	t.shutdown(false)
}

/*
func shutdown is called when
1. Admin explicitly calls to Delete topic (call the function passed tp topic.deleteCb)
2. Broker call Delete Topic when detect trigger by topic itself when there are no consumer groups
and topic is temporary
3. Broker exit,so Exit Topic first
*/
// 1. when call to close or exit or delete, it specifies explicitly delete or not ?
func (t *Topic) shutdown(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&t.exiting, 0, 1) {
		return ErrExiting
	}
	close(t.exitChan)
	<-t.doneChan
	groups := t.GetGroups()

	if deleted {
		for _, cg := range groups {
			cg.Delete()
		}
	} else {
		// deleted = false => shutdown function is called when broker is shutting down
		// so it just close and does not affect metadata
		for _, cg := range groups {
			cg.Close()
		}
	}

	// call notify will block on the caller, so it only return when this event is actually processed
	t.notify(t, !t.temporary)
	// notify before delete or persist messages
	if deleted {
		// TODO: empty all messages in messageChan, should call Put message to MessagePool to reuse

		return t.persistentQueue.Delete()
	}

LOOP:
	for {
		select {
		case msg := <-t.messageChan:
			persistMessage(t.persistentQueue, msg)
		default:
			break LOOP
		}
	}

	return t.persistentQueue.Close()
}

func (t *Topic) GetGroups() []*ConsumerGroup {
	t.rlock()
	defer t.runlock()
	var groups []*ConsumerGroup
	for _, g := range t.csGroups {
		groups = append(groups, g)
	}
	return groups
}
func (t *Topic) SetNotifyCb(fn func(v any, persisted bool)) {
	t.notify = fn
}

func (t *Topic) MessageChan() <-chan *Message {
	return t.messageChan
}
