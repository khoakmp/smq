package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khoakmp/smq/utils"
)

type TopicMetadata struct {
	TopicName      string   `json:"topic_name"`
	ConsumerGroups []string `json:"groups"`
}

func (m *TopicMetadata) Str() string {
	buffer := bytes.NewBuffer(nil)

	buffer.WriteString(fmt.Sprintf("Topic: %s, Groups: ", m.TopicName))
	buffer.WriteString(fmt.Sprint(m.ConsumerGroups))
	buffer.WriteByte('\n')

	return buffer.String()
}

type StateMetadata struct {
	Topics []TopicMetadata `json:"topics"`
}

func (s *StateMetadata) Marshal() []byte {
	buf, _ := json.Marshal(s)
	return buf
}

func (m *StateMetadata) Str() string {
	buffer := bytes.NewBuffer(nil)
	buffer.WriteString("StateMetadata: [")

	if len(m.Topics) > 0 {
		buffer.WriteByte('\n')
		for _, t := range m.Topics {
			buffer.WriteString(t.Str())
		}
	}
	buffer.WriteString("]")
	return buffer.String()
}

type BrokerBase struct {
	ID                        string
	topics                    map[string]*Topic
	notifyChan                chan any
	lock                      sync.RWMutex
	exiting                   int32
	exitChan                  chan struct{} // passed through constructor, Broker close this channel
	updateMonitorTCPAddrsChan chan struct{} // size 1
	monitorTCPAddrs           atomic.Value
	clusterQuerier            ClusterQuerier
	persistChan               chan struct{}
	Measurer                  *Measurer
	loading                   bool
}

type Groups struct {
	GroupNames []string `json:"groups"`
}

func (b *BrokerBase) Lock()    { b.lock.Lock() }
func (b *BrokerBase) Unlock()  { b.lock.Unlock() }
func (b *BrokerBase) RLock()   { b.lock.RLock() }
func (b *BrokerBase) RUnlock() { b.lock.RUnlock() }

func CreateBrokerBaseForTest(notifyChan chan any) *BrokerBase {
	// TODO: set options as param
	state := newBrokerBase()
	state.notifyChan = notifyChan
	return state
}

func newBrokerBase() *BrokerBase {
	return &BrokerBase{
		topics:                    make(map[string]*Topic),
		notifyChan:                nil,
		exiting:                   0,
		exitChan:                  make(chan struct{}),
		lock:                      sync.RWMutex{},
		monitorTCPAddrs:           atomic.Value{},
		updateMonitorTCPAddrsChan: make(chan struct{}, 1),
		clusterQuerier:            nil,
		persistChan:               make(chan struct{}),
		Measurer:                  NewMeasurer(),
	}
}

func NewBrokerBase(ID string, monitorTCPAddrs, monitorHttpAddrs []string) *BrokerBase {
	state := newBrokerBase()
	state.notifyChan = make(chan any)
	state.clusterQuerier = NewClusterQuerierV1(monitorHttpAddrs)
	state.monitorTCPAddrs.Store(monitorTCPAddrs)

	state.ID = ID
	go state.notifyLoop()
	go state.scanQueueLoop()

	state.InitFromMetadata()

	return state
}

func (b *BrokerBase) SetMonitorTCPAddrs(addrs []string) {
	b.monitorTCPAddrs.Swap(addrs)
	select {
	case b.updateMonitorTCPAddrsChan <- struct{}{}:
	default:
	}
}

func (b *BrokerBase) Notify(v any, persisted bool) {
	// TODO: separate persist to another dedicated goroutine
	if persisted && !b.loading {
		b.PersistMetadata()
	}

	// When broker is exiting, it may not handle for notifyChan
	select {
	case <-b.exitChan:
	case b.notifyChan <- v:
	}
}

// only call at initial time
func (b *BrokerBase) InitFromMetadata() {
	b.loading = true
	var metadata StateMetadata

	metaFileName := getMetadataFileName(b.ID)

	buf, err := os.ReadFile(metaFileName)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("file metadata not existed at:", metaFileName)
			return
		}
		panic(err)
	}

	if len(buf) == 0 {
		log.Println("failed to parse metadata stored in file: ", metaFileName)
		return
	}
	err = json.Unmarshal(buf, &metadata)
	if err != nil {
		log.Println("failed to parse metadata stored in file: ", metaFileName)
		return
	}

	for _, t := range metadata.Topics {
		topic := NewTopic(t.TopicName, b.DeleteTopic, b.Notify)
		log.Printf("Topic %s is created\n", t.TopicName)

		b.topics[t.TopicName] = topic
		for _, groupName := range t.ConsumerGroups {
			topic.FindConsumerGroup(groupName)
		}
		topic.Start()
	}
	b.loading = false
	log.Println("Broker State Intialized, ", metadata.Str())
}

func (b *BrokerBase) GetFullMetdata() StateMetadata {
	return b.GetMetadata(true)
}

func (b *BrokerBase) GetMetadata(temp bool) StateMetadata {
	b.RLock()
	defer b.RUnlock()

	meta := StateMetadata{
		Topics: make([]TopicMetadata, 0, len(b.topics)),
	}

	for topicName, topic := range b.topics {
		if !topic.temporary || temp {
			topic.rlock()
			groups := make([]string, 0, len(topic.csGroups))

			for groupName, g := range topic.csGroups {
				if !g.temporary || temp {
					groups = append(groups, groupName)
				}
			}

			topic.runlock()
			meta.Topics = append(meta.Topics, TopicMetadata{
				TopicName:      topicName,
				ConsumerGroups: groups,
			})
		}
	}
	return meta
}

// measure
func getMetadataFileName(brokerID string) string {
	return opts.MetadataFilename + brokerID
}

func openFileMetadata(brokerID string) *os.File {
	filename := getMetadataFileName(brokerID)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	return file
}

func (b *BrokerBase) PersistMetadata() {
	metadata := b.GetMetadata(false)

	buf, _ := json.Marshal(metadata)
	file := openFileMetadata(b.ID)
	file.Write(buf)
	file.Close()
	// TODO: write to temporary file, and rename to metadata file
}
func (b *BrokerBase) GetTopics() []*Topic {
	b.RLock()
	defer b.RUnlock()
	var topics []*Topic
	for _, t := range b.topics {
		topics = append(topics, t)
	}
	return topics
}

func (b *BrokerBase) Close() {
	if !atomic.CompareAndSwapInt32(&b.exiting, 0, 1) {
		return
	}

	b.PersistMetadata()
	topics := b.GetTopics()

	for _, topic := range topics {
		topic.CloseResource()
	}

	// close to exit notifyLoop and perist loop
	close(b.exitChan)
}

// this function is not called to init topic at initial time
func (b *BrokerBase) FindTopic(topicName string) *Topic {
	b.RLock()
	topic, ok := b.topics[topicName]
	b.RUnlock()

	if ok {
		//fmt.Println("found existed topic:", topicName)
		return topic
	}
	topic = NewTopic(topicName, b.DeleteTopic, b.Notify)
	//s.Notify(topic, !topic.temporary)

	b.Lock()
	t, ok := b.topics[topicName]
	if ok {
		b.Unlock()
		return t
	}

	b.topics[topicName] = topic
	b.Unlock()
	// each topic has it's own measurer
	//topic.SetMeasurer(b.Measurer)
	log.Printf("Topic %s is created\n", topicName)

	// TODO: call monitor to get consumer groups registered by client at monitor
	// after get all current registered consumer groups by clients, call topic.Start()

	if b.clusterQuerier != nil {
		log.Println("Call monitor to query groups of topic", topic.Name)
		groupNames := b.clusterQuerier.QueryGroupsOfTopic(topic.Name)
		for _, groupName := range groupNames {
			topic.FindConsumerGroup(groupName)
		}
	}
	topic.Start()
	return topic
}

func (b *BrokerBase) DeleteTopic(topicName string) error {
	b.RLock()
	topic, ok := b.topics[topicName]
	b.RUnlock()
	if !ok {
		return ErrTopicNotExisted
	}
	topic.DeleteResource()
	b.lock.Lock()
	delete(b.topics, topicName)
	b.lock.Unlock()
	//s.Notify(topic, !topic.temporary)
	return nil
}

func (b *BrokerBase) consumerGroups() []*ConsumerGroup {
	var groups []*ConsumerGroup = make([]*ConsumerGroup, 0)
	b.RLock()
	for _, topic := range b.topics {
		topic.rlock()
		for _, group := range topic.csGroups {
			groups = append(groups, group)
		}
		topic.runlock()
	}
	b.RUnlock()
	return groups
}

// should create another loop for Perist
func (b *BrokerBase) scanQueueLoop() {
	recalWorkerTicker := time.NewTicker(opts.ScanQueueRecalWorkerInterval)
	scanTicker := time.NewTicker(opts.ScanQueueInterval)

	var groups []*ConsumerGroup
	var numWorker int

	var groupChan chan *ConsumerGroup = make(chan *ConsumerGroup, opts.SampleGroupsCount)
	var stopChan chan struct{} = make(chan struct{})
	var resultChan chan bool = make(chan bool, opts.SampleGroupsCount)

	calcWorkers := func() {
		var w int
		if len(groups) > 0 {
			w = max(1, len(groups)>>2)
			w = min(12, w)
		}
		delta := w - numWorker
		if delta > 0 {
			for range delta {
				go scanWorker(groupChan, stopChan, resultChan)
			}

		} else {
			for range -delta {
				stopChan <- struct{}{}
			}
		}
		if w != numWorker {
			numWorker = w
			log.Println("ScanQueue Workers Updated:", numWorker)
		}
	}

	calcWorkers()
	log.Printf("Initiated ScanQueue Loop, num cgroups:%d, num workers:%d\n", len(groups), numWorker)

	for {
		select {
		case <-b.exitChan:
			goto RET
		case <-recalWorkerTicker.C:
			groups = b.consumerGroups()
			calcWorkers()

			continue
		case <-scanTicker.C:
			// at peak time, there are so many messages in every queue
			// so it takes time to handle one queue completely,
			// it should not block the caller on waiting to push next group
			// so groupChan should be buffered channel

			// in other case, there are groups, but there is no messages to scan in most queues (low traffic time),
			// so it does not need to scan all queues, just sampling a subset of groups,
			// try to scan them, continue if positive count > threshold
			if len(groups) == 0 {
				continue
			}

			for {
				n := min(opts.SampleGroupsCount, len(groups))
				indexes := utils.RandomSubset(len(groups), n)
				for _, idx := range indexes {
					groupChan <- groups[idx]
				}

				positiveCount := 0
				for range n {
					positive := <-resultChan
					if positive {
						positiveCount++
					}
				}
				if positiveCount == 0 || positiveCount < n*4/5 {
					break
				}
			}
		}

	}
RET:
	// destroy all worker
	for range numWorker {
		stopChan <- struct{}{}
	}
	log.Printf("%c STOP Scan Queue Loop\n", '\u2714')
}

func scanWorker(groupChan <-chan *ConsumerGroup, stopChan <-chan struct{}, resultChan chan<- bool) {
	for {
		select {
		case group := <-groupChan:
			now := int(time.Now().UnixNano())
			var positive bool
			if group.ProcessInflightQueue(now) {
				positive = true
			}

			if group.ProcessDelayQueue(now) {
				positive = true
			}

			// TODO: process delayQueue
			resultChan <- positive
		case <-stopChan:
			return
		}
	}
}

func (b *BrokerBase) PeristStateLoop() {
	//TODO:
}
