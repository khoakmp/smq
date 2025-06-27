package monitor

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

type Topic struct {
	Name           string
	ConsumerGroups map[string]int
	Brokers        map[string]*BrokerNetInfo // TODO: may change to object, not pointer
	mutex          sync.RWMutex
	usedCounter    int32
	exitFlag       int32
	deleteCb       func(t *Topic) error
}

func (t *Topic) FreeKeepAlive() {
	atomic.AddInt32(&t.usedCounter, -1)
	// TODO: may trigger check whether should delete this topic
}

func (t *Topic) GetGroupNames() (groups []string) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	for g := range t.ConsumerGroups {
		groups = append(groups, g)
	}
	return
}

type TopicState struct {
	GroupNames []string        `json:"groups"`
	Brokers    []BrokerNetInfo `json:"brokers"`
}

func (t *Topic) GetState() (ans TopicState) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	for group := range t.ConsumerGroups {
		ans.GroupNames = append(ans.GroupNames, group)
	}
	for _, b := range t.Brokers {
		ans.Brokers = append(ans.Brokers, *b)
	}
	return
}

func (t *Topic) shouldExit() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	if len(t.Brokers) > 0 && atomic.LoadInt32(&t.usedCounter) == 0 {
		return false
	}
	return atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1)
}

var ErrTopicExiting = errors.New("topic exiting")

// this function is called before register groups for broker
func (t *Topic) RegisterBroker(brokerID string, info *BrokerNetInfo) error {
	fmt.Println("go here")
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return ErrTopicExiting
	}
	t.Brokers[brokerID] = info
	return nil
}

func (t *Topic) UnregisterBroker(brokerID string, groupNames []string) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	for _, groupName := range groupNames {
		cnt, ok := t.ConsumerGroups[groupName]
		if !ok {
			continue
		}
		if cnt == 1 {
			delete(t.ConsumerGroups, groupName)
			continue
		}
		t.ConsumerGroups[groupName]--
	}

	delete(t.Brokers, brokerID)

	if len(t.Brokers) == 0 {
		go func() {
			if err := t.deleteCb(t); err != nil {
				// TODO: log errror only
			}
		}()
	}
}

func (t *Topic) RegisterBrokerGroups(brokerID string, groupNames []string) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if _, ok := t.Brokers[brokerID]; !ok {
		return errors.New("unregister broker")
	}
	for _, groupName := range groupNames {
		t.ConsumerGroups[groupName]++
	}
	return nil
}

func (t *Topic) RegisterBrokerGroup(brokerID string, groupName string) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return ErrTopicExiting
	}

	if _, ok := t.Brokers[brokerID]; !ok {
		return errors.New("unregister broker")
	}
	t.ConsumerGroups[groupName]++
	return nil
}

func (t *Topic) UnregisterBrokerGroup(groupName string) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	counter, ok := t.ConsumerGroups[groupName]
	if !ok {
		return
	}
	if counter == 1 {
		delete(t.ConsumerGroups, groupName)
		return
	}
	t.ConsumerGroups[groupName] = counter - 1
}

// check group existed and add if not existed, return the number of brokers serving this group
func (t *Topic) ReserveGroup(groupName string) int {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	count, ok := t.ConsumerGroups[groupName]
	if ok {
		return count
	}
	t.ConsumerGroups[groupName] = 0
	return 0
}

func NewTopic(name string, deleteCb func(t *Topic) error) *Topic {
	return &Topic{
		Name:           name,
		ConsumerGroups: make(map[string]int),
		deleteCb:       deleteCb,
		usedCounter:    0,
		exitFlag:       0,
		Brokers:        make(map[string]*BrokerNetInfo),
	}
}

type BrokerNetInfo struct {
	BroadcastTCPPort  uint16 `json:"broadcast_tcp_port"`
	BroadcastHTTPPort uint16 `json:"broadcast_http_port"`
	Hostname          string `json:"hostname"`
}

type ClusterStore struct {
	mutex  sync.RWMutex
	topics map[string]*Topic
}

func NewClusterStore() *ClusterStore {
	return &ClusterStore{
		topics: make(map[string]*Topic),
	}
}

func (s *ClusterStore) DeleteTopic(t *Topic) error {
	s.mutex.Lock()
	if !t.shouldExit() {
		s.mutex.Unlock()
		return errors.New("topic is exiting or still has registering broker")
	}

	delete(s.topics, t.Name)
	s.mutex.Unlock()
	return nil
}

/*
when get one topic out of store, it ensure that topic is alive (not exiting)
but when get it out, another goroutine can call to delete this topic, set topic.exitFlag = 1,
so the next operator on this topic may be failed
*/

func (s *ClusterStore) GetTopic(topicName string, keepAlive bool) *Topic {
	s.mutex.RLock()
	topic, ok := s.topics[topicName]
	if ok {
		if keepAlive {
			atomic.AddInt32(&topic.usedCounter, 1)
		}
		s.mutex.RUnlock()
		return topic
	}

	s.mutex.RUnlock()
	topic = NewTopic(topicName, s.DeleteTopic)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if topic1, ok := s.topics[topicName]; ok {
		return topic1
	}
	s.topics[topicName] = topic
	if keepAlive {
		atomic.AddInt32(&topic.usedCounter, 1)
	}
	return topic
}
