package monitor

import (
	"errors"
	"strings"
)

// 1. store Topic dung
type TopicV2 struct {
	Name           string
	ConsumerGroups map[string]int
	Brokers        map[string]BrokerNetInfo
	ephemeral      bool
	deleteCb       func(t *TopicV2)
}

func (t *TopicV2) GetState() TopicState {
	return TopicState{
		GroupNames: t.GetGroupNames(),
		Brokers:    t.GetBrokers(),
	}
}

func (t *TopicV2) GetBrokers() []BrokerNetInfo {
	var brokers []BrokerNetInfo
	for _, b := range t.Brokers {
		brokers = append(brokers, b)
	}
	return brokers
}

func (t *TopicV2) GetGroupNames() []string {
	var groups []string
	for groupName := range t.ConsumerGroups {
		groups = append(groups, groupName)
	}
	return groups
}

func (t *TopicV2) ReserveGroup(groupName string) {
	if _, ok := t.ConsumerGroups[groupName]; !ok {
		//fmt.Println("add group:", groupName)
		t.ConsumerGroups[groupName] = 0
	}
}

func (t *TopicV2) RegisterBroker(brokerID string, netInfo BrokerNetInfo) {
	t.Brokers[brokerID] = netInfo
}

func (t *TopicV2) RegisterBrokerGroup(brokerID string, groupName string) error {
	if _, ok := t.Brokers[brokerID]; !ok {
		return errors.New("unregister broker")
	}
	t.ConsumerGroups[groupName]++
	return nil
}

func (t *TopicV2) RegisterBrokerGroups(brokerID string, groupNames []string) error {
	if _, ok := t.Brokers[brokerID]; !ok {
		return errors.New("unregister broker")
	}
	for _, groupName := range groupNames {
		t.ConsumerGroups[groupName]++
	}
	return nil
}

func (t *TopicV2) UnregisterBrokerGroup(brokerID string, groupName string) {
	if _, ok := t.ConsumerGroups[groupName]; ok {
		t.ConsumerGroups[groupName]--
	}
}

func (t *TopicV2) UnregisterBroker(brokerID string, groupNames []string) {
	for _, groupName := range groupNames {
		_, ok := t.ConsumerGroups[groupName]
		if !ok {
			continue
		}
		t.ConsumerGroups[groupName]--
	}
	delete(t.Brokers, brokerID)
	if len(t.Brokers) == 0 && t.ephemeral {
		t.deleteCb(t)
	}
}

func NewTopicV2(name string, deleteCb func(t *TopicV2)) *TopicV2 {
	return &TopicV2{
		ephemeral:      strings.HasSuffix(name, "temporary"),
		deleteCb:       deleteCb,
		Name:           name,
		ConsumerGroups: make(map[string]int),
		Brokers:        make(map[string]BrokerNetInfo),
	}
}

type ClusterStoreV2 struct {
	topics map[string]*TopicV2
}

func NewClusterStoreV2() *ClusterStoreV2 {
	return &ClusterStoreV2{
		topics: make(map[string]*TopicV2),
	}
}
func (c *ClusterStoreV2) DeleteTopic(t *TopicV2) {
	delete(c.topics, t.Name)
}

func (s *ClusterStoreV2) GetTopic(name string) *TopicV2 {
	if t, ok := s.topics[name]; ok {
		return t
	}
	topic := NewTopicV2(name, s.DeleteTopic)
	s.topics[name] = topic
	return topic
}
