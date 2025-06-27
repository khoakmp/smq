package monitor

import (
	"testing"

	"github.com/khoakmp/smq/api/b2m"
	"github.com/stretchr/testify/assert"
)

func TestManipulateBrokerAndStore(t *testing.T) {
	m := NewMonitor()
	b := NewBrokerConn("localhost:5421", nil, m.DeleteBrokerPeer)
	b.specify(b2m.BrokerSpecification{
		BroadcastTCPPort:  5000,
		BroadcastHTTPPort: 8000,
	})

	topicName := "topic-1#tmp"
	groupName1 := "group-1#tmp"
	register(b, b2m.Registration{TopicName: topicName}, m)
	register(b, b2m.Registration{
		TopicName: topicName,
		GroupName: groupName1,
	}, m)

	topic := m.store.GetTopic(topicName, true)
	assert.Equal(t, topicName, topic.Name)
	assert.Equal(t, 1, len(topic.Brokers))
	assert.Equal(t, 1, len(topic.ConsumerGroups))
	groupNames := topic.GetGroupNames()
	assert.Equal(t, groupName1, groupNames[0])
	assert.Equal(t, int32(1), topic.usedCounter)

	assert.Equal(t, uint16(8000), topic.Brokers[b.addr].BroadcastHTTPPort)
	assert.Equal(t, uint16(5000), topic.Brokers[b.addr].BroadcastTCPPort)

	unregister(b, b2m.Registration{
		TopicName: topicName,
		GroupName: groupName1,
	}, m)

}
