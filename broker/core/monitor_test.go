package core

import (
	"fmt"
	"testing"
	"time"

	"github.com/khoakmp/smq/monitor"
	"github.com/stretchr/testify/assert"
)

func TestMonitor(t *testing.T) {
	m := monitor.Run(":5050", ":8080")
	notifyChan := make(chan any)

	b := CreateBrokerBaseForTest(notifyChan)
	b.clusterQuerier = NewClusterQuerierV1([]string{"localhost:8080"})
	time.Sleep(time.Millisecond)

	go b.notifyLoop()
	b.SetMonitorTCPAddrs([]string{"localhost:5050"})

	topicName1 := "topic-1#tmp"
	groupName1 := "group-1#tmp"

	topicState := m.LookupTopic(topicName1, groupName1)

	assert.Equal(t, 0, len(topicState.Brokers))
	assert.Equal(t, 1, len(topicState.GroupNames))

	topic1 := b.FindTopic(topicName1)

	assert.Equal(t, true, topic1.temporary)
	//topicState := m.LookupTopic(topicName1, groupName1)

}

func TestQueryGroup(t *testing.T) {

	m := monitor.Run(":5051", ":8081")
	q := NewClusterQuerierV1([]string{"localhost:8081"})
	time.Sleep(time.Millisecond)
	topicName1 := "topic1#tmp"
	groups := q.QueryGroupsOfTopic(topicName1)
	fmt.Println(groups)
	gs := m.QueryTopicGroups(topicName1)
	fmt.Println(gs)
}
