package core

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestLoadState(t *testing.T) {
	notifyChan := make(chan any)
	go func() {
		for {
			<-notifyChan
		}
	}()

	state := CreateBrokerBaseForTest(notifyChan)
	opts.MetadataFilename = "testdata/meta"
	fmt.Println("hey")
	state.InitFromMetadata()
	topic1 := state.FindTopic("topic-1#tmp")
	topic1.FindConsumerGroup("group-1#tmp")

	topic2 := state.FindTopic("topic-2")
	topic2.FindConsumerGroup("group-2")

	meta := state.GetMetadata(true)
	fmt.Println(meta.Str())
	fmt.Println("reach here")
	state.Close()
}
func TestDeleteTopic(t *testing.T) {
	notifyChan := make(chan any)

	// only notify monitor in 2 cases
	// 1. one specified consumer group is deleted out of the a topic
	// 2. one specified topic is deleted out of state

	go func() {
		for {
			v := <-notifyChan
			switch v := v.(type) {
			case *Topic:
				fmt.Println("topic", v.Name, "is deleted")
			case *ConsumerGroup:
				fmt.Println("consumer group", v.Name, "is deleted")
			}
		}
	}()

	state := CreateBrokerBaseForTest(notifyChan)
	opts.MetadataFilename = "testdata/meta"
	state.InitFromMetadata()

	topic2 := state.FindTopic("topic-2")
	topic2.FindConsumerGroup("group-2")

	err := state.DeleteTopic("topic-2")
	assert.Equal(t, nil, err)
	time.Sleep(time.Millisecond)
}

func TestScanQueue(t *testing.T) {
	broker := NewBrokerBase("SMQB-1", nil, nil)
	opts.MetadataFilename = "testdata/meta"
	go broker.scanQueueLoop()
	topicName1 := "t1#tmp"
	groupName1 := "g1#tmp"
	topic1 := broker.FindTopic(topicName1)
	g, err := topic1.FindConsumerGroup(groupName1)
	assert.Equal(t, nil, err)
	msg := MakeMessage(MessageID(uuid.New()), []byte("oak2"))

	err = topic1.PutMessage(msg)
	assert.Equal(t, nil, err)
	msg1 := <-g.messageChan
	g.StartInflight(msg1, ClientID(1), time.Second)
	mid, _ := uuid.FromBytes(msg1.ID[:])
	fmt.Println("Start inflight msg:", mid.String())

	time.Sleep(5 * time.Second)
	broker.Close()
	time.Sleep(time.Millisecond)
}
