package cli

import (
	"github.com/khoakmp/smq/broker/core"
)

func HandlePublish(client *Client, state *core.BrokerBase, topicName string, msgBody []byte) error {
	topic := state.FindTopic(topicName)
	msg := core.MakeMessage(topic.NextMessageID(), msgBody)
	return topic.PutMessage(msg)
}

func HandlePublishMulti(client *Client, state *core.BrokerBase, topicName string, msgs [][]byte) error {
	topic := state.FindTopic(topicName)
	var messages []*core.Message
	for _, msgBody := range msgs {
		msg := core.MakeMessage(topic.NextMessageID(), msgBody)
		messages = append(messages, msg)
	}
	return topic.PutMessages(messages)
}
