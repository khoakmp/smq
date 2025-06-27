package cli

import (
	"github.com/khoakmp/smq/broker/core"
)

func HandlePublish(client *Client, state *core.BrokerBase, topicName string, msgBody []byte) error {
	topic := state.FindTopic(topicName)
	//fmt.Println("broker recv msg:", string(msgBody))
	topic.Measurer.TryRecordFirstRecvMsg()

	/*
		just for test
		if len(msgBody) == 3 {
			return errors.New("invalid message content")
		} */

	msg := core.MakeMessage(topic.NextMessageID(), msgBody)

	err := topic.PutMessage(msg)

	if err != nil {
		return err
	}

	// TODO: add other for stats and control state
	return nil
}

func HandlePublishMulti(client *Client, state *core.BrokerBase, arg []byte) {

}
