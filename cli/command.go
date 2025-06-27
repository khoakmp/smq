package cli

import (
	"encoding/binary"
	"io"

	"github.com/khoakmp/smq/api/c2b"
	"github.com/khoakmp/smq/broker/core"
)

type Command struct {
	apiKey  byte
	payload []byte
}

func (c *Command) WriteTo(w io.Writer) (int64, error) {
	var buf [5]byte
	buf[0] = c.apiKey
	binary.BigEndian.PutUint32(buf[1:], uint32(len(c.payload)))
	w.Write(buf[:])
	_, err := w.Write(c.payload)
	if err != nil {
		return 0, err
	}
	return int64(5 + len(c.payload)), nil
}

func CmdReadyCount(count int32) *Command {
	var payload = make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(count))

	return &Command{
		apiKey:  c2b.KeyReadyCount,
		payload: payload,
	}
}

func CmdFinish(msgID core.MessageID) *Command {
	var payload = make([]byte, core.MessageIDLength)
	copy(payload, msgID[:])

	return &Command{
		apiKey:  c2b.KeyACK,
		payload: payload,
	}
}

func CmdRequeue(msgID core.MessageID) *Command {
	var payload = make([]byte, core.MessageIDLength)
	copy(payload, msgID[:])

	return &Command{
		apiKey:  c2b.KeyRequeue,
		payload: payload,
	}
}

func CmdSubscirbe(topicName, groupName string) *Command {
	var payload = c2b.PayloadSubscribe{
		TopicName: topicName,
		GroupName: groupName,
	}

	return &Command{
		apiKey:  c2b.KeySubscribe,
		payload: payload.Encode(),
	}
}

func CmdStartClose() *Command {
	return &Command{
		apiKey:  c2b.KeyClose,
		payload: nil,
	}
}

func CmdPublish(topicName string, msgBody []byte) *Command {
	var payload = c2b.PayloadPublishMessage{
		TopicName:   topicName,
		MessageBody: msgBody,
	}
	return &Command{
		apiKey:  c2b.KeyPublish,
		payload: payload.Encode(),
	}
}

func CmdPublishBatch(topicName string, msgs [][]byte) *Command {
	var payload = c2b.PayloadPublishMulti{
		TopicName: topicName,
		Messages:  msgs,
	}

	return &Command{
		apiKey:  c2b.KeyPublishMulti,
		payload: payload.Encode(),
	}
}
