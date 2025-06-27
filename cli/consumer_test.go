package cli

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/khoakmp/smq/api"
	"github.com/khoakmp/smq/api/c2b"
	"github.com/khoakmp/smq/broker"
	"github.com/khoakmp/smq/monitor"
	"github.com/stretchr/testify/assert"
)

type MockMessageHandler struct {
	cnt int
}

func (m *MockMessageHandler) Handle(msg *Message) error {
	fmt.Println("handle msg:", string(msg.Payload))
	m.cnt++

	return nil
}
func publishMessageLoop(brokerTCPAddr string, topicName string) {
	conn, err := net.Dial("tcp", brokerTCPAddr)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("hey")
	buffer := bytes.NewBuffer(nil)
	reader := bufio.NewReader(conn)

	readResp := func() (Type byte, payload []byte, err error) {
		var header [5]byte
		_, err = reader.Read(header[:])
		if err != nil {
			return 0, nil, err
		}
		Type = header[0]
		var sz uint32 = binary.BigEndian.Uint32(header[1:])
		payload = make([]byte, sz)
		_, err = io.ReadFull(reader, payload)
		return
	}

	sendMsg := func() error {
		buffer.Reset()
		buffer.WriteByte(c2b.KeyPublish)
		payload := c2b.PayloadPublishMessage{
			TopicName:   topicName,
			MessageBody: []byte("helloo ok "),
		}
		buf := payload.Encode()
		binary.Write(buffer, binary.BigEndian, uint32(len(buf)))
		buffer.Write(buf)
		_, err := conn.Write(buffer.Bytes())
		return err
	}
	sendMsg()

	readResp()

}

func TestConsumer(t *testing.T) {
	monitor.Run(":5050", ":8080")

	broker := broker.New("SMQ-Broker-1", []string{":5050"}, []string{"localhost:8080"})
	go broker.RunTCP(":5000")
	msgHandler := &MockMessageHandler{}
	topicName := "topic1#tmp"
	csm := NewConsumer(topicName, "group1#tmp", msgHandler, &ConsumerConfig{
		ConcurrentHandler:  1,
		BackOffTimeUnit:    time.Second,
		MaxBackoffCounter:  5,
		DefaultMaxInflight: 1000,
	})
	time.Sleep(time.Millisecond)

	err := csm.ConnectBroker(":5000")
	assert.Equal(t, nil, err)
	//publishMessageLoop(":5000", topicName)
	conn, err := net.Dial("tcp", ":5000")
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("hey")
	buffer := bytes.NewBuffer(nil)
	reader := bufio.NewReader(conn)

	readResp := func() (Type byte, payload []byte, err error) {
		var header [5]byte
		_, err = reader.Read(header[:])
		if err != nil {
			return 0, nil, err
		}
		Type = header[0]
		var sz uint32 = binary.BigEndian.Uint32(header[1:])
		payload = make([]byte, sz)
		_, err = io.ReadFull(reader, payload)
		return
	}

	sendMsg := func() error {
		buffer.Reset()
		buffer.WriteByte(c2b.KeyPublish)
		payload := c2b.PayloadPublishMessage{
			TopicName:   topicName,
			MessageBody: []byte("helloo ok "),
		}
		buf := payload.Encode()
		binary.Write(buffer, binary.BigEndian, uint32(len(buf)))
		buffer.Write(buf)
		_, err := conn.Write(buffer.Bytes())
		return err
	}

	sendMsg()

	frameType, payload, err := readResp()
	assert.Equal(t, nil, err)
	assert.Equal(t, api.FrameResponse, frameType)
	assert.Equal(t, []byte("OK"), payload)
	time.Sleep(time.Second)
	assert.Equal(t, 1, msgHandler.cnt)
	//time.Sleep(time.Second)
}

func TestInteract(t *testing.T) {
	r := bufio.NewReader(os.Stdin)
	l, _, err := r.ReadLine()
	fmt.Println("s", l)
	fmt.Print(err)
}
