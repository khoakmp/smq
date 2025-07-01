package cli

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/khoakmp/smq/api"
	"github.com/khoakmp/smq/api/c2b"
	"github.com/khoakmp/smq/broker/core"
	"github.com/stretchr/testify/assert"
)

func TestProtocol(t *testing.T) {
	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println(err)
		return
	}
	notifyChan := make(chan any)
	go func() {
		for {
			v := <-notifyChan
			switch v := v.(type) {
			case *core.Topic:
				if v.Exiting() {
					fmt.Printf("[Event] Topic %s is deleted\n", v.Name)
				} else {
					fmt.Printf("[Event] Topic %s is creating\n", v.Name)
				}
			case *core.ConsumerGroup:
				if v.Exiting() {
					fmt.Printf("[Event] ConsumerGroup %s is deleted\n", v.Name)
				} else {
					fmt.Printf("[Event] ConsumerGroup %s is creating\n", v.Name)
				}
			}
		}
	}()

	b := core.CreateBrokerBaseForTest(notifyChan)
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Println(err)
				continue
			}
			client := NewClient(conn)
			p := ServeV1{}
			go p.IOLoop(client, b)
		}
	}()
	topicName := "topic-1#tmp"
	conn, err := net.Dial("tcp", ":8080")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("connect successfully")
	var joinGroupReq = c2b.PayloadSubscribe{
		TopicName: topicName,
		GroupName: "group-1#tmp",
	}

	go func() {
		reader := bufio.NewReader(conn)
		var headerBuf [5]byte
		for {
			_, err := io.ReadFull(reader, headerBuf[:])
			if err != nil {
				fmt.Println(err)
				return
			}
			sz := binary.BigEndian.Uint32(headerBuf[1:])
			payload := make([]byte, sz)
			_, err = io.ReadFull(reader, payload)
			if err != nil {
				fmt.Println(err)
				continue
			}
			switch headerBuf[0] {
			case api.FrameResponse:
				fmt.Println("recv:", string(payload))
			case api.FrameMessage:
				var msg core.Message
				msg.Decode(payload)
				fmt.Println("recv msg:", msg.Str())
			}
		}
	}()

	writer := bufio.NewWriter(conn)
	writer.WriteByte(c2b.KeySubscribe)
	joinGroupReq.WriteWithSizeTo(writer)
	writer.Flush()
	time.Sleep(time.Millisecond)
	//topic := state.FindTopic(topicName)
	metadata := b.GetMetadata(true)
	fmt.Println(metadata.Str())
	topic := b.FindTopic(topicName)
	msg := core.MakeMessage(topic.NextMessageID(), []byte("payload here"))
	topic.PutMessage(msg)
	time.Sleep(time.Second)

	writer.WriteByte(c2b.KeyACK)
	writer.Write(msg.ID[:])
	writer.Flush()
	time.Sleep(time.Millisecond)

	writer.WriteByte(c2b.KeyPublish)
	msgBody := []byte("hey")
	var bufSize [4]byte

	binary.BigEndian.PutUint32(bufSize[:], uint32(len(topicName)+1+len(msgBody)))
	writer.Write(bufSize[:])
	writer.Write([]byte(topicName + " "))
	writer.Write(msgBody)
	writer.Flush()
	time.Sleep(time.Second)
	/*
		 	conn.Close() // when this close => it triggers multi operations

			time.Sleep(time.Millisecond)
			metadata = state.CalcMetadata(true)
			fmt.Println(metadata.Str())
	*/
}

func TestPublishMessage(t *testing.T) {
	notifyChan := make(chan any)
	go func() {
		for {
			<-notifyChan
		}
	}()
	b := core.CreateBrokerBaseForTest(notifyChan)

	readyChan := make(chan struct{})

	go func() {
		l, err := net.Listen("tcp", ":5001")
		if err != nil {
			panic(err)
		}

		readyChan <- struct{}{}
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Println(err)
				return
			}
			client := NewClient(conn)
			p := ServeV1{}
			p.IOLoop(client, b)
		}
	}()
	<-readyChan

	conn, err := net.Dial("tcp", ":5001")
	if err != nil {
		fmt.Println(err)
		return
	}

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
			TopicName:   "topic1#tmp",
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
}

func TestPublisBatch(t *testing.T) {
	state := core.NewBrokerBase("SMQB-0", nil, nil)

	var msgPattern = []byte("kmp")
	var messages [][]byte = make([][]byte, 3)

	for i := range len(messages) {
		messages[i] = make([]byte, 3)
		copy(messages[i], msgPattern)
	}

	err := HandlePublishMulti(nil, state, "t1", messages)
	assert.Equal(t, nil, err)
	time.Sleep(time.Millisecond)
	state.Close()
}
