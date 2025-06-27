package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/khoakmp/smq/api"
	"github.com/khoakmp/smq/api/c2b"
	"github.com/khoakmp/smq/broker"
	"github.com/khoakmp/smq/cli"
	"github.com/khoakmp/smq/monitor"
)

const (
	MsgTypeGood = "g"
	MsgTypeBad  = "b"
)

type Client struct {
	netConn net.Conn
	reader  *bufio.Reader
	wbuf    *bytes.Buffer
	TraceID string
}

func (c *Client) readResponse() (frameType byte, payload []byte, err error) {
	var hbuf [5]byte
	_, err = c.reader.Read(hbuf[:])
	if err != nil {
		c.netConn.Close()
		return
	}
	frameType = hbuf[0]
	var sz uint32 = binary.BigEndian.Uint32(hbuf[1:])
	payload = make([]byte, sz)
	io.ReadFull(c.reader, payload)
	return
}

func (c *Client) ReadLoop() {
	for {
		frameType, payload, err := c.readResponse()
		if err != nil {
			fmt.Printf("[%s]failed to read resp %s\n", c.TraceID, err)
			return
		}
		if frameType == api.FrameError {
			fmt.Printf("[%s] Recv FrameError %s\n", c.TraceID, string(payload))
		}

		/* if frameType == api.FrameResponse {
			fmt.Println("Frame Response:", string(payload))
		} */

	}
}

func (c *Client) PublishMessage(topic string, msgbody []byte) {
	c.wbuf.Reset()
	c.wbuf.Grow(len(msgbody) + 1 + len(topic))

	c.wbuf.WriteByte(c2b.KeyPublish)
	binary.Write(c.wbuf, binary.BigEndian, uint32(len(msgbody)+1+len(topic)))
	c.wbuf.WriteString(topic)
	c.wbuf.WriteByte(' ')
	c.wbuf.Write(msgbody)

	c.netConn.Write(c.wbuf.Bytes())
}

func (c *Client) PublishMutli(topic string, n int, msgBody []byte) {
	for range n {
		c.PublishMessage(topic, msgBody)
	}
}

func ConnectBroker(tcpAddr string, ClientID string) (*Client, error) {
	conn, err := net.Dial("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}
	return &Client{
		TraceID: ClientID,
		netConn: conn,
		reader:  bufio.NewReader(conn),
		wbuf:    bytes.NewBuffer(nil),
	}, nil
}

type MockMessageHandler struct {
	cnt     int
	TraceID string
}

var ErrBadMessage = errors.New("bad message")

func (m *MockMessageHandler) Handle(msg *cli.Message) error {
	m.cnt++
	id, err := uuid.FromBytes(msg.ID[:])
	if err != nil {
		fmt.Printf("[%s] handle msg FAILED, wrong format ID\n", m.TraceID)
		return ErrBadMessage
	}
	result := "FAILED"

	if bytes.Equal(msg.Payload, []byte("g")) {
		result = "SUCCESS"
	}

	fmt.Printf("[%s] handle msg {ID:%s, payload: %s}: %s\n", m.TraceID, id.String(), msg.Payload, result)
	if result == "FAILED" {
		return ErrBadMessage
	}

	return nil
}

func TestConsumerInteractive() {
	monitor.Run(":5050", ":8080")

	broker1 := broker.New("SMQ-Broker#1", []string{":5050"}, []string{"localhost:8080"})
	time.Sleep(time.Millisecond)

	go broker1.RunTCP(":5000")
	time.Sleep(time.Millisecond)

	client, err := ConnectBroker(":5000", "CLI-1")
	if err != nil {
		fmt.Println("failed to connect broker:", err)
		return
	}

	go client.ReadLoop()
	msgHandler := &MockMessageHandler{}
	topicName := "topic1#tmp"
	groupName := "group1#tmp"
	// FailedMsgCountOpenCB dung

	csm := cli.NewConsumerDebug(topicName, groupName, msgHandler, &cli.ConsumerConfig{
		ConcurrentHandler:         1,
		BackOffTimeUnit:           time.Second,
		MaxBackoffCounter:         5,
		DefaultMaxInflight:        1000,
		FailedMsgCountToOpenCB:    3,
		SucceeddMsgCountToCloseCB: 3,
	}, "CSM-1")

	csm.SetMaxAttemp(1)

	msgHandler.TraceID = "CSM-1"

	err = csm.ConnectBroker(":5000")

	if err != nil {
		fmt.Printf("[%s] failed to connect broker at port 5000\n", csm.TraceID)
	} else {
		fmt.Printf("[%s] connect broker at port 5000 successfully\n", csm.TraceID)
	}

	input := bufio.NewReader(os.Stdin)

	for {
		line, _, err := input.ReadLine()
		if err != nil {
			fmt.Println(err)
			return
		}

		arr := bytes.Split(line, []byte(" "))
		switch {

		case bytes.Equal(arr[0], []byte("pub")):
			if len(arr) != 2 {
				fmt.Println("WRONG format input")
				continue
			}
			client.PublishMessage(topicName, arr[1])

		case bytes.Equal(arr[0], []byte("pubm")):
			if len(arr) < 3 {
				fmt.Println("WRONG format input")
				continue
			}
			n, err := strconv.Atoi(string(arr[1]))
			if err != nil {
				fmt.Println("WRONG format")
				continue
			}

			msgBody := arr[2]
			client.PublishMutli(topicName, n, msgBody)
		case bytes.Equal(arr[0], []byte("stop-csm")):
			csm.Stop()

		case bytes.Equal(arr[0], []byte("q")):
			return
		}
	}
}

var monitorTCPAddr string = ":5050"
var monitorHTTPAddr string = "localhost:8080"

var defaultConsumerConfig = cli.ConsumerConfig{
	ConcurrentHandler:         1,
	BackOffTimeUnit:           time.Second,
	MaxBackoffCounter:         5,
	DefaultMaxInflight:        1000,
	FailedMsgCountToOpenCB:    3,
	SucceeddMsgCountToCloseCB: 3,
}

func RunMonitor() {
	monitor.Run(monitorTCPAddr, monitorHTTPAddr)
	input := bufio.NewReader(os.Stdin)

	for {
		input.ReadLine()
	}
}

func RunBroker() {

	tcpAddr := os.Args[1]
	id := os.Args[2]
	broker := broker.New(id, []string{monitorTCPAddr}, []string{monitorHTTPAddr})
	go broker.RunTCP(tcpAddr)
	input := bufio.NewReader(os.Stdin)

	for {
		line, _, err := input.ReadLine()
		if err != nil {
			fmt.Println(err)
			return
		}

		arr := bytes.Split(line, []byte(" "))
		switch {
		case bytes.Equal(arr[0], []byte("q")):
			broker.Stop()
			return
			// TODO: add queries
		}
	}
}

var Letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandString returns a random string of length n using characters a-z and A-Z
func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = Letters[rand.Intn(len(Letters))]
	}
	return string(b)
}

func RunConsumer() {
	topicName, groupName := os.Args[1], os.Args[2]
	msgHandler := &MockMessageHandler{}
	traceID := RandString(6)
	csm := cli.NewConsumerDebug(topicName, groupName, msgHandler, &defaultConsumerConfig, traceID)
	csm.SetMaxAttemp(1)
	input := bufio.NewReader(os.Stdin)

	for {
		line, _, err := input.ReadLine()
		if err != nil {
			fmt.Println(err)
			return
		}
		arr := bytes.Split(line, []byte(" "))
		switch {
		case bytes.Equal(arr[0], []byte("q")):
			csm.Stop()
			return
			// TODO: add queries
		}
	}
}

func RunClient() {
	brokerTCPAddr := os.Args[1]
	clientID := RandString(6)
	client, err := ConnectBroker(brokerTCPAddr, clientID)
	if err != nil {
		fmt.Println(err)
		return
	}
	input := bufio.NewReader(os.Stdin)
	for {
		line, _, err := input.ReadLine()
		if err != nil {
			fmt.Println(err)
			return
		}

		arr := bytes.Split(line, []byte(" "))
		switch {

		case bytes.Equal(arr[0], []byte("pub")):
			if len(arr) != 3 {
				fmt.Println("WRONG format input")
				continue
			}
			client.PublishMessage(string(arr[1]), arr[2])

		case bytes.Equal(arr[0], []byte("pubm")):
			if len(arr) < 4 {
				fmt.Println("WRONG format input")
				continue
			}
			topicName := string(arr[1])
			n, err := strconv.Atoi(string(arr[2]))
			if err != nil {
				fmt.Println("WRONG format")
				continue
			}

			msgBody := arr[3]
			client.PublishMutli(topicName, n, msgBody)

		case bytes.Equal(arr[0], []byte("q")):
			return
		}
	}
}
func main() {
	//TestConsumerInteractive()

}
