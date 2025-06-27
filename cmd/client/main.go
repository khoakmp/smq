package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/khoakmp/smq/api/c2b"
	"github.com/khoakmp/smq/utils"
)

type Client struct {
	netConn net.Conn
	reader  *bufio.Reader
	wbuf    *bytes.Buffer
	TraceID string
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
func RunClient() {
	brokerTCPAddr := os.Args[1]
	clientID := utils.RandString(6)
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
			client.netConn.Close()
			return
		}
	}
}

func main() {
	RunClient()
}
