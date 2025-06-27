package cli

import (
	"bufio"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khoakmp/smq/api/c2b"
	"github.com/khoakmp/smq/broker/core"
)

const (
	StateInit int32 = iota
	StateSpecified
	StateSubscribed
	StateCloseWait
)

type Client struct {
	ClientID core.ClientID
	state    int32
	netConn  net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	// multi clientID can have same TraceID correspond to one specified consumer
	writeLock       sync.Mutex
	updateStateChan chan struct{}
	closeChan       chan struct{}
	opts            ClientOptions
	TraceID         string //  sent by client in specification payload
	startWriteLoop  int32
}

func MayStartWriteLoop(c *Client) {
	if !atomic.CompareAndSwapInt32(&c.startWriteLoop, 0, 1) {
		return
	}

	go func() {

		ticker := time.NewTicker(time.Millisecond * 100)
		for {
			select {
			case <-c.closeChan:
				goto exit
			case <-ticker.C:
				c.writeLock.Lock()
				c.writer.Flush()
				c.writeLock.Unlock()
			}
		}
	exit:
		ticker.Stop()
		log.Printf("[Client %s] Stop Write Loop\n", c.TraceID)
	}()
}

// at that time, it must keep large amount of message dung?

var idCounter int64 = 0

func genClientID() core.ClientID {
	return core.ClientID(atomic.AddInt64(&idCounter, 1))
}

func NewClient(conn net.Conn) *Client {
	client := &Client{
		netConn:         conn,
		reader:          bufio.NewReaderSize(conn, 4096),
		writer:          bufio.NewWriterSize(conn, 2048),
		ClientID:        genClientID(),
		closeChan:       make(chan struct{}),
		updateStateChan: make(chan struct{}, 1),
		opts:            defaultClientOptions(),
	}

	client.TraceID = strconv.Itoa(int(client.ClientID))
	return client
}

func (c *Client) Close() {
	c.netConn.Close()
}

func (c *Client) ID() core.ClientID {
	return c.ClientID
}

func (c *Client) TriggerUpdateState() {
	select {
	case c.updateStateChan <- struct{}{}:
	default:
	}
}

func HandleSpec(client *Client, spec *c2b.PayloadClientSpec, broker *core.BrokerBase) {
	client.opts.FlushMessagesInterval = time.Duration(spec.PushMessagesInterval)
	client.opts.RecvWindow = int32(spec.RecvWindow)
	client.TraceID = spec.TraceID
	atomic.StoreInt32(&client.state, StateSpecified)
}

func HandleSubscribe(server Server, client *Client, state *core.BrokerBase, topicName, groupName string) (*Consumer, error) {
	topic := state.FindTopic(topicName)
	group, err := topic.FindConsumerGroup(groupName)
	if err != nil {
		return nil, err
	}
	group.AddMember(client)

	consumer := &Consumer{
		group:  group,
		Client: client,
	}

	consumer.ReadyCount = int32(consumer.opts.RecvWindow)
	consumer.state = StateSubscribed

	go pushMessagesLoop(server, consumer)
	// observation: to serve one client, it always handles commands sequentially
	log.Printf("[Client %s] Subscribes Topic %s, Group: %s\n", client.TraceID, topicName, groupName)

	return consumer, nil
}
