package cli

import (
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/khoakmp/smq/broker/core"
)

type ClientOptions struct {
	RecvWindow            int32
	FlushMessagesInterval time.Duration
	SendMessageTimeout    time.Duration
}

func defaultClientOptions() ClientOptions {
	return ClientOptions{
		RecvWindow:            1000,
		FlushMessagesInterval: time.Millisecond * 100,
		SendMessageTimeout:    time.Second * 2,
	}
}

type Consumer struct {
	*Client
	group            *core.ConsumerGroup
	InflightMsgCount int32
	ReadyCount       int32
}

func (c *Consumer) Str() string {
	return c.TraceID
}

func (c *Consumer) SendMessage() {
	atomic.AddInt32(&c.InflightMsgCount, 1)
	c.TriggerUpdateState()
}

func (c *Consumer) MessageResponse() {
	atomic.AddInt32(&c.InflightMsgCount, -1)
	c.TriggerUpdateState()
}

// this function is called in only one goroutine
func (c *Consumer) IsReady() bool {
	if atomic.LoadInt32(&c.InflightMsgCount) >= atomic.LoadInt32(&c.ReadyCount) || c.group.Pausing() {
		// TODO: replace ClientID by TraceID sent by client
		//fmt.Printf("Client %s is not ready to recv msg\n", c.TraceID)
		return false
	}
	return true
}

func HandleACK(consumer *Consumer, msgID core.MessageID) error {
	consumer.MessageResponse()
	consumer.group.FinishMessage(msgID, consumer.ClientID)

	//fmt.Printf("[Client %s] recv finish msg\n", consumer.Str())
	return nil
}

func HandleReadyCount(consumer *Consumer, count int32) error {
	if consumer.state == StateCloseWait {
		return errors.New("close wait")
	}

	atomic.StoreInt32(&consumer.ReadyCount, count)
	consumer.TriggerUpdateState()
	return nil
}

func HandleRequeue(consumer *Consumer, msgID core.MessageID) error {
	err := consumer.group.RequeueMessage(msgID, consumer.ClientID)
	consumer.MessageResponse()
	return err
}

func HandleClose(consumer *Consumer) error {
	atomic.StoreInt32(&consumer.ReadyCount, 0)
	consumer.state = StateCloseWait
	log.Printf("[Client %s] change to CLOSE state\n", consumer.Str())
	return nil
}
func pushMessagesLoop(s Server, consumer *Consumer) {
	log.Printf("%c START Push Messages Loop to Client[%s]\n", '\U0001F680', consumer.TraceID)
	/* fmt.Println("starting push msg loop to consumer:", consumer.ID())*/
	fmt.Println("flush msg interval:", consumer.opts.FlushMessagesInterval)
	flushTicker := time.NewTicker(consumer.opts.FlushMessagesInterval)
	var flushChan <-chan time.Time
	var group *core.ConsumerGroup = consumer.group

	var messageChan <-chan *core.Message = nil
	var persistentChan <-chan []byte = nil

	var flushed bool = true

	for {
		if !consumer.IsReady() {
			// when group != nil => client.group !=nil => client.ReadyRecvMsg valid
			messageChan = nil
			persistentChan = nil
			flushChan = nil

			if !flushed {
				consumer.writeLock.Lock()
				consumer.writer.Flush()
				consumer.writeLock.Unlock()
				flushed = true
			}
		} else {
			messageChan = group.MessageChan()
			persistentChan = group.PersistentQueue().ReadChan()
			if flushed {
				// there are no buffered messages in write buffer
				flushChan = nil
			} else {
				flushChan = flushTicker.C
			}
		}
		select {
		case <-flushChan:
			consumer.writeLock.Lock()
			consumer.writer.Flush()
			consumer.writeLock.Unlock()
			flushed = true

		case <-consumer.updateStateChan:
			continue
		case msg := <-messageChan:
			err := s.SendMessage(consumer.Client, msg)
			if err != nil {
				// TODO:
			}

			flushed = false
			consumer.SendMessage()
			//msg.ConsumerID = consumer.ClientID

			group.StartInflight(msg, consumer.ClientID, consumer.opts.SendMessageTimeout)

		case msgBuf := <-persistentChan:
			msg := new(core.Message)
			msg.Decode(msgBuf)
			err := s.SendMessage(consumer.Client, msg)
			flushed = false
			if err != nil {
				// TODO:
			}
			consumer.SendMessage()
			//msg.ConsumerID = consumer.ClientID
			group.StartInflight(msg, consumer.ClientID, consumer.opts.SendMessageTimeout)
		case <-consumer.closeChan:
			goto exit
		}
	}
exit:
	flushTicker.Stop()
	log.Printf("[Client %s] Stop pushing message to client\n", consumer.TraceID)
}
