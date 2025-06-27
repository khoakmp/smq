package cli

import (
	"encoding/binary"
	"errors"
	"io"
	"log"

	"github.com/khoakmp/smq/api"
	"github.com/khoakmp/smq/api/c2b"
	"github.com/khoakmp/smq/broker/bfpool"
	"github.com/khoakmp/smq/broker/core"
)

var OK []byte = []byte("OK")

type ServeV1 struct{}

type handleContext struct {
	consumer *Consumer
	client   *Client
	state    *core.BrokerBase
}

func (p ServeV1) readRequest(client *Client) (apiKey byte, payload []byte, err error) {
	var buf [5]byte

	_, err = io.ReadFull(client.reader, buf[:])
	if err != nil {
		return
	}
	apiKey = buf[0]
	var sz uint32 = binary.BigEndian.Uint32(buf[1:])
	payload = bfpool.GetByteSlice(sz)

	//payload = make([]byte, sz)
	_, err = io.ReadFull(client.reader, payload)

	return
}

func (p ServeV1) readRequestHeader(client *Client) (cmdType byte, payloadSize uint32, err error) {
	var buf [5]byte
	_, err = io.ReadFull(client.reader, buf[:])
	if err != nil {
		return
	}
	cmdType = buf[0]
	payloadSize = binary.BigEndian.Uint32(buf[1:])
	return
}

func (p ServeV1) IOLoop(client *Client, state *core.BrokerBase) {
	var ctx handleContext = handleContext{
		client: client,
		state:  state,
	}
	// TODO: create write loop here -> used for all cases

	for {
		apiKey, payload, err := p.readRequest(client)

		if err != nil {
			// with all read error-> always stop
			log.Printf("[Client %s] read failed, stop IOLoop, err: %s\n", client.TraceID, err.Error())
			break
		}

		resp, err := p.route(&ctx, apiKey, payload)
		bfpool.PutByteSlice(payload)

		if err != nil {
			p.sendFrame(client, api.FrameError, []byte(err.Error()), true)
			continue
		}
		// problem of releasing message in inflight queue

		if resp != nil {
			flush := true
			if apiKey == c2b.KeyPublish || apiKey == c2b.KeyPublishMulti {
				flush = false
			}
			p.sendFrame(client, api.FrameResponse, resp, flush)
		}

		//bfpool.PutByteSlice(payload)
	}

	close(client.closeChan) // stop the pushing messages loop
	if ctx.consumer != nil {
		ctx.consumer.group.DeleteConsumer(client.ClientID)
	}
}

func (p ServeV1) route(ctx *handleContext, apiKey byte, payloadBuf []byte) ([]byte, error) {
	switch apiKey {
	// call it as Handle(cmd  *Command)
	case c2b.KeySpec:
		var payload c2b.PayloadClientSpec
		payload.Decode(payloadBuf)
		HandleSpec(ctx.client, &payload, ctx.state)
		// TODO: change later
		return OK, nil

	case c2b.KeySubscribe:
		// in client life time, it only send subscribtion only one time ?
		var payload c2b.PayloadSubscribe
		err := payload.Decode(payloadBuf)
		if err != nil {
			return nil, err
		}

		consumer, err := HandleSubscribe(p, ctx.client, ctx.state, payload.TopicName, payload.GroupName)
		if err != nil {
			return nil, err
		}
		ctx.consumer = consumer
		return OK, nil

	case c2b.KeyACK:
		if len(payloadBuf) != core.MessageIDLength {
			return nil, errors.New("wrong message id format")
		}
		var msgID core.MessageID = core.MessageID(payloadBuf)
		err := HandleACK(ctx.consumer, msgID)
		return nil, err

	case c2b.KeyReadyCount:
		count := binary.BigEndian.Uint32(payloadBuf)
		//fmt.Printf("[Broker] recv readycount %d\n", count)
		err := HandleReadyCount(ctx.consumer, int32(count))
		return nil, err

	case c2b.KeyRequeue:
		var msgID core.MessageID
		copy(msgID[:], payloadBuf)
		err := HandleRequeue(ctx.consumer, msgID)
		return nil, err

	case c2b.KeyClose:
		// should send OK back to trigger flush all
		// at this point, payloadSize = 0, so payloadBuf is nil
		return []byte("CLOSE_ACK"), HandleClose(ctx.consumer)

	case c2b.KeyPublish:
		var payload c2b.PayloadPublishMessage
		// only topicName + msg []byte
		// when one message is published to broker, it need to keep this message (and may create multi copies)
		// until it is completely pushed to consumer, so we don't know when this event occur
		// so can't reuse the msg body []byte now
		err := payload.Decode(payloadBuf)
		if err != nil {
			return nil, err
		}
		msgBody := bfpool.GetByteSlice(uint32(len(payload.MessageBody)))
		copy(msgBody, payload.MessageBody)
		payload.MessageBody = msgBody

		err = HandlePublish(ctx.client, ctx.state, payload.TopicName, payload.MessageBody)
		if err != nil {
			return nil, err
		}

		MayStartWriteLoop(ctx.client)
		return OK, err

	}
	return nil, errors.New("not support request type")
}

func (p ServeV1) sendFrame(client *Client, frameType uint8, data []byte, shouldFlush bool) {
	var bufSize [4]byte

	binary.BigEndian.PutUint32(bufSize[:], uint32(len(data)))
	client.writeLock.Lock()
	client.writer.WriteByte(frameType)
	client.writer.Write(bufSize[:])
	client.writer.Write(data)

	if shouldFlush {
		client.writer.Flush()
	}
	client.writeLock.Unlock()
}

func (p ServeV1) SendMessage(client *Client, msg *core.Message) error {
	// TODO: add buffer pool to reuse

	//buffer := bytes.NewBuffer(nil)
	buffer := bfpool.GetBuffer()
	buffer.Grow(msg.LenEncode())

	_, err := msg.WriteTo(buffer)
	if err != nil {
		return err
	}

	p.sendFrame(client, api.FrameMessage, buffer.Bytes(), false)
	bfpool.PutBuffer(buffer)
	return nil
}
