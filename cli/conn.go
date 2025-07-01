package cli

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khoakmp/smq/api"
	"github.com/khoakmp/smq/api/c2b"
	"github.com/khoakmp/smq/broker/core"
)

type reader interface {
	Read([]byte) (int, error)
}

type writer interface {
	Write([]byte) (int, error)
	Flush() error
}

type ConnDelegate interface {
	OnMessage(msg *Message)
	OnResponse(resp []byte, c *Conn)
	OnError(err []byte, c *Conn)
	OnHeartbeat(c *Conn)
	OnConnClose(c *Conn)
	OnIOError(err error, c *Conn)
}

type Conn struct {
	netConn          *net.TCPConn
	addr             string
	r                reader
	w                writer
	msgHandlingCount int32
	writeMutex       sync.Mutex
	msgRespChan      chan *Message // non-buffered channel
	RdyCount         int32
	closeFlag        int32
	Delegate         ConnDelegate
	exitChan         chan struct{}
	readExitFlag     int32
	readDoneChan     chan struct{}
	wg               sync.WaitGroup
	lastRecvMsgTime  int64
	flushInterval    time.Duration
	flushed          int32

	MsgRespBatchSz []int // for debugging
}

func (c *Conn) MsgRespBatchSzStats() string {
	s, bmax, bmin := 0, 0, 10000
	buffer := bytes.NewBuffer(nil)

	for i := 0; i < len(c.MsgRespBatchSz); i++ {
		if c.MsgRespBatchSz[i] > bmax {
			bmax = c.MsgRespBatchSz[i]
		}
		if c.MsgRespBatchSz[i] < bmin {
			bmin = c.MsgRespBatchSz[i]
		}
		s += c.MsgRespBatchSz[i]
	}
	fmt.Fprintf(buffer, "Num batch: %d\n", len(c.MsgRespBatchSz))

	if len(c.MsgRespBatchSz) > 0 {
		fmt.Fprintf(buffer, "Max batch size:%d\n", bmax)
		fmt.Fprintf(buffer, "Min batch size:%d\n", bmin)
		fmt.Fprintf(buffer, "Ave batch size: %d\n", s/len(c.MsgRespBatchSz))
	}
	for i := 0; i < len(c.MsgRespBatchSz); i++ {
		fmt.Fprintf(buffer, "%d,", c.MsgRespBatchSz[i])
	}
	fmt.Fprintln(buffer)

	return buffer.String()
}
func NewConn(addr string, delegate ConnDelegate, flushInterval time.Duration) *Conn {
	return &Conn{
		netConn:          nil,
		addr:             addr,
		r:                nil,
		w:                nil,
		msgHandlingCount: 0,
		writeMutex:       sync.Mutex{},
		msgRespChan:      make(chan *Message),
		closeFlag:        0,
		exitChan:         make(chan struct{}),
		readExitFlag:     0,
		readDoneChan:     make(chan struct{}),
		wg:               sync.WaitGroup{},
		RdyCount:         0,
		Delegate:         delegate,
		flushInterval:    flushInterval,
	}
}

func (c *Conn) ReadyCount() int32 {
	return atomic.LoadInt32(&c.RdyCount)
}

func (c *Conn) Connect(rbufSize, wbufSize int, spec c2b.PayloadClientSpec) error {
	///dialer := net.Dialer{}
	// TODO: create custom dialer
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return err
	}
	c.netConn = conn.(*net.TCPConn)
	// TODO: send

	buffer := bytes.NewBuffer(nil)
	buffer.WriteByte(c2b.KeySpec)
	buf := spec.Encode()
	binary.Write(buffer, binary.BigEndian, uint32(len(buf)))
	buffer.Write(buf)

	_, err = conn.Write(buffer.Bytes())
	if err != nil {
		return err
	}
	frameType, framePayload, err := readFrame(conn)
	if err != nil {
		return err
	}
	if frameType == api.FrameError {
		conn.Close()
		return errors.New(string(framePayload))
	}

	// TODO: frameType is response, handle this response, set up some fields in this conn
	// setup r/w for conn
	c.r = bufio.NewReaderSize(c.netConn, rbufSize) // default buffer size: 4096
	c.w = bufio.NewWriterSize(c.netConn, wbufSize)

	c.wg.Add(2)

	go c.readLoop()
	go c.writeLoop()

	//log.Printf("[Conn -> %s] is connected\n", c.addr)

	return nil
}

func (c *Conn) Str() string {
	return c.addr
}

func (c *Conn) WriteCommand(cmd *Command) (err error) {
	c.writeMutex.Lock()
	_, err = cmd.WriteTo(c.w)
	if err != nil {
		c.writeMutex.Unlock()
		c.Delegate.OnIOError(err, c)
		return
	}
	err = c.w.Flush()
	atomic.StoreInt32(&c.flushed, 1)
	c.writeMutex.Unlock()

	if err != nil {
		c.Delegate.OnIOError(err, c)
	}
	return
}

func (c *Conn) BufferCommand(cmd *Command) (err error) {
	c.writeMutex.Lock()
	_, err = cmd.WriteTo(c.w)
	atomic.StoreInt32(&c.flushed, 0)
	c.writeMutex.Unlock()
	return
}

func (c *Conn) Flush() (err error) {
	if atomic.CompareAndSwapInt32(&c.flushed, 0, 1) {
		c.writeMutex.Lock()
		err = c.w.Flush()
		c.writeMutex.Unlock()
		if err != nil {
			c.Delegate.OnIOError(err, c)
		}
	}
	/* 	c.writeMutex.Lock()
	   	if atomic.CompareAndSwapInt32(&c.flushed, 0, 1) {
	   		err = c.w.Flush()
	   	}
	   	c.writeMutex.Unlock()
	   	if err != nil {
	   		c.Delegate.OnIOError(err, c)
	   	} */
	return
}

func readFrame(r reader) (frameType byte, framePayload []byte, err error) {
	var buf [5]byte
	_, err = io.ReadFull(r, buf[:])
	if err != nil {
		return
	}
	frameType = buf[0]
	sz := binary.BigEndian.Uint32(buf[1:])
	framePayload = bspool.Get(sz)

	//framePayload = make([]byte, sz)
	_, err = io.ReadFull(r, framePayload)
	return
}

func (c *Conn) readLoop() {
	for {
		if atomic.LoadInt32(&c.closeFlag) == 1 {
			goto exit
		}

		frameType, framePayload, err := readFrame(c.r)
		if err != nil {

			goto exit
		}

		switch frameType {
		case api.FrameMessage:
			// TODO: handle error
			atomic.AddInt32(&c.msgHandlingCount, 1)
			atomic.StoreInt64(&c.lastRecvMsgTime, time.Now().UnixNano())

			msg := decodeMessage(framePayload)
			msg.conn = c
			c.Delegate.OnMessage(msg)
			// when readloop done, it ensures that msgInflightCount no change more

		case api.FrameResponse:
			c.Delegate.OnResponse(framePayload, c)
			// at this point, if c is consumer conn, if it recv close_ack
			// it trigger set closeFlag => trigger to out readloop

		case api.FrameError:
			c.Delegate.OnError(framePayload, c)
		case api.FrameHearbeat:
			c.Delegate.OnHeartbeat(c)
		}
		bspool.Put(framePayload)
	}

exit:

	close(c.exitChan) // should trigger to stop wait on writeloop
	close(c.readDoneChan)
	atomic.StoreInt32(&c.readExitFlag, 1)
	c.wg.Done()
	//log.Printf("[Conn -> %s] Close read loop\n", c.addr)
}

func decodeMessage(buf []byte) *Message {
	var cmsg core.Message
	cmsg.Decode2(buf)

	payload := bspool.Get(uint32(len(cmsg.Payload)))
	copy(payload, cmsg.Payload)

	msg := GetMessageFromPool()

	msg.ID = cmsg.ID
	msg.Payload = payload
	msg.Timestamp = int64(cmsg.Timestamp)
	msg.Attemps = cmsg.Attemps
	return msg
}

func (c *Conn) writeLoop() {
	var flushChan <-chan time.Time
	if c.flushInterval != 0 {
		flushChan = time.NewTicker(c.flushInterval).C
	}

	handleMsgResp := func(msg *Message) error {
		atomic.AddInt32(&c.msgHandlingCount, -1)
		bspool.Put(msg.Payload)
		var cmd *Command
		// TODO: may check another condition
		if msg.State == StateMsgSucceed {
			cmd = CmdFinish(msg.ID)
		} else {
			cmd = CmdRequeue(msg.ID)
		}
		PutMessageToPool(msg)
		return c.BufferCommand(cmd)
	}

	for {
		select {
		case <-flushChan:
			c.Flush()

		case <-c.exitChan:
			// readloop call to close exitChan when it detect error
			goto exit

		case msg := <-c.msgRespChan:

			atomic.AddInt32(&c.msgHandlingCount, -1)
			bspool.Put(msg.Payload)
			err := handleMsgResp(msg)
			if err != nil {
				atomic.StoreInt32(&c.closeFlag, 1)
				goto exit
			}

			/*
				*** This loop to send batch, an alternative for send periodically
					err := handleMsgResp(msg)
					if err != nil {
						atomic.StoreInt32(&c.closeFlag, 1)
						goto exit
					}
					var cnt = 1

				recv_loop:
					for {
						select {
						case msg := <-c.msgRespChan:
							if err := handleMsgResp(msg); err != nil {
								atomic.StoreInt32(&c.closeFlag, 1)
								goto exit
							}
							cnt++
						default:
							c.Flush()
							c.MsgRespBatchSz = append(c.MsgRespBatchSz, cnt)
							break recv_loop
						}
					} */

			if atomic.LoadInt32(&c.readExitFlag) == 1 && atomic.LoadInt32(&c.msgHandlingCount) == 0 {
				// when go here it may close gracefully, the connection still be ok, but caller call explicitly
				// to close and there are no messsage being processed
				goto exit
			}
		}
	}

exit:

	/*
		when reach this point, it's in 2 cases:
		Case 1: connection 's still ok, it reaches here by a gracefull close, and it ensures that
		there are no msg Inflight more in the future. So it should call conn.CloseRead to ensure that
		the connection does not block on read side

		Case 2: the conn is broken, it ensures that the readloop would exit by itself,
	*/

	c.netConn.CloseRead()
	<-c.readDoneChan
	// wait for readLoop done first to ensure that there are no msg more,
	// so not block the message handler loop
	for atomic.LoadInt32(&c.msgHandlingCount) > 0 {
		<-c.msgRespChan
		atomic.AddInt32(&c.msgHandlingCount, -1)
	}
	c.netConn.CloseWrite()
	//log.Printf("[Conn -> %s] close writeloop\n", c.addr)

	c.Delegate.OnConnClose(c)
	c.wg.Done()
}

func (c *Conn) TriggerClose() {
	//atomic.StoreInt32(&c.closeFlag, 1)
	if atomic.CompareAndSwapInt32(&c.closeFlag, 0, 1) && atomic.LoadInt32(&c.msgHandlingCount) == 0 {
		if c.netConn != nil {
			c.netConn.CloseRead()
		}
	}
}
