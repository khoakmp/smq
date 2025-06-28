package cli

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khoakmp/smq/api/c2b"
	"github.com/khoakmp/smq/utils"
)

const (
	StateInit int32 = iota
	StateDisconnected
	StateConnected
	StateStopped
)

type ProducerTransaction struct {
	cmd      *Command // but it should keep one channel
	doneChan chan struct{}
	err      error
	resp     []byte
}

func newProducerTransaction(cmd *Command) *ProducerTransaction {
	return &ProducerTransaction{
		cmd:      cmd,
		doneChan: make(chan struct{}),
	}
}

func (tx *ProducerTransaction) Error() error {
	<-tx.doneChan
	return tx.err
}

func (tx *ProducerTransaction) Success() bool {
	<-tx.doneChan
	return tx.err == nil
}

func (tx *ProducerTransaction) fail(err error) {
	tx.err = err
	close(tx.doneChan)
}

func (tx *ProducerTransaction) success(resp []byte) {
	tx.resp = resp
	close(tx.doneChan)
}

type ProducerMeasure struct {
	txCounter         int64
	startFlag         int32
	firstRecvTxTs     int64
	lastCompletedTxTs int64
	lastN             int64
}

func (m *ProducerMeasure) CountAndRecord() {
	m.txCounter++

	if m.txCounter == m.lastN {
		m.lastCompletedTxTs = time.Now().UnixNano()
	}
}

func (m *ProducerMeasure) Set(lastN int64) {
	m.lastN = lastN
	m.txCounter = 0
	m.startFlag = 0
	m.firstRecvTxTs = 0
	m.lastCompletedTxTs = 0
}

func (m *ProducerMeasure) RecordFirstTx() {
	if atomic.CompareAndSwapInt32(&m.startFlag, 0, 1) {
		m.firstRecvTxTs = time.Now().UnixNano()
	}
}
func (m *ProducerMeasure) Print() {
	fmt.Printf("Duration for completing %d tx: %s\n", m.lastN, time.Duration(m.lastCompletedTxTs-m.firstRecvTxTs))
}

// when len == 0 or len == cap -> rp = wp dugn la dang do dung
type TransactionQueue struct {
	trans    []*ProducerTransaction
	rp, wp   int // rp: position to read, wp: position to write
	len, cap int
}

func newTransactionQueue() *TransactionQueue {
	return &TransactionQueue{
		trans: make([]*ProducerTransaction, 4),
		rp:    0,
		wp:    0,
		len:   0,
		cap:   4,
	}
}

// not call grow when len == 0
func (q *TransactionQueue) grow() {
	ncap := q.cap << 1
	arr := make([]*ProducerTransaction, ncap)
	if q.rp < q.wp {
		copy(arr[:q.len], q.trans[q.rp:q.wp])

	} else {
		offset := q.cap - q.rp
		copy(arr[:offset], q.trans[q.rp:q.cap])
		copy(arr[offset:offset+q.wp], q.trans[:q.wp])
	}
	q.trans = arr
	q.rp = 0
	q.wp = q.len
	q.cap = ncap
}

func (q *TransactionQueue) Push(tx *ProducerTransaction) {
	if q.len == q.cap {
		q.grow()
	}
	q.trans[q.wp] = tx
	q.wp = (q.wp + 1) % q.cap
	q.len++
}

func (q *TransactionQueue) Peek() *ProducerTransaction {
	if q.len == 0 {
		return nil
	}
	return q.trans[q.rp]
}

func (q *TransactionQueue) Pop() *ProducerTransaction {
	if q.len == 0 {
		return nil
	}
	var ans = q.trans[q.rp]
	q.trans[q.rp] = nil
	q.rp = (q.rp + 1) % q.cap
	q.len--
	return ans
}

type Producer struct {
	addr            string
	conn            *Conn
	connClosedChan  chan struct{}             // should use non-buffer channel ?
	exitChan        chan struct{}             // non-buffer channel
	transactionChan chan *ProducerTransaction // non-buffered
	transQueue      *TransactionQueue
	state           int32
	errChan         chan []byte
	respChan        chan []byte
	wg              sync.WaitGroup
	flushChan       chan struct{}
	TraceID         string // self-generated or get from constructor, send this id to broker to trace

	// used for debug
	TxNotSendCount   int64
	TxSendCount      int64
	TxCompletedCount int64

	Measurer *ProducerMeasure
}

// TODO: check addr valid , return err
func NewProducer(addr string) *Producer {

	traceID := utils.RandString(6)
	// messages are kept at broker, try to push immediately to consumer if consumer is ready ?

	p := &Producer{
		addr:            addr,
		conn:            nil,
		connClosedChan:  make(chan struct{}, 1),
		exitChan:        make(chan struct{}),
		transactionChan: make(chan *ProducerTransaction, 200),
		transQueue:      newTransactionQueue(),
		state:           StateInit,
		errChan:         make(chan []byte, 200),
		respChan:        make(chan []byte, 200),
		flushChan:       make(chan struct{}),
		TraceID:         traceID,
		Measurer:        &ProducerMeasure{},
	}
	p.wg.Add(1)
	go p.routeLoop()
	return p
}

func (p *Producer) Flush() {
	select {
	case p.flushChan <- struct{}{}:
	case <-p.exitChan:
	}
}

// take 7 bytes for each message publish response dung?
// at this point, it also send message in batch dung la dang do dng
// van de ro rang la send message in batch
func (p *Producer) sendTx(tx *ProducerTransaction) error {
	select {
	case p.transactionChan <- tx:
	case <-p.exitChan:
		return errors.New("producer stopping")
	}
	return nil
}

func (p *Producer) Produce(topic string, msg []byte) (*ProducerTransaction, error) {
	if atomic.LoadInt32(&p.state) == StateDisconnected {
		return nil, ErrConnDisconnected
	}

	tx := newProducerTransaction(CmdPublish(topic, msg))
	if err := p.sendTx(tx); err != nil {
		return nil, err
	}

	return tx, nil
}

func (p *Producer) ProduceBatch(topic string, msgs [][]byte) (*ProducerTransaction, error) {
	if atomic.LoadInt32(&p.state) == StateDisconnected {
		return nil, ErrConnDisconnected
	}
	tx := newProducerTransaction(CmdPublishBatch(topic, msgs))
	if err := p.sendTx(tx); err != nil {
		return nil, err
	}
	return tx, nil
}

func (p *Producer) TxQueueLen() int {
	return p.transQueue.len
}

func (p *Producer) pushTransaction(tx *ProducerTransaction) {
	p.transQueue.Push(tx)
}

func (p *Producer) popTransaction() (tx *ProducerTransaction) {
	tx = p.transQueue.Pop()
	return
}

// only one goroutine can call this function,
// when call this function, it always guarantee that the old connection is closed (stop both readloop/writeloop)
// and no goroutines can change p.state, current p.state is StateInit
func (p *Producer) connect() error {
	connDeletage := ProducerConnDelegate{p: p}
	// TODO: change flushInterval
	conn := NewConn(p.addr, connDeletage, 0)

	// TOOD: change write buffer size
	err := conn.Connect(1024, 4096, c2b.PayloadClientSpec{
		RecvWindow:           0,
		PushMessagesInterval: 0,
		TraceID:              p.TraceID,
	})

	if err != nil {
		log.Printf("[Producer %s] Failed to connect broker %s, caused by: %s\n", p.TraceID, p.addr, err)
		return err
	}

	p.conn = conn
	atomic.StoreInt32(&p.state, StateConnected)
	log.Printf("[Producer %s] SUCCESS Connect Broker %s\n", p.TraceID, p.addr)
	return nil
}

var ErrConnDisconnected = errors.New("conn is disconnected")

func (p *Producer) routeLoop() {
	defer p.wg.Done()

	handleNewTx := func(tx *ProducerTransaction) error {
		err := p.conn.BufferCommand(tx.cmd)
		if err != nil {
			atomic.AddInt64(&p.TxNotSendCount, 1)
			tx.fail(ErrConnDisconnected)
			return err
		}
		atomic.AddInt64(&p.TxSendCount, 1)
		p.Measurer.RecordFirstTx()

		p.pushTransaction(tx)
		return nil
	}

	for {
		select {
		case tx := <-p.transactionChan:
			if atomic.LoadInt32(&p.state) == StateInit {
				if err := p.connect(); err != nil {
					atomic.AddInt64(&p.TxNotSendCount, 1)
					tx.fail(ErrConnDisconnected)
					continue
				}
			}

			if err := handleNewTx(tx); err != nil {
				continue
			}
		loop:
			for {
				select {
				case tx := <-p.transactionChan:
					if err := handleNewTx(tx); err != nil {
						//break loop
					}
				default:
					p.conn.Flush()
					break loop
				}
			}
			/* if err := p.conn.BufferCommand(tx.cmd); err != nil {
				atomic.AddInt64(&p.TxNotSendCount, 1)
				tx.fail(ErrConnDisconnected)
				continue
			}

			// at this point, it ensure that command of this transaction is written to send buffer
			// so we can push it to tx queue
			atomic.AddInt64(&p.TxSendCount, 1)
			p.Measurer.RecordFirstTx()

			p.pushTransaction(tx) */
		case <-p.flushChan:
			p.conn.Flush()

		case <-p.connClosedChan:
			//fmt.Println("Get signal current connection closed completely")
			p.cleanTransactionQueue()
			atomic.StoreInt32(&p.state, StateInit)
			log.Printf("[%s] Closed connection to %s completely, change to state: StateInit\n", p.TraceID, p.addr)

		case errBuf := <-p.errChan:
			//fmt.Println("Get error from errChan")
			p.Measurer.CountAndRecord()
			p.completeTx(nil, errBuf)
		case resp := <-p.respChan:
			//fmt.Println("Get resp from respChan")
			p.Measurer.CountAndRecord()
			p.completeTx(resp, nil)
		case <-p.exitChan:
			goto exit
		}
	}
exit:

	state := atomic.LoadInt32(&p.state)
	if state == StateInit {
		p.cleanTransactionQueue()
		atomic.StoreInt32(&p.state, StateStopped)
		return
	}

	for p.TxQueueLen() != 0 {
		select {
		case err := <-p.errChan:
			p.completeTx(nil, err)
		case resp := <-p.respChan:
			p.completeTx(resp, nil)

		case <-p.connClosedChan:
			p.cleanTransactionQueue()
			atomic.StoreInt32(&p.state, StateStopped)
			return
		}
	}
	p.closeConn()
	<-p.connClosedChan
	atomic.StoreInt32(&p.state, StateStopped)
}

func (p *Producer) completeTx(resp []byte, err []byte) {
	tx := p.popTransaction()
	if tx == nil {
		p.closeConn()
		log.Printf("[Producer %s] Conflict No Transaction found for response\n", p.TraceID)
		return
	}
	atomic.AddInt64(&p.TxCompletedCount, 1)
	if err != nil {
		tx.fail(errors.New(string(err)))
		return
	}

	tx.success(resp)
}

func (p *Producer) cleanTransactionQueue() {
	for p.TxQueueLen() > 0 {
		tx := p.popTransaction()
		tx.fail(ErrConnDisconnected)
	}
}

func (p *Producer) closeConn() {
	if !atomic.CompareAndSwapInt32(&p.state, StateConnected, StateDisconnected) {
		return
	}
	p.conn.TriggerClose()
}

func (p *Producer) CloseWait() {
	close(p.exitChan)
	p.wg.Wait()
	log.Printf("[Producer %s] CLOSED completely\n", p.TraceID)
}

func (p *Producer) PrintStats() {
	buffer := bytes.NewBuffer(nil)
	fmt.Fprintf(buffer, "TxNotSendCount:%d\n", atomic.LoadInt64(&p.TxNotSendCount))
	fmt.Fprintf(buffer, "TxSendCount:%d\n", atomic.LoadInt64(&p.TxSendCount))
	fmt.Fprintf(buffer, "TxCompletedCount:%d\n", atomic.LoadInt64(&p.TxCompletedCount))
	fmt.Print(buffer.String())
}

type ProducerConnDelegate struct {
	p *Producer
}

func (d ProducerConnDelegate) OnIOError(err error, c *Conn) { d.p.closeConn() }

func (d ProducerConnDelegate) OnConnClose(c *Conn) {
	select {
	case d.p.connClosedChan <- struct{}{}:
	default:
	}
}

func (d ProducerConnDelegate) OnMessage(msg *Message)      {}
func (d ProducerConnDelegate) OnError(err []byte, c *Conn) { d.p.errChan <- err }
func (d ProducerConnDelegate) OnResponse(resp []byte, c *Conn) {
	d.p.respChan <- resp
}
func (d ProducerConnDelegate) OnHeartbeat(c *Conn) {}

/*
OnMessage(msg *Message)
OnResponse(resp []byte, c *Conn)
OnError(err []byte, c *Conn)
OnHeartbeat(c *Conn)
OnConnClose(c *Conn)
OnIOError(err error, c *Conn)
*/
