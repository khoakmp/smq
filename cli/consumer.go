package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khoakmp/smq/api/c2b"
)

type MessageHandler interface {
	Handle(msg *Message) error
}

type Consumer struct {
	topicName string
	groupName string

	incommingMessageChan chan *Message // non-buffered channel
	rmtx                 sync.RWMutex
	conns                map[string]*Conn
	handler              MessageHandler
	wg                   sync.WaitGroup
	updateConnsChan      chan struct{} // buffered channel with size 1,just trigger only
	backoffState         int32
	backoffCounter       int32
	updateBackoffChan    chan struct{}
	cbreaker             *CircuitBreaker
	exitChan             chan struct{}
	maxInflight          int32
	config               ConsumerConfig
	pendingConns         map[string]*Conn
	stopFlag             int32

	monitorHttpUrls   []string
	monitorIndex      int           // use round-robin when contact to monitors
	pollMonitorChan   chan struct{} // size 1
	monitorHttpClient *http.Client

	connsClosedChan   chan struct{}
	closeAllConnsFlag int32
	TraceID           string // use for debug
	MaxAttemp         uint16 // default is 1, disable retry
}

func (cs *Consumer) SetMaxAttemp(n uint16) {
	cs.MaxAttemp = n
}

func NewConsumerDebug(topicName, groupName string, handler MessageHandler, config *ConsumerConfig, traceID string) *Consumer {
	c := NewConsumer(topicName, groupName, handler, config)
	c.TraceID = traceID
	return c
}

func NewConsumer(topicName, groupName string, handler MessageHandler, config *ConsumerConfig) *Consumer {
	c := &Consumer{
		topicName:            topicName,
		groupName:            groupName,
		incommingMessageChan: make(chan *Message, 10),
		conns:                make(map[string]*Conn),
		pendingConns:         make(map[string]*Conn),
		handler:              handler,
		rmtx:                 sync.RWMutex{},
		wg:                   sync.WaitGroup{},
		updateConnsChan:      make(chan struct{}, 1),
		backoffState:         0,
		backoffCounter:       0,
		updateBackoffChan:    make(chan struct{}, 1),
		cbreaker:             nil,
		exitChan:             make(chan struct{}),
		maxInflight:          config.DefaultMaxInflight, // use const
		config:               *config,
		stopFlag:             0,
		monitorHttpUrls:      make([]string, 0),
		monitorIndex:         0,
		pollMonitorChan:      make(chan struct{}, 1),
		connsClosedChan:      make(chan struct{}),
		closeAllConnsFlag:    0,
		MaxAttemp:            1,
	}

	c.cbreaker = NewCircuitBreaker(c.updateBackoff, CircuitBreakerConfig{
		FailedMsgCountOpenCB:      c.config.FailedMsgCountToOpenCB,
		SucceeddMsgCountToCloseCB: c.config.SucceeddMsgCountToCloseCB,
	})
	c.wg.Add(1)
	go c.calcReadyLoop()
	concurrent := max(config.ConcurrentHandler, 1)
	c.wg.Add(concurrent)

	for range concurrent {
		go c.HandleLoop()
	}
	return c
}

func (cs *Consumer) triggerPollMonitor() {
	select {
	case cs.pollMonitorChan <- struct{}{}:
	default:
	}
}

// start or stop backoff
func (cs *Consumer) updateBackoff(setBackoff int32) {
	atomic.StoreInt32(&cs.backoffState, setBackoff)

	if setBackoff == 0 {
		atomic.StoreInt32(&cs.backoffCounter, 0)
		log.Printf("[Consumer %s] Stop Backoff mode\n", cs.TraceID)
	} else {
		v := atomic.AddInt32(&cs.backoffCounter, 1)
		log.Printf("[Consumer %s] Incr Backoff counter to %d \n", cs.TraceID, v)
	}
	select {
	case cs.updateBackoffChan <- struct{}{}:
	default:
	}
}

var ErrConnAlreadyExisted = errors.New("conn already existed")

func (cs *Consumer) pollMonitorLoop() {

	ticker := time.NewTicker(time.Second * 60)
	for {
		select {
		case <-cs.exitChan:
			goto exit
		case <-ticker.C:
		case <-cs.pollMonitorChan:
		}
		cs.queryMonitor()
	}
exit:
	cs.wg.Done()
	// TODO:
}

type BrokerNetInfo struct {
	BroadcastTCPPort  uint16 `json:"broadcast_tcp_port"`
	BroadcastHTTPPort uint16 `json:"broadcast_http_port"`
	Hostname          string `json:"hostname"`
}

type QueryMonitorResp struct {
	BrokerNetInfors []BrokerNetInfo `json:"brokers"`
	GroupNames      []string        `json:"groups"`
}

// this function is called in only one goroutine
func (cs *Consumer) queryMonitor() {
	retries := 0
	for retries < 3 {
		index := cs.monitorIndex
		var url string

		cs.rmtx.RLock()
		n := len(cs.monitorHttpUrls)
		if n == 0 {
			cs.rmtx.RUnlock()
			return
		}
		if index >= n {
			index = n - 1
		}
		url = cs.monitorHttpUrls[index]
		cs.monitorIndex = (cs.monitorIndex + 1) % n
		cs.rmtx.RUnlock()

		resp, err := cs.monitorHttpClient.Get(url)
		if err != nil {
			retries++
			continue
		}
		var queryResult QueryMonitorResp
		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			retries++
			continue
		}

		json.Unmarshal(respBody, &queryResult)
		for _, broker := range queryResult.BrokerNetInfors {
			addr := net.JoinHostPort(broker.Hostname, strconv.Itoa(int(broker.BroadcastTCPPort)))
			err := cs.ConnectBroker(addr)
			if err != nil {
				//TODO: log + handle error
			}
		}
		return
	}
}

// addr format {host:port}
// this function can be called from any goroutines and it's thread-safe
func (cs *Consumer) ConnectMonitor(addr string) error {

	if atomic.LoadInt32(&cs.stopFlag) == 1 {
		return errors.New("consumer stopping")
	}

	monitorUrl := fmt.Sprintf("http://%s/lookup?topic=%s&group=%s", addr, cs.topicName, cs.groupName)
	cs.rmtx.Lock()
	if slices.Contains(cs.monitorHttpUrls, monitorUrl) {
		cs.rmtx.Unlock()
		return errors.New("monitor already connected")
	}

	cs.monitorHttpUrls = append(cs.monitorHttpUrls, monitorUrl)
	n := len(cs.monitorHttpUrls)

	// only initiate client on the first call to connect monitor
	if cs.monitorHttpClient == nil {
		cs.monitorHttpClient = &http.Client{
			Transport: &http.Transport{}, // TODO: set up fields
			Timeout:   time.Second * 60,  // should be replaced by const
		}
	}

	cs.rmtx.Unlock()

	if n == 1 {
		cs.wg.Add(1)
		go cs.pollMonitorLoop()
		cs.triggerPollMonitor()
	}

	return nil
}

func (cs *Consumer) ConnectBroker(addr string) error {

	if atomic.LoadInt32(&cs.stopFlag) == 1 {
		return errors.New("consumer stopping")
	}

	conn := NewConn(addr, &ConsumerConnDelegate{c: cs}, time.Millisecond*200)
	// use flush ticker to flush buffered message responses
	// after calling newConn, it call connect immediately
	// it check that whether the connection to this address is already created by another goroutine, if so
	// it does not take time to establish network connection, otherwise, it reserve this connection
	// in pending conns list, so other goroutine try to create conn should return immediately

	cs.rmtx.Lock()
	_, ok := cs.conns[addr]
	_, pendingOk := cs.pendingConns[addr]
	if ok || pendingOk {
		cs.rmtx.Unlock()
		return ErrConnAlreadyExisted
	}

	cs.pendingConns[addr] = conn
	cs.rmtx.Unlock()

	cleanup := func() {
		cs.rmtx.Lock()
		delete(cs.pendingConns, addr)
		cs.rmtx.Unlock()
		conn.TriggerClose()
	}
	// vi cai dong nay with 8192 ?
	//
	err := conn.Connect(2048, 4096, c2b.PayloadClientSpec{
		RecvWindow:           uint32(cs.maxInflight),
		PushMessagesInterval: uint64(time.Millisecond) * 100,
		TraceID:              cs.TraceID,
	})

	if err != nil {
		// at this point, there are no goroutines for readloop/writeloop
		cleanup()
		return err
	}

	err = conn.WriteCommand(CmdSubscirbe(cs.topicName, cs.groupName))
	if err != nil {
		cleanup()
		return err
	}

	cs.rmtx.Lock()
	delete(cs.pendingConns, addr)
	if atomic.LoadInt32(&cs.stopFlag) == 1 {
		conn.TriggerClose()
		cs.rmtx.Unlock()
		return errors.New("consumer stopping")
	}

	cs.conns[addr] = conn
	cs.rmtx.Unlock()

	cs.triggerUpdateConns()
	return nil
}

func (cs *Consumer) calcReadyLoop() {
	var conns []*Conn
	var resumeTimerChan <-chan time.Time = nil

	var redistributeTicker *time.Ticker = time.NewTicker(5 * time.Second)

	rdyCountPerConn := func() int32 {
		if len(conns) == 0 {
			return 0
		}
		return int32(atomic.LoadInt32(&cs.maxInflight) / int32(len(conns)))
	}

	for {
		select {
		case <-cs.exitChan:
			goto exit
		case <-resumeTimerChan:
			// it does not change the backoff
			// it still be in backoff state and no change backoff counter

			cs.resume(conns)
			cs.cbreaker.TriggerHalfOpen()
			resumeTimerChan = nil

		case <-cs.updateBackoffChan:
			backoff := atomic.LoadInt32(&cs.backoffState)
			//log.Printf("[Consumer %s] Start update ready for conns, backoff state %d\n", cs.TraceID, backoff)

			backoffCounter := atomic.LoadInt32(&cs.backoffCounter)
			if backoff == 0 {
				// Stop backoff
				count := rdyCountPerConn()
				for _, c := range conns {
					cs.updateRdyCount(c, count)
				}
				continue
			}

			// Start backoff
			for _, c := range conns {
				cs.updateRdyCount(c, 0)
			}
			timeout := cs.calcBackoffTimeout(backoffCounter)
			log.Printf("[Consumer %s] backoff timeout: %s\n", cs.TraceID, timeout.String())
			resumeTimerChan = time.NewTimer(timeout).C

		case <-cs.updateConnsChan:
			for idx := range conns {
				conns[idx] = nil
			}
			conns = conns[:0]

			cs.rmtx.RLock()
			for _, c := range cs.conns {
				conns = append(conns, c)
			}
			cs.rmtx.RUnlock()

			if resumeTimerChan != nil || cs.backoffCounter > 0 {
				continue
			}

			// at this point, we ensure that there are no backoff or wait for try to resume
			// so we can distribute evently maxInfligh to all conns
			count := rdyCountPerConn()
			for _, c := range conns {
				cs.updateRdyCount(c, count)
			}

			// when cs.maxInflight < len(conns) , count is 0
			// so must redistribute when redistribute ticker tick
			// take time to process that one dung nen la can lam gi de ma no ok now dung?

		case <-redistributeTicker.C:
			candidates := make([]*Conn, 0)
			n := int(atomic.LoadInt32(&cs.maxInflight))
			// TODO: change condition ...
			if n >= len(conns) || resumeTimerChan != nil {
				continue
			}
			for _, c := range conns {
				if c.RdyCount > 0 {
					duration := time.Now().UnixNano() - atomic.LoadInt64(&c.lastRecvMsgTime)
					if duration > 5*time.Second.Nanoseconds() {
						cs.updateRdyCount(c, 0)
					}
				}
				candidates = append(candidates, c)
			}

			for n > 0 {
				idx := rand.Int() % len(candidates)
				conn := candidates[idx]
				candidates = slices.Delete(candidates, idx, idx+1)
				n--
				cs.updateRdyCount(conn, 1)
			}
		}
	}
exit:
	cs.wg.Done()
}

func (cs *Consumer) resume(conns []*Conn) {
	idx := rand.Int() % len(conns)
	log.Printf("[Consumer %s] Chose Broker connection %s to resume\n", cs.TraceID, conns[idx].addr)
	cs.updateRdyCount(conns[idx], 1)
}

func (cs *Consumer) calcBackoffTimeout(backoffCounter int32) time.Duration {
	backoffCounter = min(backoffCounter, cs.config.MaxBackoffCounter)
	return cs.config.BackOffTimeUnit * (1 << backoffCounter)
}

func (cs *Consumer) updateRdyCount(c *Conn, count int32) {
	c.RdyCount = count
	err := c.WriteCommand(CmdReadyCount(count))
	if err != nil {
		log.Printf("[Consumer %s] FAILED to send readyCount %d to broker [%s] \n", cs.TraceID, count, c.addr)
	} /*  else {
		fmt.Printf("[%s] SUCCEED to send readycount %d to broker [%s] \n", cs.TraceID, count, c.addr)
	} */
}

func (cs *Consumer) triggerUpdateConns() {
	select {
	case cs.updateConnsChan <- struct{}{}:
	default:
	}
}

func (cs *Consumer) Stop() {
	// this function close all connections and wait until it stop all readloop and writeloop for every conn
	// then call stop all handleLoop
	if !atomic.CompareAndSwapInt32(&cs.stopFlag, 0, 1) {
		return
	}

	log.Printf("[Consumer %s] Start STOPPING\n", cs.TraceID)

	cs.rmtx.RLock()
	// before start close for every connection, need ensure that there are no conn added,
	// so we aquired RLock
	if len(cs.conns) == 0 {
		cs.rmtx.RUnlock()

		if atomic.CompareAndSwapInt32(&cs.closeAllConnsFlag, 0, 1) {
			close(cs.connsClosedChan)
		}

	} else {
		// this section start graceful close for all current conns
		for _, c := range cs.conns {
			c.WriteCommand(CmdStartClose())
		}
		cs.rmtx.RUnlock()
	}

	go func() {
		<-cs.connsClosedChan

		cs.StopHandlers()
		close(cs.exitChan)
		cs.wg.Wait()
		log.Printf("[Consumer %s] STOP completed\n", cs.TraceID)
		// TODO: log to indicate consumer stops completely
	}()
}

func (cs *Consumer) HandleLoop() {
	fmt.Printf("[%s] running handle loop\n", cs.TraceID)

	for {
		msg, ok := <-cs.incommingMessageChan
		if !ok {
			goto exit
		}
		generation, err := cs.cbreaker.preProcess()
		if err != nil {
			msg.Requeue()
			continue
		}

		err = cs.handler.Handle(msg)
		cs.cbreaker.postProcess(generation, err == nil)
		msg.Attemps++

		if err != nil && msg.Attemps < cs.MaxAttemp {
			msg.Requeue()
			continue
		}

		msg.Finish()
		// TODO:
		//msg.conn.BufferCommand(CmdFinish(msg.ID))
	}
exit:
	cs.wg.Done()
}
func (cs *Consumer) StopHandlers() {
	close(cs.incommingMessageChan)
}

type ConsumerConnDelegate struct {
	c *Consumer
}

func (d *ConsumerConnDelegate) OnMessage(msg *Message) {
	d.c.incommingMessageChan <- msg
}

func (d *ConsumerConnDelegate) OnConnClose(c *Conn) {
	d.c.rmtx.Lock()
	delete(d.c.conns, c.Str())
	n := len(d.c.conns)
	d.c.rmtx.Unlock()
	log.Printf("[Consumer %s] Closed broker connection at [%s]\n", d.c.TraceID, c.addr)

	/* when consumer.stopFlag == 1,  there is some goroutine called Stop
	and in case that there is at least one conn in conns list, one goroutine is spawned to wait until
	all conns are closed, so the last closing conn should notify that all conns are closed to unblock
	the spawned goroutine
	*/
	if atomic.LoadInt32(&d.c.stopFlag) == 1 {
		if n == 0 {
			if atomic.CompareAndSwapInt32(&d.c.closeAllConnsFlag, 0, 1) {
				log.Printf("[Consumer %s] Trigger all conns are closed \n", d.c.TraceID)
				close(d.c.connsClosedChan)
			}
		}
	}

	d.c.triggerUpdateConns()
	d.c.triggerPollMonitor()
}

func (d *ConsumerConnDelegate) OnResponse(resp []byte, c *Conn) {
	if bytes.Equal(resp, []byte("CLOSE_ACK")) {
		c.TriggerClose()
	}
}

func (d *ConsumerConnDelegate) OnIOError(err error, c *Conn) {
	c.TriggerClose()
}

func (d *ConsumerConnDelegate) OnError(err []byte, c *Conn) {
	// TODO: log err
	// this error is sent by the broker, does not trigger close connection on this error
}
func (d *ConsumerConnDelegate) OnHeartbeat(c *Conn) {}
