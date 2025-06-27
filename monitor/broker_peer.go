package monitor

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"

	"github.com/khoakmp/smq/api"
	"github.com/khoakmp/smq/api/b2m"
)

const (
	StateInitialized = 0
)

type Groups struct {
	GroupNames []string `json:"groups"`
}

type BrokerTopics map[string]*Groups

// this is created when accepting connection from broker
type BrokerPeer struct {
	netConn         net.Conn
	topicGroups     map[string]*Groups
	mutex           sync.RWMutex
	state           int // no se la first > initialize dung
	addr            string
	netInfor        *BrokerNetInfo
	deleteCb        func(b *BrokerPeer)
	cmdResponseChan chan *CommandResp
	exitChan        chan struct{}
	wg              sync.WaitGroup
}

type CommandResp struct {
	resp []byte
	err  error
}

func (p *BrokerPeer) writeLoop(m *MonitorV2) {
	fmt.Println("start writeloop")

	for {
		select {
		case <-p.exitChan:
			goto exit
		case resp := <-p.cmdResponseChan:
			var frameType byte = api.FrameResponse

			var payload []byte = resp.resp
			if resp.err != nil {
				frameType = api.FrameError
				payload = []byte(resp.err.Error())
			}
			//fmt.Println("send frame type:", frameType)
			if err := sendFrame(p.netConn, frameType, payload); err != nil {
				goto exit
			}
		}
	}
exit:
	// does not call netconn.close at this point
	// because in all cases, it only reach this point when the netconn is already close
	//p.netConn.Close()
	m.HandleCommand(&Command{
		broker:  p,
		Type:    TypeClearBrokerRegistration,
		Payload: nil,
	})

	if p.deleteCb != nil {
		p.deleteCb(p)
	}
	p.wg.Done()
}

var ErrTopicAlreadyRegistered = errors.New("topic is already registered")

func (p *BrokerPeer) RegisterTopic(topicName string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if _, ok := p.topicGroups[topicName]; ok {
		return ErrTopicAlreadyRegistered
	}
	p.topicGroups[topicName] = &Groups{}
	return nil
}

func (p *BrokerPeer) UnregisterTopic(topicName string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	delete(p.topicGroups, topicName)
}

func (p *BrokerPeer) GetGroupNames(topicName string) []string {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.topicGroups[topicName].GroupNames
}

var ErrGroupAlreadyRegistered = errors.New("group is already registered by broker")

var ErrTopicUnregistered = errors.New("topic is unregistered")

func (p *BrokerPeer) RegisterGroup(topicName, groupName string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	groups, ok := p.topicGroups[topicName]

	if ok {
		if slices.Contains(groups.GroupNames, groupName) {
			return ErrGroupAlreadyRegistered
		}
		groups.GroupNames = append(groups.GroupNames, groupName)
		return nil
	}
	return ErrTopicUnregistered
}

var ErrNotRegisteredGroup = errors.New("group is not registered by broker")

func (p *BrokerPeer) UnregisterGroup(topicName, groupName string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	groups, ok := p.topicGroups[topicName]

	if !ok {
		return ErrNotRegisteredGroup
	}

	idx := slices.Index(groups.GroupNames, groupName)
	if idx != -1 {
		groups.GroupNames = slices.Delete(groups.GroupNames, idx, idx+1)
	}
	return nil
}

func (c *BrokerPeer) specify(spec b2m.BrokerSpecification) {
	c.netInfor.BroadcastHTTPPort = spec.BroadcastHTTPPort
	c.netInfor.BroadcastTCPPort = spec.BroadcastTCPPort
}

func (c *BrokerPeer) Str() string {
	return c.addr
}

func (c *BrokerPeer) Close() {
	c.netConn.Close()
}

func NewBrokerConn(addr string, c net.Conn, deleteCb func(b *BrokerPeer)) *BrokerPeer {
	hostname := strings.Split(addr, ":")[0]

	return &BrokerPeer{
		netConn: c,
		state:   StateInitialized,
		addr:    addr,
		netInfor: &BrokerNetInfo{
			Hostname: hostname,
		},
		topicGroups:     make(map[string]*Groups),
		deleteCb:        deleteCb,
		cmdResponseChan: make(chan *CommandResp),
		mutex:           sync.RWMutex{},
		exitChan:        make(chan struct{}),
	}
}

func readRequest(r io.Reader) (rtype byte, payload []byte, err error) {
	var buf [5]byte
	// observation: all commands from broker to monitor are small size , < 100byte ?
	_, err = r.Read(buf[:])
	if err != nil {
		return
	}
	rtype = buf[0]
	var sz uint32 = binary.BigEndian.Uint32(buf[1:])
	if sz > 0 {
		payload = make([]byte, sz)
		_, err = io.ReadFull(r, payload)
		if err != nil {
			fmt.Println(err)
		}
	}

	return
}

func IOLoop(c *BrokerPeer, monitor *Monitor) {
	for {
		rtype, payload, err := readRequest(c.netConn)
		if err != nil {
			goto exit
		}
		resp, err := exec(rtype, payload, c, monitor)
		if err != nil {
			err := sendFrame(c.netConn, api.FrameError, []byte(err.Error()))
			if err != nil {
				goto exit
			}
		}
		err = sendFrame(c.netConn, api.FrameResponse, resp)
		if err != nil {
			goto exit
		}
	}
exit:
	c.deleteCb(c)
}

func (b *BrokerPeer) readloop(m *MonitorV2) {
	fmt.Println("start readloop")
	for {
		cmdType, payload, err := readRequest(b.netConn)
		if err != nil {
			fmt.Println("failed to read", err)
			goto exit
		}
		switch cmdType {
		case b2m.CmdTypeRegister, b2m.CmdTypeUnRegister:
			var p *b2m.Registration = new(b2m.Registration)
			p.Decode(payload)
			cmd := NewCommand(TypeRegister, p, b, b.cmdResponseChan)

			if cmdType == b2m.CmdTypeUnRegister {
				cmd.Type = TypeUnRegister
			}
			m.HandleCommand(cmd)

		case b2m.CmdTypeSpec:
			var r b2m.BrokerSpecification
			r.Decode(payload)
			b.specify(r)
			b.cmdResponseChan <- &CommandResp{
				resp: OK,
				err:  nil,
			}
		}
	}
exit:
	close(b.exitChan)
	b.wg.Done()
}

func sendFrame(w io.Writer, frameType byte, payload []byte) error {
	buffer := bytes.NewBuffer(nil)
	buffer.Grow(5 + len(payload))

	buffer.WriteByte(frameType)
	binary.Write(buffer, binary.BigEndian, uint32(len(payload)))
	buffer.Write(payload)
	_, err := w.Write(buffer.Bytes())

	return err
}

func exec(rtype byte, payload []byte, b *BrokerPeer, m *Monitor) ([]byte, error) {
	switch rtype {
	case b2m.CmdTypeSpec:
		var spec b2m.BrokerSpecification
		spec.Decode(payload)
		b.specify(spec)
		return []byte("OK"), nil

	case b2m.CmdTypeRegister:
		var reg b2m.Registration
		reg.Decode(payload)
		register(b, reg, m)
		// TODO: handle error
		return []byte("OK"), nil

	case b2m.CmdTypeUnRegister:
		var reg b2m.Registration
		reg.Decode(payload)
		unregister(b, reg, m)
		return []byte("OK"), nil

	}

	return nil, errors.New("unsupport command type")
}

func register(b *BrokerPeer, reg b2m.Registration, m *Monitor) error {
	if len(reg.GroupName) == 0 {
		b.RegisterTopic(reg.TopicName)
		topic := m.store.GetTopic(reg.TopicName, true)
		err := topic.RegisterBroker(b.Str(), b.netInfor)
		topic.FreeKeepAlive()
		return err
	}
	b.RegisterGroup(reg.TopicName, reg.GroupName)
	topic := m.store.GetTopic(reg.TopicName, true)
	err := topic.RegisterBrokerGroup(b.Str(), reg.GroupName)
	topic.FreeKeepAlive()
	return err
}

func unregister(b *BrokerPeer, reg b2m.Registration, m *Monitor) {
	if len(reg.GroupName) == 0 {
		groupNames := b.GetGroupNames(reg.TopicName)
		topic := m.store.GetTopic(reg.TopicName, true)
		topic.UnregisterBroker(b.Str(), groupNames)
		topic.FreeKeepAlive()
		b.UnregisterTopic(reg.TopicName)
		return
	}
	topic := m.store.GetTopic(reg.TopicName, true)
	topic.UnregisterBrokerGroup(reg.GroupName)
	topic.FreeKeepAlive()
	b.UnregisterGroup(reg.TopicName, reg.GroupName)
}
