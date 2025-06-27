package core

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"slices"
	"time"

	"github.com/khoakmp/smq/api"
	"github.com/khoakmp/smq/api/b2m"
)

const (
	StateDisConnected int32 = 0
	StateConnected    int32 = 1
)

const (
	CmdTypeSpec uint8 = iota
	CmdTypeRegister
	CmdTypeUnRegister
)

// all call to modify state of MonitorPeer just in one goroutine -> not need lock
type MonitorPeer struct {
	addr    string //tcp address
	netConn net.Conn
	state   int32
	// not have reader or writer, it will read and recv directly throught conn
	getStateMetadata func() StateMetadata
}

func (p *MonitorPeer) Read(buf []byte) (n int, err error) {
	p.netConn.SetReadDeadline(time.Now().Add(time.Second))
	return p.netConn.Read(buf)
}

func (p *MonitorPeer) Write(buf []byte) (n int, err error) {
	p.netConn.SetWriteDeadline(time.Now().Add(time.Second))
	return p.netConn.Write(buf)
}

func (p *MonitorPeer) Connect() error {
	// limit the time for establishing connection and IO operator
	// avoid blocking in long time because all call Monitors are maded in one goroutine
	conn, err := net.DialTimeout("tcp", p.addr, time.Second)
	if err != nil {
		return err
	}
	p.netConn = conn

	spec := b2m.BrokerSpecification{
		BroadcastTCPPort:  opts.BroadcastTCPPort,
		BroadcastHTTPPort: opts.BroadcastHTTPPort,
	}
	cmd := CmdSpec(&spec)
	_, err = cmd.WriteTo(p)
	if err != nil {
		p.Close()
		return err
	}
	err = p.sendCurrentState()
	if err != nil {
		p.Close()
		return err
	}
	p.state = StateConnected
	return nil
}

func (p *MonitorPeer) sendCurrentState() error {
	stateMetadata := p.getStateMetadata()
	var err error
	for _, topicMetadata := range stateMetadata.Topics {
		cmd := CmdRegister(topicMetadata.TopicName, "")
		_, err = cmd.WriteTo(p)
		if err != nil {
			return err
		}
		for _, groupName := range topicMetadata.ConsumerGroups {
			cmd = CmdRegister(topicMetadata.TopicName, groupName)
			_, err = cmd.WriteTo(p)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type Command struct {
	Type    byte
	Payload []byte
}

func (c *Command) WriteTo(w io.Writer) (int64, error) {

	var buf = make([]byte, 5+len(c.Payload))
	buf[0] = c.Type

	binary.BigEndian.PutUint32(buf[1:5], uint32(len(c.Payload)))
	copy(buf[5:], c.Payload)

	_, err := w.Write(buf[:])
	if err != nil {
		return 0, err
	}
	return int64(len(buf)), nil
}

func CmdSpec(s *b2m.BrokerSpecification) *Command {
	return &Command{
		Type:    CmdTypeSpec,
		Payload: s.Encode(),
	}
}

func CmdRegister(topicName, groupName string) *Command {
	payload := b2m.Registration{
		TopicName: topicName,
		GroupName: groupName,
	}
	return &Command{
		Type:    CmdTypeRegister,
		Payload: payload.Encode(),
	}
}
func CmdUnRegister(topicName, groupName string) *Command {
	payload := b2m.Registration{
		TopicName: topicName,
		GroupName: groupName,
	}

	return &Command{
		Type:    CmdTypeUnRegister,
		Payload: payload.Encode(),
	}
}

func (p *MonitorPeer) Do(cmd *Command) error {
	if p.state == StateDisConnected {
		err := p.Connect()
		if err != nil {
			return err
		}
	}
	// at this point, c.state = StateConnected
	_, err := cmd.WriteTo(p)
	if err != nil {
		p.Close()
		return err
	}
	err = readMonitorResponse(p)
	if err != nil {
		p.Close()
		return err
	}
	return nil
}

func readMonitorResponse(r io.Reader) error {
	// TODO: may update later
	var buf [5]byte
	_, err := r.Read(buf[:])
	if err != nil {
		return err
	}

	var size uint32 = binary.BigEndian.Uint32(buf[1:])
	switch buf[0] {
	case api.FrameError:
		buffer := make([]byte, size)
		_, err = r.Read(buffer)
		if err != nil {
			return err
		}
		return errors.New(string(buffer))
	}
	return nil
}

func (p *MonitorPeer) Close() {
	p.state = StateDisConnected
	if p.netConn != nil {
		p.netConn.Close()
	}
}

func newMonitorPeer(addr string, getStateMetadata func() StateMetadata) *MonitorPeer {
	return &MonitorPeer{
		addr:             addr,
		netConn:          nil,
		state:            StateDisConnected,
		getStateMetadata: getStateMetadata,
	}
}

// call this loop at initial time, it must have list of monitor tcp addresses
func (b *BrokerBase) notifyLoop() {
	var peers []*MonitorPeer
	var tcpAddrs []string

	getPeer := func(addr string) (int, *MonitorPeer) {
		for idx, c := range peers {
			if c.addr == addr {
				return idx, c
			}
		}
		return -1, nil
	}

	for _, addr := range b.monitorTCPAddrs.Load().([]string) {
		peers = append(peers, newMonitorPeer(addr, b.GetFullMetdata))
		tcpAddrs = append(tcpAddrs, addr)
	}

	log.Printf("%c Initiated notifyloop, num monitors: %d\n", '\U00002705', len(peers))

	for {
		select {

		case <-b.exitChan:
			goto exit
		case v := <-b.notifyChan:
			var cmd *Command
			switch v := v.(type) {
			case *Topic:
				if v.Exiting() {
					cmd = CmdUnRegister(v.Name, "")
				} else {
					cmd = CmdRegister(v.Name, "")
				}

			case *ConsumerGroup:
				if v.Exiting() {
					cmd = CmdUnRegister(v.TopicName, v.Name)
				} else {
					cmd = CmdRegister(v.TopicName, v.Name)
				}
			}

			for _, c := range peers {
				err := c.Do(cmd)
				log.Println(err)
			}

		case <-b.updateMonitorTCPAddrsChan:
			addrs := b.monitorTCPAddrs.Load().([]string)

			//fmt.Println("detect monitor tcp addrs updated, the new addrs:", addrs)
			log.Printf("%c Monitor TCP Addresses updated %s\n", '\U0001F4E2', addrs)

			newPeers := make([]*MonitorPeer, 0, len(addrs))

			for _, addr := range tcpAddrs {
				if slices.Contains(addrs, addr) {
					_, p := getPeer(addr)
					newPeers = append(newPeers, p)
					continue
				}
				_, p := getPeer(addr)
				p.Close()
			}

			for _, addr := range addrs {
				if slices.Contains(tcpAddrs, addr) {
					continue
				}
				// new addr added, so try to create a monitorPeer, at this time, the new peer state is Disconnected
				// it only connect when there are some event sent to notify chan
				newPeers = append(newPeers, newMonitorPeer(addr, b.GetFullMetdata))
			}

			tcpAddrs = make([]string, 0, len(addrs))
			tcpAddrs = append(tcpAddrs, addrs...)
			peers = newPeers
		}
	}
exit:
	for _, p := range peers {
		p.Close()
	}
	log.Printf("%c STOP notifyLoop\n", '\u2714')
}
