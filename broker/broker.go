package broker

import (
	"log"
	"net"

	"github.com/khoakmp/smq/broker/cli"
	"github.com/khoakmp/smq/broker/core"
)

type Broker struct {
	state       *core.BrokerBase
	tcpListener net.Listener
}

func New(ID string, monitorTCPAddrs, monitorHttpAddrs []string) *Broker {
	b := &Broker{
		state: core.NewBrokerBase(ID, monitorTCPAddrs, monitorHttpAddrs),
	}
	return b
}

func (b *Broker) Stop() {
	if b.state != nil {
		b.state.Close()
	}
	if b.tcpListener != nil {
		b.tcpListener.Close()
	}
}

func (b *Broker) RunTCP(addr string) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		panic("faield to run tcp server")
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println(err)
			break
		}

		go func() {
			client := cli.NewClient(conn)
			server := &cli.ServeV1{}
			server.IOLoop(client, b.state)
		}()
	}

	log.Println("STOP TCP listener")
}

func (b *Broker) GetBrokerBase() *core.BrokerBase {
	return b.state
}
