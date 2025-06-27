package monitor

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/khoakmp/smq/api/b2m"
)

type MonitorV2 struct {
	store       *ClusterStoreV2
	brokers     map[string]*BrokerPeer
	mutex       sync.RWMutex
	commandChan chan *Command // only use non-buffered chan
	httpAddr    string
	httpServer  *http.Server
	tcpAddr     string
	tcpListener net.Listener
	wg          sync.WaitGroup
	exitFlag    int32
	exitChan    chan struct{}
}

func (m *MonitorV2) ExecAsync(fn func()) {
	m.wg.Add(1)
	go func() {
		fn()
		m.wg.Done()
	}()
}

// TODO: at http server to serve query

func Run(tcpAddr string, httpAddr string) *MonitorV2 {
	m := NewMonitorV2()

	m.tcpAddr = tcpAddr
	m.httpAddr = httpAddr

	m.ExecAsync(m.RunHttpServer)
	m.ExecAsync(m.RunTCPServer)
	m.ExecAsync(m.HandleLoop)

	return m
}

func (m *MonitorV2) RunHttpServer() {
	m.runHttp(m.httpAddr)
	fmt.Println("Exit Http Server")
}

func (m *MonitorV2) RunTCPServer() {
	m.ListenBrokers(m.tcpAddr)
}

func NewMonitorV2() *MonitorV2 {
	m := &MonitorV2{
		store:       NewClusterStoreV2(),
		brokers:     make(map[string]*BrokerPeer),
		mutex:       sync.RWMutex{},
		commandChan: make(chan *Command),
		exitFlag:    0,
		exitChan:    make(chan struct{}),
	}

	//m.SetupHTTP()
	return m
}

var OK []byte = []byte("OK")

// blocking method
func (m *MonitorV2) Shutdown() {
	if !atomic.CompareAndSwapInt32(&m.exitFlag, 0, 1) {
		return
	}
	if m.httpServer != nil {
		m.httpServer.Close()
	}
	if m.tcpListener != nil {
		fmt.Println("stop tcp listener")
		m.tcpListener.Close()
	}
	close(m.exitChan) // trigger stop handleloop
	m.mutex.RLock()
	for _, b := range m.brokers {
		b.Close() // just trigger close connection
	}

	m.mutex.RUnlock()
	fmt.Println("Waiting for all goroutines closed")
	m.wg.Wait()
}

func (m *MonitorV2) runHttp(addr string) {
	mux := http.NewServeMux()

	mux.HandleFunc("/lookup", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		payload := &LookupTopicPayload{
			TopicName: q.Get("topic"),
			GroupName: q.Get("group"),
		}
		respChan := make(chan *CommandResp)
		cmd := NewCommand(TypeLookupTopic, payload, nil, respChan)
		serveCommand(cmd, m, w)
	})

	mux.HandleFunc("/query_groups", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		topicName := q.Get("topic")
		if len(topicName) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		respChan := make(chan *CommandResp)
		cmd := NewCommand(TypeQueryTopicGroups, topicName, nil, respChan)
		serveCommand(cmd, m, w)
	})

	m.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	err := m.httpServer.ListenAndServe()
	if err != nil {
		fmt.Println(err)
	}
}

func serveCommand(cmd *Command, m *MonitorV2, w http.ResponseWriter) {
	err := m.HandleCommand(cmd)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	resp := <-cmd.respChan
	if resp.err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(resp.err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(resp.resp)
}

var ErrMonitorExiting = errors.New("monitor exiting")

func (m *MonitorV2) HandleCommand(cmd *Command) error {
	select {
	case m.commandChan <- cmd:
	case <-m.exitChan:
		return ErrMonitorExiting
	}
	return nil
}

func (m *MonitorV2) AddBroker(addr string, c net.Conn) (*BrokerPeer, error) {
	m.mutex.Lock()
	if _, ok := m.brokers[addr]; ok {
		m.mutex.Unlock()
		return nil, errors.New("brokerPeer already connected")
	}

	b := NewBrokerConn(addr, c, m.DeleteBroker)
	m.brokers[addr] = b
	m.mutex.Unlock()
	return b, nil
}
func (m *MonitorV2) DeleteBroker(b *BrokerPeer) {
	m.mutex.Lock()
	delete(m.brokers, b.Str())
	m.mutex.Unlock()
}

func (m *MonitorV2) ListenBrokers(addr string) error {

	l, err := net.Listen("tcp", addr)

	if err != nil {
		return err
	}
	m.tcpListener = l
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("failed to accept conn:", err)
			goto exit
		}

		b, err := m.AddBroker(conn.RemoteAddr().String(), conn)
		if err == nil {
			b.wg.Add(2)
			go b.readloop(m)
			go b.writeLoop(m)
		} else {
			log.Println(err)
			conn.Close()
		}
	}
exit:
	fmt.Println("Exit TCP Server")
	return err
}

func (m *MonitorV2) HandleLoop() {
	for {
		select {
		case <-m.exitChan:
			goto exit
		case cmd := <-m.commandChan:
			var err error
			var resp []byte
			switch cmd.Type {

			case TypeRegister:
				p := cmd.Payload.(*b2m.Registration)
				err = m.Register(p, cmd.broker)

			case TypeUnRegister:
				p := cmd.Payload.(*b2m.Registration)
				err = m.Unregister(p, cmd.broker)

			case TypeClearBrokerRegistration:
				m.ClearBrokerRegistration(cmd.broker)

			case TypeLookupTopic:
				p := cmd.Payload.(*LookupTopicPayload)
				topicState := m.LookupTopic(p.TopicName, p.GroupName)
				resp, _ = json.Marshal(topicState)

			case TypeQueryTopicGroups:
				topicName := cmd.Payload.(string)
				groups := m.QueryTopicGroups(topicName)
				g := Groups{
					GroupNames: groups,
				}
				resp, _ = json.Marshal(g)
			}

			if err == nil && resp == nil {
				resp = OK
			}
			cmd.Response(resp, err)
		}
	}

exit:
	fmt.Println("Exit Handle Loop")
}

func (m *MonitorV2) Register(reg *b2m.Registration, b *BrokerPeer) error {

	if len(reg.GroupName) == 0 {
		err := b.RegisterTopic(reg.TopicName)
		if err != nil {
			if err == ErrTopicAlreadyRegistered {
				log.Printf("[Topic %s] is already registered by broker %s\n", reg.TopicName, b.Str())
				return nil
			}
			return err
		}

		topic := m.store.GetTopic(reg.TopicName)
		topic.RegisterBroker(b.Str(), *b.netInfor)
		log.Printf("[Topic %s] is just registered by broker %s\n", reg.TopicName, b.Str())
		return nil
	}

	err := b.RegisterGroup(reg.TopicName, reg.GroupName)
	if err != nil {
		if err == ErrGroupAlreadyRegistered {
			return nil
		}
		return err
	}
	topic := m.store.GetTopic(reg.TopicName)
	topic.RegisterBrokerGroup(b.Str(), reg.GroupName)
	return nil
}

func (m *MonitorV2) Unregister(reg *b2m.Registration, b *BrokerPeer) error {
	if len(reg.GroupName) == 0 {
		groupNames := b.GetGroupNames(reg.TopicName)
		topic := m.store.GetTopic(reg.TopicName)
		topic.UnregisterBroker(b.Str(), groupNames)
		b.UnregisterTopic(reg.TopicName)
		return nil
	}
	err := b.UnregisterGroup(reg.TopicName, reg.GroupName)
	if err != nil {
		if err == ErrNotRegisteredGroup {
			return nil
		}
		return err
	}
	topic := m.store.GetTopic(reg.TopicName)
	topic.UnregisterBrokerGroup(b.Str(), reg.GroupName)
	return nil
}

func (m *MonitorV2) ClearBrokerRegistration(b *BrokerPeer) {
	for topicName, groups := range b.topicGroups {
		topic := m.store.GetTopic(topicName)
		topic.UnregisterBroker(b.Str(), groups.GroupNames)
	}
	b.topicGroups = nil
}

func (m *MonitorV2) QueryTopicGroups(topicName string) []string {
	topic := m.store.GetTopic(topicName)
	return topic.GetGroupNames()
}

func (m *MonitorV2) LookupTopic(topicName, groupName string) TopicState {
	topic := m.store.GetTopic(topicName)
	topic.ReserveGroup(groupName)
	return topic.GetState()
}
