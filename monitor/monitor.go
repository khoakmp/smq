package monitor

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

type Monitor struct {
	store   *ClusterStore
	brokers map[string]*BrokerPeer
	mutex   sync.RWMutex
}

func NewMonitor() *Monitor {
	return &Monitor{
		store:   NewClusterStore(),
		brokers: make(map[string]*BrokerPeer),
	}
}

// when call this function, it ensure that it does not change broker topics state anymore
func (m *Monitor) DeleteBrokerPeer(p *BrokerPeer) {
	for topicName, groupNames := range p.topicGroups {
		topic := m.store.GetTopic(topicName, true)
		topic.UnregisterBroker(p.Str(), groupNames.GroupNames)
	}
	m.mutex.Lock()
	delete(m.brokers, p.Str())
	m.mutex.Unlock()
}

func (m *Monitor) ListenBrokers() {
	l, err := net.Listen("tcp", ":5050")
	if err != nil {
		panic(err)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			// TODO: handle error here
			log.Println("failed to accept conn:", err)
			time.Sleep(time.Second)
			continue
		}
		brokerPeer := NewBrokerConn(conn.RemoteAddr().String(), conn, m.DeleteBrokerPeer)
		// TODO: add to m.brokers
		go IOLoop(brokerPeer, m)
	}
}

func (m *Monitor) RunHTTP() {
	//every consumer queries one time per minute
	// need query to lookup?topic =? dung di
	// dung la no se quan trong do nhung ma can lam gi de ma handle it nw dung
	// http://%s/lookup?topic=%s&group=%s
	mux := http.NewServeMux()
	mux.HandleFunc("/lookup", m.HandleLookupTopicGroup)
	mux.HandleFunc("/groups", m.HandleQueryGroups)
}

// lookup all broker serving topic and reserve the group (add to topic if not existed)
func (m *Monitor) HandleLookupTopicGroup(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	topicName, groupName := values.Get("topic"), values.Get("group")

	topic := m.store.GetTopic(topicName, true)
	topic.ReserveGroup(groupName)
	state := topic.GetState()
	topic.FreeKeepAlive()
	buf, _ := json.Marshal(state)
	w.Write(buf)
}

func (m *Monitor) HandleQueryGroups(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	topicName := values.Get("topic")
	topic := m.store.GetTopic(topicName, true)
	groups := Groups{
		GroupNames: topic.GetGroupNames(),
	}
	topic.FreeKeepAlive()

	buf, _ := json.Marshal(groups)
	w.Write(buf)
}
