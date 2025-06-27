package monitor

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/khoakmp/smq/api"
	"github.com/khoakmp/smq/api/b2m"
	"github.com/stretchr/testify/assert"
)

func TestLogicService(t *testing.T) {
	m := NewMonitorV2()
	b, err := m.AddBroker("localhost:5010", nil)
	assert.Equal(t, nil, err)
	topicName1 := "topic-1#tmp"
	//groupName1 := "group-1#tmp"
	b.specify(b2m.BrokerSpecification{
		BroadcastTCPPort:  5000,
		BroadcastHTTPPort: 8000,
	})

	m.Register(&b2m.Registration{
		TopicName: topicName1,
		GroupName: "",
	}, b)
	assert.Equal(t, 1, len(b.topicGroups))

	topic := m.store.GetTopic(topicName1)
	assert.Equal(t, topicName1, topic.Name)
	assert.Equal(t, 0, len(topic.ConsumerGroups))
	assert.Equal(t, 1, len(topic.Brokers))
	netInfo := topic.Brokers[b.Str()]

	assert.Equal(t, uint16(5000), netInfo.BroadcastTCPPort)
	assert.Equal(t, uint16(8000), netInfo.BroadcastHTTPPort)
	assert.Equal(t, "localhost", netInfo.Hostname)

	groupName1 := "group-1#tmp"

	m.Register(&b2m.Registration{
		TopicName: topicName1,
		GroupName: groupName1,
	}, b)

	bTopic1 := b.topicGroups[topicName1]
	assert.Equal(t, groupName1, bTopic1.GroupNames[0])
	assert.Equal(t, 1, len(topic.ConsumerGroups))
	// Test query topic groups
	groups := m.QueryTopicGroups(topicName1)
	assert.Equal(t, 1, len(groups))
	assert.Equal(t, groupName1, groups[0])

	// Test check topic state
	topicState := m.LookupTopic(topicName1, groupName1)
	assert.Equal(t, 1, len(topicState.Brokers))
	assert.Equal(t, *b.netInfor, topicState.Brokers[0])

	assert.Equal(t, 1, len(topicState.GroupNames))
	assert.Equal(t, groupName1, topicState.GroupNames[0])
	// create philosophy design first
	// Test Unregister Group
	m.Unregister(&b2m.Registration{
		TopicName: topicName1,
		GroupName: groupName1,
	}, b)

	assert.Equal(t, 0, len(b.topicGroups[topicName1].GroupNames))
	assert.Equal(t, 1, len(topic.Brokers))
	assert.Equal(t, 0, topic.ConsumerGroups[groupName1])
	_, ok := topic.ConsumerGroups[groupName1]
	assert.Equal(t, true, ok)

	b2, err := m.AddBroker("localhost:5011", nil)
	assert.Equal(t, nil, err)

	b2.specify(b2m.BrokerSpecification{
		BroadcastTCPPort:  5001,
		BroadcastHTTPPort: 8001,
	})

	assert.Equal(t, "localhost", b2.netInfor.Hostname)
	assert.Equal(t, 2, len(m.brokers))

	err = m.Register(&b2m.Registration{
		TopicName: topicName1,
		GroupName: "",
	}, b2)
	assert.Equal(t, nil, err)
	assert.Equal(t, *b2.netInfor, topic.Brokers[b2.addr])
	assert.Equal(t, 2, len(topic.Brokers))

	// unregister b1 from topic 1
	m.Unregister(&b2m.Registration{
		TopicName: topicName1,
		GroupName: "",
	}, b)

	assert.Equal(t, 1, len(topic.Brokers))
	assert.Equal(t, 1, len(topic.ConsumerGroups))
	assert.Equal(t, 0, len(b.topicGroups))

	// currently, there are no broker serve group-1
	assert.Equal(t, 0, topic.ConsumerGroups[groupName1])
	m.Register(&b2m.Registration{
		TopicName: topicName1,
		GroupName: groupName1,
	}, b2)
	assert.Equal(t, 1, len(b2.topicGroups[topicName1].GroupNames))
	assert.Equal(t, 1, topic.ConsumerGroups[groupName1])
}

func TestHandleCommand(t *testing.T) {
	m := NewMonitorV2()
	b, err := m.AddBroker("localhost:5010", nil)
	assert.Equal(t, nil, err)
	b.specify(b2m.BrokerSpecification{
		BroadcastTCPPort:  5000,
		BroadcastHTTPPort: 8000,
	})

	topicName1 := "topic-1#tmp"
	groupName1 := "group-1#tmp"
	go m.HandleLoop()

	cmd := NewCommand(TypeRegister, &b2m.Registration{
		TopicName: topicName1,
		GroupName: groupName1,
	}, b, b.cmdResponseChan)

	m.HandleCommand(cmd)
	resp := <-b.cmdResponseChan
	// playing with network programming is much harder ?

	assert.Equal(t, ErrTopicUnregistered, resp.err)
	assert.Equal(t, 0, len(b.topicGroups))
	assert.Equal(t, 0, len(m.store.topics))

	cmd = NewCommand(TypeRegister, &b2m.Registration{
		TopicName: topicName1,
		GroupName: "",
	}, b, b.cmdResponseChan)
	m.HandleCommand(cmd)
	resp = <-b.cmdResponseChan
	assert.Equal(t, nil, resp.err)
	assert.Equal(t, OK, resp.resp)
	assert.Equal(t, 1, len(b.topicGroups))
	topic := m.store.GetTopic(topicName1)
	assert.Equal(t, topicName1, topic.Name)
	assert.Equal(t, 0, len(topic.ConsumerGroups))
	assert.Equal(t, 1, len(topic.Brokers))
}

type encoder interface {
	Encode() []byte
}

func TestIO(t *testing.T) {
	m := NewMonitorV2()

	go func() {
		m.ListenBrokers(":5050")
	}()
	go m.HandleLoop()

	time.Sleep(time.Millisecond)
	c, err := net.Dial("tcp", ":5050")
	if err != nil {
		panic(err)
	}
	fmt.Println("connect successfully")

	spec := b2m.BrokerSpecification{
		BroadcastTCPPort:  5000,
		BroadcastHTTPPort: 8000,
	}

	var buffer = bytes.NewBuffer(nil)

	writeCmd := func(cmdType byte, payload encoder) error {
		buffer.Reset()
		buffer.WriteByte(cmdType)
		buf := payload.Encode()
		binary.Write(buffer, binary.BigEndian, uint32(len(buf)))
		buffer.Write(buf)
		_, err := c.Write(buffer.Bytes())
		return err
	}

	err = writeCmd(b2m.CmdTypeSpec, &spec)
	assert.Equal(t, nil, err)
	var connReader = bufio.NewReader(c)

	readResp := func() (byte, error) {
		var sz uint32
		resptype, err := connReader.ReadByte()
		if err != nil {
			return 0, err
		}

		binary.Read(connReader, binary.BigEndian, &sz)
		_, err = connReader.Discard(int(sz))
		if err != nil {
			fmt.Println(err)
		}
		return resptype, nil
	}

	frameType, err := readResp()

	assert.Equal(t, nil, err)
	assert.Equal(t, api.FrameResponse, frameType)
	fmt.Println("complete spec")

	brokerID := c.LocalAddr().String()
	b, contained := m.brokers[brokerID]
	assert.Equal(t, true, contained)
	assert.Equal(t, b.netInfor.BroadcastHTTPPort, uint16(8000))

	topicName1 := "topic-1#tmp"
	//groupName1 := "group-1#tmp"

	reg := b2m.Registration{
		TopicName: topicName1,
		GroupName: "",
	}

	err = writeCmd(b2m.CmdTypeRegister, &reg)

	assert.Equal(t, nil, err)

	frameType, err = readResp()
	// focusing on low-level code only dung

	assert.Equal(t, nil, err)
	assert.Equal(t, api.FrameResponse, frameType)
	c.Close()
	b.wg.Wait()
	time.Sleep(time.Millisecond)
	assert.Equal(t, 0, len(b.topicGroups))
	topic := m.store.GetTopic(topicName1)

	assert.Equal(t, 0, len(topic.Brokers))

}
func TestClearBrokerRegistration(t *testing.T) {
	m := NewMonitorV2()
	b, _ := m.AddBroker("localhost:5013", nil)
	b.specify(b2m.BrokerSpecification{
		BroadcastTCPPort:  5010,
		BroadcastHTTPPort: 8010,
	})
	topicName := "tp"
	m.Register(&b2m.Registration{
		TopicName: topicName,
		GroupName: "",
	}, b)
	m.Register(&b2m.Registration{
		TopicName: topicName,
		GroupName: "g1",
	}, b)
	m.ClearBrokerRegistration(b)
	topic := m.store.GetTopic(topicName)

	assert.Equal(t, 0, len(topic.Brokers))
	assert.Equal(t, 1, len(topic.ConsumerGroups))
	assert.Equal(t, 0, len(b.topicGroups))

}
func TestReadRequest(t *testing.T) {

	var buffer = bytes.NewBuffer(nil)
	writeCmd := func(cmdType byte, payload encoder) {
		buffer.Reset()
		buffer.WriteByte(cmdType)
		buf := payload.Encode()
		binary.Write(buffer, binary.BigEndian, uint32(len(buf)))
		buffer.Write(buf)
		//_, err := c.Write(buffer.Bytes())
	}

	writeCmd(b2m.CmdTypeRegister, &b2m.Registration{
		TopicName: "topic",
		GroupName: "group",
	})
	rtype, p, err := readRequest(buffer)
	assert.Equal(t, nil, err)
	fmt.Println(string(p))
	fmt.Println("type:", rtype)
}

func TestHTTP(t *testing.T) {
	Run(":5050", ":8080")
	time.Sleep(time.Millisecond)
	resp, err := http.DefaultClient.Get("http://localhost:8080/query_groups?topic=topic1")
	assert.Equal(t, nil, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close() // free this connection, return it to client.ConnPool maybe
	assert.Equal(t, nil, err)
	fmt.Println(string(body))
	var p TopicState
	json.Unmarshal(body, &p)
	fmt.Println(p)
}
