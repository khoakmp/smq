package b2m

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

const (
	CmdTypeSpec uint8 = iota
	CmdTypeRegister
	CmdTypeUnRegister
)

type BrokerSpecification struct {
	BroadcastTCPPort  uint16 // consumer or producer connect to this port to interact with broker
	BroadcastHTTPPort uint16
}

// should change to marshal/unmarshal instead of encode/decode

func (s *BrokerSpecification) Encode() []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint16(buf[:2], s.BroadcastTCPPort)
	binary.BigEndian.PutUint16(buf[2:], s.BroadcastHTTPPort)
	return buf
}

func (s *BrokerSpecification) Decode(buf []byte) error {
	s.BroadcastTCPPort = binary.BigEndian.Uint16(buf[:2])
	s.BroadcastHTTPPort = binary.BigEndian.Uint16(buf[2:])

	return nil
}

type Registration struct {
	TopicName string
	GroupName string
}

func (r *Registration) Encode() []byte {
	buf := make([]byte, len(r.TopicName)+1+len(r.GroupName))
	offset := len(r.TopicName)
	copy(buf[:offset], []byte(r.TopicName))
	buf[offset] = ' '
	if len(r.GroupName) > 0 {
		copy(buf[offset+1:], []byte(r.GroupName))
	}
	return buf
}

func (r *Registration) Decode(buf []byte) error {
	idx := bytes.Index(buf, []byte(" "))
	r.TopicName = string(buf[:idx])
	if idx != len(buf)-1 {
		r.GroupName = string(buf[idx+1:])
	} else {
		r.GroupName = ""
	}
	return nil
}

type TopicMetadata struct {
	TopicName      string   `json:"topic_name"`
	ConsumerGroups []string `json:"groups"`
}

type BrokerStateMetadata struct {
	Topics []TopicMetadata `json:"topics"`
}

func (s *BrokerStateMetadata) Encode() []byte {
	buf, _ := json.Marshal(s)
	return buf
}

func (s *BrokerStateMetadata) Decode(buf []byte) {
	json.Unmarshal(buf, s)
}
