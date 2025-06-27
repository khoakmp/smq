package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"

	"github.com/google/uuid"
	"github.com/khoakmp/smq/broker/bfpool"
)

const (
	MessageIDLength int = 16
)

type MessageID [MessageIDLength]byte

// 1. test not make Message but do like this dung

func MakeMessage(id MessageID, payload []byte) *Message {
	//fmt.Println("Message Payload len:", len(payload))
	msg := GetMessageFromPool()
	msg.ID = id
	msg.Payload = payload
	return msg
}

type Message struct {
	ID         MessageID
	ConsumerID ClientID
	Timestamp  int
	Priority   int
	Index      int // index in infligtPQ
	Payload    []byte
	Attemps    uint16
}

func (m *Message) Reset() {
	m.ConsumerID = 0
	m.Timestamp = 0
	m.Priority = 0
	m.Payload = nil
	m.Attemps = 0
	m.Index = -1
}
func (m *Message) Str() string {
	// ID, Timestamp, Priority , Payload
	id, err := uuid.FromBytes(m.ID[:])
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("(ID:%s, Timestamp:%d, Priority:%d, Payload:\"%s\")", id.String(), m.Timestamp, m.Priority,
		string(m.Payload))
}

// only call clone at topic forward messages loop
func (m *Message) Clone() *Message {
	msg := GetMessageFromPool()
	msg.ID = m.ID
	msg.ConsumerID = m.ConsumerID
	msg.Timestamp = m.Timestamp
	payload := bfpool.GetByteSlice(uint32(len(m.Payload)))
	copy(payload, m.Payload)
	msg.Payload = payload
	msg.Priority = m.Priority
	msg.Index = m.Index
	msg.Attemps = 0
	return msg
	/* return &Message{
		ID:         m.ID,
		ConsumerID: m.ConsumerID,
		Timestamp:  m.Timestamp,
		Payload:    m.Payload,
		Priority:   m.Priority, // set later when start inflighting
		Index:      m.Index,
		Attemps:    0,
	} */
}

// Layout: ID (16 bytes) + Timestamp(8 bytes) + Priority (8 bytes)+ Attemps (2 bytes) + Payload(variable)
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	n := m.LenEncode()

	var buf [18]byte
	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint64(buf[8:], uint64(m.Priority))

	w.Write(m.ID[:])
	binary.BigEndian.PutUint16(buf[16:], uint16(m.Attemps))
	w.Write(buf[:])
	//binary.Write(w, binary.BigEndian, uint16(m.Attemps))

	w.Write(m.Payload)
	return int64(n), nil
}

// Not allocate new buffer for message.Payload
func (m *Message) Decode2(buf []byte) {
	offset := 0
	copy(m.ID[:], buf[:MessageIDLength])
	offset = MessageIDLength
	m.Timestamp = int(binary.BigEndian.Uint64(buf[offset : offset+8]))
	offset += 8
	m.Priority = int(binary.BigEndian.Uint64(buf[offset : offset+8]))
	offset += 8
	m.Attemps = binary.BigEndian.Uint16(buf[offset : offset+2])
	offset += 2
	m.Payload = buf[offset:]
}

func (m *Message) Decode(buf []byte) {
	offset := 0
	copy(m.ID[:], buf[:MessageIDLength])
	offset = MessageIDLength
	m.Timestamp = int(binary.BigEndian.Uint64(buf[offset : offset+8]))
	offset += 8
	m.Priority = int(binary.BigEndian.Uint64(buf[offset : offset+8]))
	offset += 8
	m.Attemps = binary.BigEndian.Uint16(buf[offset : offset+2])
	offset += 2

	payload := make([]byte, len(buf)-34)

	copy(payload, buf[offset:])
	m.Payload = payload
}

func persistMessage(q PersistentQueue, msg *Message) error {
	buffer := bytes.NewBuffer(nil)
	msg.WriteTo(buffer)
	if err := q.Put(buffer.Bytes()); err != nil {
		log.Printf("Failed to persist message, caused by %s\n", err.Error())
		return err
	}
	return nil
}

func (m *Message) LenEncode() int {
	return 32 + len(m.Payload) + 2
}
