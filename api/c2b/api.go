package c2b

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

const (
	KeySpec uint8 = iota
	KeySubscribe
	KeyACK
	KeyRequeue
	KeyReadyCount
	KeyClose
	KeyPublish
	KeyPublishMulti
)

type Payload interface {
	WriteTo(io.Writer) (int64, error)
	Decode([]byte) error
}

// used for both producer and consumer, first frame send to broker after establish connection
type PayloadClientSpec struct {
	RecvWindow           uint32 // number of messages that broker can push but not recv ACK yet
	PushMessagesInterval uint64
	TraceID              string
	// add other fields later
}

var ErrParseFailed = errors.New("parse failed")

func (p *PayloadClientSpec) Encode() []byte {
	n := 4 + 8 + len(p.TraceID)
	buf := make([]byte, n)
	binary.BigEndian.PutUint32(buf[:4], p.RecvWindow)
	binary.BigEndian.PutUint64(buf[4:12], p.PushMessagesInterval)
	copy(buf[12:], []byte(p.TraceID))
	return buf
}

func (p *PayloadClientSpec) Decode(buf []byte) {
	p.RecvWindow = binary.BigEndian.Uint32(buf[:4])
	p.PushMessagesInterval = binary.BigEndian.Uint64(buf[4:12])
	p.TraceID = string(buf[12:])
}

type PayloadSubscribe struct {
	TopicName string `json:"topic_name"`
	GroupName string `json:"group_name"`
}

// write size first
func (p *PayloadSubscribe) WriteWithSizeTo(w io.Writer) error {
	var bufSize [4]byte
	binary.BigEndian.PutUint32(bufSize[:], p.Size())
	w.Write(bufSize[:])
	_, err := p.WriteTo(w)
	if err != nil {
		return err
	}

	return nil
}

func (p *PayloadSubscribe) Size() uint32 {
	return uint32(len(p.TopicName) + len(p.GroupName) + 1)
}

func (p *PayloadSubscribe) WriteTo(w io.Writer) (int64, error) {
	l := p.Size()
	w.Write([]byte(p.TopicName))
	w.Write([]byte(" "))
	_, err := w.Write([]byte(p.GroupName))
	if err != nil {
		return 0, err
	}
	return int64(l), nil
}

func (p *PayloadSubscribe) Encode() []byte {
	buffer := bytes.NewBuffer(nil)
	buffer.Grow(int(p.Size()))
	p.WriteTo(buffer)
	return buffer.Bytes()
}

func (p *PayloadSubscribe) Decode(buf []byte) error {
	arr := bytes.Split(buf, []byte(" "))
	if len(arr) != 2 {
		return ErrParseFailed
	}
	p.TopicName = string(arr[0])
	p.GroupName = string(arr[1])
	return nil
}

type PayloadPublishMessage struct {
	TopicName   string
	MessageBody []byte
}

func (p *PayloadPublishMessage) WriteTo(w io.Writer) (int64, error) {
	w.Write([]byte(p.TopicName))
	w.Write([]byte(" "))
	_, err := w.Write(p.MessageBody)
	if err != nil {
		return 0, err
	}
	return int64(len(p.TopicName) + len(p.MessageBody) + 1), nil
}

// Caution: p.MessageBody is not allocated new buffer, it only point to buf[idx:]
func (p *PayloadPublishMessage) Decode(buf []byte) error {
	idx := bytes.IndexByte(buf, ' ')
	if idx == -1 {
		return ErrParseFailed
	}
	p.TopicName = string(buf[:idx])
	//p.MessageBody = make([]byte, len(buf)-idx-1)
	//copy(p.MessageBody, buf[idx+1:])
	p.MessageBody = buf[idx+1:]

	return nil
}

func (p *PayloadPublishMessage) Size() uint32 {
	return uint32(len(p.MessageBody) + 1 + len(p.TopicName))
}

func (p *PayloadPublishMessage) Encode() []byte {
	buffer := bytes.NewBuffer(nil)
	buffer.Grow(int(p.Size()))
	p.WriteTo(buffer)
	return buffer.Bytes()
}

type PayloadReadyCount struct {
	ReadyCount int32
}

type PayloadPublishMulti struct {
	TopicName string
	Messages  [][]byte
}

func (p *PayloadPublishMulti) Encode() []byte {
	sz := 4 + len(p.TopicName)
	for i := range len(p.Messages) {
		sz += len(p.Messages[i]) + 4
	}
	buffer := bytes.NewBuffer(nil)
	buffer.Grow(sz)
	binary.Write(buffer, binary.BigEndian, uint32(len(p.TopicName)))
	buffer.WriteString(p.TopicName)
	for i := range len(p.Messages) {
		binary.Write(buffer, binary.BigEndian, uint32(len(p.Messages[i])))
		buffer.Write(p.Messages[i])
	}
	return buffer.Bytes()
}

func (p *PayloadPublishMulti) Decode(buf []byte) error {
	idx := bytes.IndexByte(buf, ' ')
	if idx == -1 {
		return ErrParseFailed
	}
	p.TopicName = string(buf[:idx])
	offset := idx + 1
	for offset < len(buf) {
		var sz uint32 = binary.BigEndian.Uint32(buf[offset : offset+4])
		offset += 4
		p.Messages = append(p.Messages, buf[offset:offset+int(sz)])
		offset += int(sz)
	}
	return nil
}

func (p *PayloadPublishMulti) DecodeV2(buf []byte, getByteSlice func(n uint32) []byte) error {
	idx := bytes.IndexByte(buf, ' ')
	if idx == -1 {
		return ErrParseFailed
	}
	p.TopicName = string(buf[:idx])
	offset := idx + 1

	for offset < len(buf) {
		var sz uint32 = binary.BigEndian.Uint32(buf[offset : offset+4])
		offset += 4
		msg := getByteSlice(sz)
		copy(msg, buf[offset:offset+int(sz)])
		p.Messages = append(p.Messages, msg)
		offset += int(sz)
	}
	return nil
}
