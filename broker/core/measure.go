package core

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Record struct {
	n  int32
	ts int64
}

type Measurer struct {
	startFlag             int32
	sendCounter           int32
	counter               int32
	firstRecvMsgTimestamp int64
	records               []Record
	lock                  sync.RWMutex
	writePQueueTopicCount int64
	writePQueueGroupCount int64
}

func NewMeasurer() *Measurer {
	records := make([]Record, 5)
	records[0].n = 100000
	records[1].n = 200000
	records[2].n = 250000
	records[3].n = 300000
	records[4].n = 500000
	/* records[2].n = 50000
	records[3].n = 100000
	records[4].n = 200000
	records[5].n = 500000
	records[6].n = 1000000 */
	// 10k, 20k, 50k, 100k, 200k, 500k, 1m
	return &Measurer{
		records: records,
	}
}

func (m *Measurer) IncrWritePQueueTopic() {
	atomic.AddInt64(&m.writePQueueTopicCount, 1)
}
func (m *Measurer) IncrWritePQueueGroup() {
	atomic.AddInt64(&m.writePQueueGroupCount, 1)
}

func (m *Measurer) TryRecordFirstRecvMsg() {
	if atomic.CompareAndSwapInt32(&m.startFlag, 0, 1) {
		m.firstRecvMsgTimestamp = time.Now().UnixNano()
	}
}

func (m *Measurer) CountRecordSend() {
	p := atomic.AddInt32(&m.sendCounter, 1)
	for idx, r := range m.records {
		if r.n == p {
			m.records[idx].ts = time.Now().UnixNano()
		}
	}
}

func (m *Measurer) CountAndRecordFinish() {
	p := atomic.AddInt32(&m.counter, 1)
	for i := 0; i < len(m.records); i++ {
		if m.records[i].n == p {
			m.lock.Lock()
			m.records[i].ts = time.Now().UnixNano()
			m.lock.Unlock()
		}
	}
}

func (m *Measurer) Print() {
	buffer := bytes.NewBuffer(nil)
	fmt.Fprintf(buffer, "Total Received Messages:%d\n", m.counter)
	fmt.Fprintf(buffer, "Timestamp Recv First Message:%d\n", m.firstRecvMsgTimestamp)
	fmt.Fprintf(buffer, "Write Topic Persistent Queue Count:%d\n", m.writePQueueTopicCount)
	fmt.Fprintf(buffer, "Write Group Persistent Queue Count:%d\n", m.writePQueueGroupCount)

	m.lock.RLock()
	for _, rec := range m.records {
		dur := time.Duration(rec.ts - m.firstRecvMsgTimestamp)
		fmt.Fprintf(buffer, "Duration Finish %d msgs: %s\n", rec.n, dur.String())
	}
	m.lock.RUnlock()
	fmt.Println(buffer.String())
}
func (m *Measurer) Reset() {
	m.startFlag = 0
	m.counter = 0
	m.firstRecvMsgTimestamp = 0
	m.writePQueueGroupCount = 0
	m.writePQueueTopicCount = 0
	for i := 0; i < len(m.records); i++ {
		m.records[i].ts = 0
	}
}
