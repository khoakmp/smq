package cli

const (
	StateMsgFree       = 0
	StateMsgInHandling = 1
	StateMsgSucceed    = 2
	StateMsgFailed     = 3
)

type Message struct {
	ID        [16]byte
	Payload   []byte
	Timestamp int64
	conn      *Conn
	Attemps   uint16
	State     int // 0: InProcess/ 1: Succeed / 2: Failed
}

func (m *Message) Reset() {
	m.Payload = nil
	m.Timestamp = 0
	m.Attemps = 0
	m.conn = nil
	m.State = StateMsgFree
}

func (m *Message) Finish() {
	m.State = StateMsgSucceed
	m.conn.msgRespChan <- m
}

func (m *Message) Requeue() {
	m.State = StateMsgFailed
	m.conn.msgRespChan <- m
}
