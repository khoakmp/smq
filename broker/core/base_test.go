package core

import "time"

func genMessage(id MessageID, priority int, delay time.Duration) *Message {
	return &Message{
		ID:         id,
		ConsumerID: 1,
		Timestamp:  time.Now().Add(delay).Nanosecond(),
		Priority:   priority,
		Payload:    []byte("payload"),
	}
}
