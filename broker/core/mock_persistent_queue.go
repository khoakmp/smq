package core

import "errors"

type MockPersistentQueue struct{}

/*
Put([]byte) error

	ReadChan() <-chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
*/
var ErrTooManyMessages = errors.New("too many messages")

func (m *MockPersistentQueue) Put([]byte) error        { return ErrTooManyMessages }
func (m *MockPersistentQueue) ReadChan() <-chan []byte { return nil }
func (m *MockPersistentQueue) Close() error            { return nil }
func (m *MockPersistentQueue) Delete() error           { return nil }
func (m *MockPersistentQueue) Depth() int64            { return 0 }
func (m *MockPersistentQueue) Empty() error            { return nil }
