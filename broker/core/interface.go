package core

type ConsumerGroupMember interface {
	ID() ClientID
	TriggerUpdateState()
	Close()
}

type PersistentQueue interface {
	Put([]byte) error
	ReadChan() <-chan []byte
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}
