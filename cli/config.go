package cli

import "time"

type ConsumerConfig struct {
	ConcurrentHandler         int // default 1
	BackOffTimeUnit           time.Duration
	MaxBackoffCounter         int32
	DefaultMaxInflight        int32 // default 1024
	FailedMsgCountToOpenCB    int
	SucceeddMsgCountToCloseCB int
}
