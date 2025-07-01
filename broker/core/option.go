package core

import "time"

type Options struct {
	BroadcastHTTPPort            uint16
	InMemQueueSize               int
	MetadataFilename             string // both path+name
	ScanQueueRecalWorkerInterval time.Duration
	ScanQueueInterval            time.Duration
	SampleGroupsCount            int
	BroadcastTCPPort             uint16
	EnableFileQueue              bool
	FileQueueDir                 string
}

var opts Options

func init() {
	opts.BroadcastHTTPPort = 8000
	opts.BroadcastTCPPort = 5000
	opts.InMemQueueSize = 10000
	opts.MetadataFilename = "data/meta"
	opts.ScanQueueRecalWorkerInterval = time.Second * 2
	opts.ScanQueueInterval = time.Second
	opts.SampleGroupsCount = 10
	opts.EnableFileQueue = true
	opts.FileQueueDir = "data"
}
