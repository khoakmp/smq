package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/khoakmp/smq/cli"
	"github.com/khoakmp/smq/utils"
)

var defaultConsumerConfig = cli.ConsumerConfig{
	ConcurrentHandler:         1,
	BackOffTimeUnit:           time.Second,
	MaxBackoffCounter:         5,
	DefaultMaxInflight:        2048,
	FailedMsgCountToOpenCB:    3,
	SucceeddMsgCountToCloseCB: 3,
}

type MockMessageHandler struct {
	handledCount int64
	TraceID      string
	failedCount  int64
	succeedCount int64
}

func (m *MockMessageHandler) PrintState() {
	s := fmt.Sprintf("Total: %d, Succeed: %d, Failed: %d\n", m.handledCount, m.succeedCount, m.failedCount)
	fmt.Println(s)
}

var ErrBadMessage = errors.New("bad message")

func (m *MockMessageHandler) Handle(msg *cli.Message) error {
	atomic.AddInt64(&m.handledCount, 1)
	atomic.AddInt64(&m.succeedCount, 1)
	return nil
}

func RunConsumer() {
	// 3 args
	topicName, groupName := os.Args[1], os.Args[2]

	traceID := utils.RandString(6)
	/* f, err := os.Create(fmt.Sprintf("pprof/cons-%s", traceID))
	if err != nil {
		fmt.Println(err)
		return
	}

	if err := pprof.StartCPUProfile(f); err != nil {
		fmt.Println("failed to start cpu profile", err)
		return
	}
	defer pprof.StopCPUProfile() */

	msgHandler := &MockMessageHandler{
		handledCount: 0,
		TraceID:      traceID,
	}
	csm := cli.NewConsumerDebug(topicName, groupName, msgHandler, &defaultConsumerConfig, traceID)
	csm.SetMaxAttemp(1)

	input := bufio.NewReader(os.Stdin)
	wrongFormat := func() {
		fmt.Println("wrong format input")
	}

	for {
		line, _, err := input.ReadLine()
		if err != nil {
			fmt.Println(err)
			return
		}

		arr := bytes.Split(line, []byte(" "))
		switch {
		case bytes.Equal(arr[0], []byte("s")):
			csm.Stop()
		case bytes.Equal(arr[0], []byte("q")):
			csm.Stop()
			fmt.Println("Wait for stop...")
			time.Sleep(time.Second * 3)
			return
			// TODO: add queries
		case bytes.Equal(arr[0], []byte("conn-b")):
			if len(arr) < 2 {
				wrongFormat()
				continue
			}

			tcpAddr := string(arr[1])
			err := csm.ConnectBroker(tcpAddr)
			if err != nil {
				fmt.Println("FAILED connect", err)
			} else {
				fmt.Println("SUCCEED connect")
			}

		case bytes.Equal(arr[0], []byte("hstate")):
			msgHandler.PrintState()
		case bytes.Equal(arr[0], []byte("mem")):
			var memstats runtime.MemStats
			runtime.ReadMemStats(&memstats)
			fmt.Println("AllocHeap:", memstats.HeapAlloc, "TotalAlloc:", memstats.TotalAlloc)
			fmt.Println("NumGC:", memstats.NumGC)
		}
	}
}

func main() {
	RunConsumer()
}
