package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/khoakmp/smq/broker"
	"github.com/khoakmp/smq/broker/bfpool"
	"github.com/khoakmp/smq/broker/cli"
	"github.com/khoakmp/smq/broker/core"
)

var monitorTCPAddr string = ":5050"
var monitorHTTPAddr string = ":8080"

func publish(topicName string, msgBody []byte, b *core.BrokerBase) error {
	msg := core.MakeMessage(core.MessageID(uuid.New()), msgBody)
	topic := b.FindTopic(topicName)
	return topic.PutMessage(msg)
}

func RunBroker() {
	tcpAddr := os.Args[1]
	id := os.Args[2]

	/* f, err := os.Create(fmt.Sprintf("pprof/cpu-%s", id))
	if err != nil {
		fmt.Println(err)
		return
	}

	//runtime.GOMAXPROCS(2)
	err = pprof.StartCPUProfile(f)
	if err != nil {
		fmt.Println("failed to start profiling")
		return
	}

	defer pprof.StopCPUProfile()
	// with 25bytes/msg => it use */
	msgPattern := "message pattern here sssssssssssssssssssssssssssss"

	/* file, err := os.OpenFile("/dev/pts/4", os.O_WRONLY, 0600)
	if err != nil {
		fmt.Println("Failed to open file:", err)
		return
	}
	log.SetOutput(file) */
	broker := broker.New(id, []string{monitorTCPAddr}, []string{monitorHTTPAddr})
	go broker.RunTCP(tcpAddr)
	input := bufio.NewReader(os.Stdin)
	// SMQB SMQM SMQ-Consumer SMQ-Producer dung?

	for {
		line, _, err := input.ReadLine()
		if err != nil {
			fmt.Println(err)
			return
		}

		arr := bytes.Split(line, []byte(" "))
		switch {
		case bytes.Equal(arr[0], []byte("q")):
			broker.Stop()
			fmt.Println("Wait to completely stop")
			time.Sleep(time.Second * 3)
			return
		case bytes.Equal(arr[0], []byte("pub")):
			if len(arr) < 3 {
				fmt.Println("Wrong format input")
				continue
			}

			topicName := string(arr[1])
			msgBody := arr[2]
			err := publish(topicName, msgBody, broker.GetBrokerBase())
			if err != nil {
				fmt.Printf("[Topic %s] Failed to put msg, err:%s\n", topicName, err.Error())
			} else {
				fmt.Printf("[Topic %s] SUCCESS put msg\n", topicName)
			}
		case bytes.Equal(arr[0], []byte("measure")):
			if len(arr) < 2 {
				fmt.Println("Wrong format input")
				continue
			}
			topicName := string(arr[1])
			topic := broker.GetBrokerBase().FindTopic(topicName)
			println("topic.Measurer:", topic.Measurer)
			topic.Measurer.Print()
			// broker.GetBrokerBase().Measurer.Print()

		case bytes.Equal(arr[0], []byte("rsm")):
			if len(arr) < 2 {
				fmt.Println("Wrong format input")
				continue
			}
			topicName := string(arr[1])
			topic := broker.GetBrokerBase().FindTopic(topicName)
			topic.Measurer.Reset()

		case bytes.Equal(arr[0], []byte("mem")):
			var memstat runtime.MemStats
			runtime.ReadMemStats(&memstat)
			fmt.Println("Heap Alloc:", memstat.HeapAlloc, "TotalAlloc:", memstat.TotalAlloc)
			fmt.Println("NumGC:", memstat.NumGC)

		case bytes.Equal(arr[0], []byte("pubm")):
			topicName := string(arr[1])
			if len(arr) < 3 {
				fmt.Println("Wrong format input")
				continue
			}
			n, err := strconv.Atoi(string(arr[2]))
			if err != nil {
				fmt.Println(err)
				continue
			}

			for range n {
				buf := bfpool.GetByteSlice(uint32(len(msgPattern)))
				copy(buf, []byte(msgPattern))
				cli.HandlePublish(nil, broker.GetBrokerBase(), topicName, buf)
			}

		case bytes.Equal(arr[0], []byte("msg-stats")):
			core.PrintMessageAllocStats()
		case bytes.Equal(arr[0], []byte("bs-stats")):
			bfpool.PrintBsPoolStats()
		}

	}
}

func main() {
	RunBroker()
}
