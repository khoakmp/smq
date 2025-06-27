package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"

	"github.com/khoakmp/smq/cli"
)

func RunProducer() {
	brokerTCPAddr := os.Args[1]
	p := cli.NewProducer(brokerTCPAddr)
	// producer lazily initial connection, it only connects to broker on the first call produce message
	msgBodyPattern := []byte("this is pattern of message body")

	input := bufio.NewReader(os.Stdin)
	for {
		line, _, err := input.ReadLine()
		if err != nil {
			fmt.Println(err)
			return
		}

		arr := bytes.Split(line, []byte(" "))
		switch {
		case bytes.Equal(arr[0], []byte("q")):
			p.CloseWait()
			return

		case bytes.Equal(arr[0], []byte("pub")):
			if len(arr) < 3 {
				fmt.Println("Wrong format input")
				continue
			}

			topicName := string(arr[1])
			msgBody := arr[2]
			tx, err := p.Produce(topicName, msgBody)
			if err != nil {
				fmt.Println("Failed to Write message to internal SND_BUF:", err)
				continue
			}

			if err = tx.Error(); err != nil {
				fmt.Println("Failed to push message to broker:", err)
			}

		case bytes.Equal(arr[0], []byte("pubm")):
			if len(arr) < 3 {
				fmt.Println("Wrong format input")
				continue
			}
			topicName := string(arr[1])
			n, err := strconv.Atoi(string(arr[2]))
			if err != nil {
				fmt.Println(err)
				continue
			}

			p.Measurer.Set(int64(n))
			//cnt := 0
			if len(arr) == 3 {
				for range n {
					p.Produce(topicName, msgBodyPattern)
					/* cnt++
					if cnt%30000 == 0 {
						time.Sleep(time.Millisecond * 2)
					} */
				}
				p.Flush()
			}
		case bytes.Equal(arr[0], []byte("stats")):
			p.PrintStats()
		case bytes.Equal(arr[0], []byte("measure")):
			p.Measurer.Print()
		case bytes.Equal(arr[0], []byte("mem")):
			var memstats runtime.MemStats
			runtime.ReadMemStats(&memstats)
			fmt.Println("AllocHeap:", memstats.HeapAlloc, "Total Alloc:", memstats.TotalAlloc)
			fmt.Println("NumGC:", memstats.NumGC)
		case bytes.Equal(arr[0], []byte("gc")):
			runtime.GC()
			var memstats runtime.MemStats
			runtime.ReadMemStats(&memstats)
			fmt.Println("AllocHeap:", memstats.HeapAlloc, "Total Alloc:", memstats.TotalAlloc)
			fmt.Println("NumGC:", memstats.NumGC)
		}
	}
}

func main() {
	RunProducer()
}
