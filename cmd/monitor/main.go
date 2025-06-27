package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"

	"github.com/khoakmp/smq/monitor"
)

var monitorTCPAddr string = ":5050"
var monitorHTTPAddr string = ":8080"

func RunMonitor() {
	m := monitor.Run(monitorTCPAddr, monitorHTTPAddr)
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
			m.Shutdown()
			return
		}
	}
}
func main() {
	RunMonitor()
}
