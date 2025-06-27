package cli

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/khoakmp/smq/broker"
)

func TestProducer(t *testing.T) {
	b := broker.New("SMQB-1", []string{}, []string{})

	go b.RunTCP(":5000")
	time.Sleep(time.Millisecond)
	p := NewProducer(":5000")
	tx, err := p.Produce("t1#tmp", []byte("hey"))
	if err != nil {
		fmt.Println("Failed to produce msg:", err)
		return
	}
	err = tx.Error()
	fmt.Println(err)
	b.Stop()
	time.Sleep(time.Second)
}
func TestCloseConn(t *testing.T) {
	go func() {
		l, _ := net.Listen("tcp", ":5001")
		c, _ := l.Accept()
		c.Close()
		err := c.Close()
		fmt.Println("at l:", err)
	}()

	time.Sleep(time.Millisecond)
	c, _ := net.Dial("tcp", ":5001")
	c.Close()
	c.Close()
	time.Sleep(time.Second)
}
