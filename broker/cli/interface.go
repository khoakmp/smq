package cli

import "github.com/khoakmp/smq/broker/core"

type Server interface {
	IOLoop(client *Client, state *core.BrokerBase)
	SendMessage(client *Client, msg *core.Message) error
}
