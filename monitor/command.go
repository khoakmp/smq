package monitor

const (
	TypeRegister uint8 = iota
	TypeUnRegister
	TypeClearBrokerRegistration
	TypeQueryTopicGroups
	TypeLookupTopic // return all groups, and broker netinfors serve topic
)

type Command struct {
	Type     byte
	Payload  interface{}
	broker   *BrokerPeer // it can be nil
	respChan chan *CommandResp
}

func NewCommand(cmdType byte, payload interface{}, broker *BrokerPeer, respChan chan *CommandResp) *Command {
	return &Command{
		broker:   broker,
		respChan: respChan,
		Type:     cmdType,
		Payload:  payload,
	}
}

func (cmd *Command) Response(resp []byte, err error) {
	var exitChan chan struct{} = nil
	if cmd.broker != nil {
		exitChan = cmd.broker.exitChan
	}
	select {
	case <-exitChan:
	case cmd.respChan <- &CommandResp{
		resp: resp,
		err:  err,
	}:
	}
}

type LookupTopicPayload struct {
	TopicName string
	GroupName string
}
