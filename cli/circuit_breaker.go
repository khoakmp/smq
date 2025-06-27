package cli

import (
	"errors"
	"sync"
)

const (
	CBStateClose int32 = iota
	CBStateOpen
	CBStateHalfOpen
)

type CircuitBreaker struct {
	counts               cbCounts
	generation           uint64
	mutex                sync.Mutex
	state                int32
	triggerUpdateBackoff func(setBackoff int32) // set or clear
	config               CircuitBreakerConfig
}

type CircuitBreakerConfig struct {
	FailedMsgCountOpenCB      int
	SucceeddMsgCountToCloseCB int
}

func NewCircuitBreaker(updateBackoff func(setBackoff int32), cfg CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		counts:               cbCounts{},
		generation:           0,
		state:                CBStateClose,
		triggerUpdateBackoff: updateBackoff,
		config:               cfg,
	}
}

type cbCounts struct {
	consecutiveSuccess int
	consecutiveFail    int
}

func (c *cbCounts) clear() {
	c.consecutiveFail = 0
	c.consecutiveSuccess = 0
}

var ErrInStateOpen = errors.New("in open state")

func (cb *CircuitBreaker) preProcess() (uint64, error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()
	if cb.state == CBStateOpen {
		return cb.generation, ErrInStateOpen
	}
	// not prevent handle when state is halfOpen
	return cb.generation, nil
}

func (cb *CircuitBreaker) postProcess(preGeneration uint64, success bool) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	if cb.generation != preGeneration {
		return
	}

	if success {
		cb.counts.consecutiveSuccess++
		cb.counts.consecutiveFail = 0
		// TODO: replace 5 by const
		if cb.state == CBStateHalfOpen && cb.counts.consecutiveSuccess >= cb.config.SucceeddMsgCountToCloseCB {
			cb.setState(CBStateClose)
			cb.triggerUpdateBackoff(0)
		}
		return
	}
	cb.counts.consecutiveFail++
	cb.counts.consecutiveSuccess = 0

	switch cb.state {
	case CBStateHalfOpen:
		cb.setState(CBStateOpen)
	case CBStateClose:
		if cb.counts.consecutiveFail >= cb.config.FailedMsgCountOpenCB {
			cb.setState(CBStateOpen)
		}
	}

	if cb.state == CBStateOpen {
		cb.triggerUpdateBackoff(1)
	}
}

func (cb *CircuitBreaker) setState(state int32) {
	if cb.state == state {
		return
	}
	cb.nextGeneration()
	cb.state = state
}

func (cb *CircuitBreaker) TriggerHalfOpen() {
	cb.mutex.Lock()
	cb.setState(CBStateHalfOpen)
	cb.mutex.Unlock()
}

// must acquire lock in caller function
func (cb *CircuitBreaker) nextGeneration() {
	cb.generation++
	cb.counts.clear()
}
