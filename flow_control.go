package bunshin

import (
	"sync"
	"time"
)

const defaultFlowControlReceiverTimeout = 5 * time.Second

type FlowControlStatus struct {
	ReceiverID   string
	Position     int64
	WindowLength int
	ObservedAt   time.Time
}

type FlowControlStrategy interface {
	InitialLimit(termBufferLength int) int64
	OnStatus(status FlowControlStatus, senderLimit int64) int64
	OnIdle(now time.Time, senderLimit, senderPosition int64) int64
}

type UnicastFlowControl struct{}

func (UnicastFlowControl) InitialLimit(termBufferLength int) int64 {
	return int64(termBufferLength)
}

func (UnicastFlowControl) OnStatus(status FlowControlStatus, senderLimit int64) int64 {
	return maxInt64(senderLimit, receiverRightEdge(status))
}

func (UnicastFlowControl) OnIdle(_ time.Time, senderLimit, _ int64) int64 {
	return senderLimit
}

type MaxMulticastFlowControl struct{}

func (MaxMulticastFlowControl) InitialLimit(termBufferLength int) int64 {
	return int64(termBufferLength)
}

func (MaxMulticastFlowControl) OnStatus(status FlowControlStatus, senderLimit int64) int64 {
	return maxInt64(senderLimit, receiverRightEdge(status))
}

func (MaxMulticastFlowControl) OnIdle(_ time.Time, senderLimit, _ int64) int64 {
	return senderLimit
}

type MinMulticastFlowControl struct {
	ReceiverTimeout time.Duration

	mu        sync.Mutex
	receivers map[string]flowControlReceiver
}

type flowControlReceiver struct {
	rightEdge int64
	seenAt    time.Time
}

func NewMinMulticastFlowControl(timeout time.Duration) *MinMulticastFlowControl {
	if timeout <= 0 {
		timeout = defaultFlowControlReceiverTimeout
	}
	return &MinMulticastFlowControl{
		ReceiverTimeout: timeout,
		receivers:       make(map[string]flowControlReceiver),
	}
}

func (f *MinMulticastFlowControl) InitialLimit(termBufferLength int) int64 {
	return int64(termBufferLength)
}

func (f *MinMulticastFlowControl) OnStatus(status FlowControlStatus, senderLimit int64) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.ensureReceivers()
	f.receivers[status.ReceiverID] = flowControlReceiver{
		rightEdge: receiverRightEdge(status),
		seenAt:    status.ObservedAt,
	}
	return f.minRightEdge(senderLimit)
}

func (f *MinMulticastFlowControl) OnIdle(now time.Time, senderLimit, _ int64) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.ensureReceivers()
	for receiverID, receiver := range f.receivers {
		if now.Sub(receiver.seenAt) > f.ReceiverTimeout {
			delete(f.receivers, receiverID)
		}
	}
	return f.minRightEdge(senderLimit)
}

func (f *MinMulticastFlowControl) ensureReceivers() {
	if f.receivers == nil {
		f.receivers = make(map[string]flowControlReceiver)
	}
	if f.ReceiverTimeout <= 0 {
		f.ReceiverTimeout = defaultFlowControlReceiverTimeout
	}
}

func (f *MinMulticastFlowControl) minRightEdge(senderLimit int64) int64 {
	if len(f.receivers) == 0 {
		return senderLimit
	}

	minRightEdge := int64(0)
	first := true
	for _, receiver := range f.receivers {
		if first || receiver.rightEdge < minRightEdge {
			minRightEdge = receiver.rightEdge
			first = false
		}
	}
	return minRightEdge
}

func receiverRightEdge(status FlowControlStatus) int64 {
	return status.Position + int64(status.WindowLength)
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
