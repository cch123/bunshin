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

type PreferredMulticastFlowControl struct {
	ReceiverTimeout      time.Duration
	PreferredReceiverIDs []string

	mu        sync.Mutex
	receivers map[string]flowControlReceiver
	preferred map[string]struct{}
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

func NewPreferredMulticastFlowControl(timeout time.Duration, preferredReceiverIDs ...string) *PreferredMulticastFlowControl {
	if timeout <= 0 {
		timeout = defaultFlowControlReceiverTimeout
	}
	flow := &PreferredMulticastFlowControl{
		ReceiverTimeout:      timeout,
		PreferredReceiverIDs: append([]string(nil), preferredReceiverIDs...),
		receivers:            make(map[string]flowControlReceiver),
		preferred:            make(map[string]struct{}, len(preferredReceiverIDs)),
	}
	for _, receiverID := range preferredReceiverIDs {
		if receiverID == "" {
			continue
		}
		flow.preferred[receiverID] = struct{}{}
	}
	return flow
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

func (f *PreferredMulticastFlowControl) InitialLimit(termBufferLength int) int64 {
	return int64(termBufferLength)
}

func (f *PreferredMulticastFlowControl) OnStatus(status FlowControlStatus, senderLimit int64) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.ensure()
	f.receivers[status.ReceiverID] = flowControlReceiver{
		rightEdge: receiverRightEdge(status),
		seenAt:    status.ObservedAt,
	}
	return f.limitLocked(senderLimit)
}

func (f *PreferredMulticastFlowControl) OnIdle(now time.Time, senderLimit, _ int64) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.ensure()
	for receiverID, receiver := range f.receivers {
		if now.Sub(receiver.seenAt) > f.ReceiverTimeout {
			delete(f.receivers, receiverID)
		}
	}
	return f.limitLocked(senderLimit)
}

func (f *PreferredMulticastFlowControl) ensure() {
	if f.receivers == nil {
		f.receivers = make(map[string]flowControlReceiver)
	}
	if f.preferred == nil {
		f.preferred = make(map[string]struct{}, len(f.PreferredReceiverIDs))
		for _, receiverID := range f.PreferredReceiverIDs {
			if receiverID == "" {
				continue
			}
			f.preferred[receiverID] = struct{}{}
		}
	}
	if f.ReceiverTimeout <= 0 {
		f.ReceiverTimeout = defaultFlowControlReceiverTimeout
	}
}

func (f *PreferredMulticastFlowControl) limitLocked(senderLimit int64) int64 {
	if len(f.receivers) == 0 {
		return senderLimit
	}

	preferredRightEdge, ok := minPreferredReceiverRightEdge(f.receivers, f.preferred)
	if ok {
		return preferredRightEdge
	}
	return minReceiverRightEdge(f.receivers)
}

func minPreferredReceiverRightEdge(receivers map[string]flowControlReceiver, preferred map[string]struct{}) (int64, bool) {
	if len(preferred) == 0 {
		return 0, false
	}
	var minRightEdge int64
	first := true
	for receiverID, receiver := range receivers {
		if _, ok := preferred[receiverID]; !ok {
			continue
		}
		if first || receiver.rightEdge < minRightEdge {
			minRightEdge = receiver.rightEdge
			first = false
		}
	}
	return minRightEdge, !first
}

func minReceiverRightEdge(receivers map[string]flowControlReceiver) int64 {
	var minRightEdge int64
	first := true
	for _, receiver := range receivers {
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
