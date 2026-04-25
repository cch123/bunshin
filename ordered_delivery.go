package bunshin

import (
	"context"
	"fmt"
	"sync"

	"github.com/quic-go/quic-go"
)

type orderedDelivery struct {
	mu     sync.Mutex
	states map[lossKey]*orderedState
	sub    *Subscription
}

type orderedState struct {
	next       uint64
	delivering bool
	pending    map[uint64]*orderedMessage
}

type orderedMessage struct {
	ctx      context.Context
	stream   *quic.Stream
	msg      Message
	ackFrame frame
	done     chan error
}

func newOrderedDelivery(sub *Subscription) *orderedDelivery {
	return &orderedDelivery{
		states: make(map[lossKey]*orderedState),
		sub:    sub,
	}
}

func (d *orderedDelivery) deliver(ctx context.Context, item orderedMessage, handler Handler) error {
	if item.msg.Sequence == 0 {
		return d.sub.handleMessage(ctx, item.stream, item.msg, item.ackFrame, handler)
	}

	key := lossKey{
		streamID:  item.msg.StreamID,
		sessionID: item.msg.SessionID,
		source:    remoteAddrString(item.msg.Remote),
	}
	item.done = make(chan error, 1)

	d.mu.Lock()
	state := d.states[key]
	if state == nil {
		state = &orderedState{
			next:    1,
			pending: make(map[uint64]*orderedMessage),
		}
		d.states[key] = state
	}
	if item.msg.Sequence < state.next {
		d.mu.Unlock()
		d.sub.metrics.incFramesDropped(1)
		return d.sub.ack(item.stream, item.ackFrame)
	}
	if _, exists := state.pending[item.msg.Sequence]; exists {
		d.mu.Unlock()
		d.sub.metrics.incFramesDropped(1)
		return fmt.Errorf("duplicate pending sequence: %d", item.msg.Sequence)
	}
	state.pending[item.msg.Sequence] = &item
	if item.msg.Sequence == state.next && !state.delivering {
		state.delivering = true
		go d.deliverReady(key, state, handler)
	}
	d.mu.Unlock()

	select {
	case err := <-item.done:
		return err
	case <-ctx.Done():
		d.removePending(key, item.msg.Sequence, item.done)
		return ctx.Err()
	}
}

func (d *orderedDelivery) deliverReady(key lossKey, state *orderedState, handler Handler) {
	for {
		d.mu.Lock()
		item := state.pending[state.next]
		if item == nil {
			state.delivering = false
			d.mu.Unlock()
			return
		}
		delete(state.pending, state.next)
		state.next++
		d.mu.Unlock()

		err := d.sub.handleMessage(item.ctx, item.stream, item.msg, item.ackFrame, handler)
		item.done <- err
	}
}

func (d *orderedDelivery) removePending(key lossKey, seq uint64, done chan error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	state := d.states[key]
	if state == nil {
		return
	}
	item := state.pending[seq]
	if item == nil || item.done != done {
		return
	}
	delete(state.pending, seq)
}
