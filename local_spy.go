package bunshin

import (
	"context"
	"errors"
	"net"
	"sync"
)

const defaultLocalSpyBuffer = 1024

type localSpyKey struct {
	transport TransportMode
	streamID  uint32
	endpoint  string
}

type localSpyAddr string

func (a localSpyAddr) Network() string {
	return "local-spy"
}

func (a localSpyAddr) String() string {
	return string(a)
}

var localSpyRegistry = struct {
	sync.Mutex
	nextID uint64
	subs   map[localSpyKey]map[uint64]*Subscription
}{
	subs: make(map[localSpyKey]map[uint64]*Subscription),
}

func newLocalSpyKey(transport TransportMode, streamID uint32, endpoint string) localSpyKey {
	return localSpyKey{
		transport: transport,
		streamID:  streamID,
		endpoint:  endpoint,
	}
}

func registerLocalSpy(sub *Subscription) {
	localSpyRegistry.Lock()
	defer localSpyRegistry.Unlock()

	localSpyRegistry.nextID++
	sub.localSpyID = localSpyRegistry.nextID
	subs := localSpyRegistry.subs[sub.localSpyKey]
	if subs == nil {
		subs = make(map[uint64]*Subscription)
		localSpyRegistry.subs[sub.localSpyKey] = subs
	}
	subs[sub.localSpyID] = sub
}

func unregisterLocalSpy(sub *Subscription) {
	if sub == nil || !sub.localSpy || sub.localSpyID == 0 {
		return
	}
	localSpyRegistry.Lock()
	defer localSpyRegistry.Unlock()

	subs := localSpyRegistry.subs[sub.localSpyKey]
	if subs == nil {
		return
	}
	delete(subs, sub.localSpyID)
	if len(subs) == 0 {
		delete(localSpyRegistry.subs, sub.localSpyKey)
	}
}

func dispatchLocalSpy(keys []localSpyKey, msg Message) {
	subs := localSpySubscribers(keys)
	for _, sub := range subs {
		spyMsg := cloneMessage(msg)
		select {
		case sub.localSpyCh <- spyMsg:
		default:
			sub.metrics.incFramesDropped(1)
		}
	}
}

func localSpySubscribers(keys []localSpyKey) []*Subscription {
	localSpyRegistry.Lock()
	defer localSpyRegistry.Unlock()

	seen := make(map[uint64]struct{})
	subs := make([]*Subscription, 0)
	for _, key := range keys {
		for id, sub := range localSpyRegistry.subs[key] {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			subs = append(subs, sub)
		}
	}
	return subs
}

func (s *Subscription) serveLocalSpy(ctx context.Context, handler Handler) error {
	if handler == nil {
		err := errors.New("handler is required")
		s.log(ctx, LogLevelWarn, "serve", "local spy serve rejected", nil, err)
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		select {
		case <-s.closed:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return ErrClosed
		case <-ctx.Done():
			_ = s.Close()
			return ctx.Err()
		case msg := <-s.localSpyCh:
			s.metrics.incFramesReceived(1)
			if err := s.ordered.deliver(ctx, orderedMessage{
				ctx:      ctx,
				msg:      msg,
				position: msg.Position,
			}, handler); err != nil {
				s.metrics.incReceiveErrors()
			}
		}
	}
}

func localSpyMessage(streamID, sessionID uint32, seq, reserved uint64, appendResult termAppend, payload []byte, remote net.Addr, response ResponseChannel) Message {
	return Message{
		StreamID:        streamID,
		SessionID:       sessionID,
		TermID:          appendResult.TermID,
		TermOffset:      appendResult.TermOffset,
		Position:        appendResult.Position,
		Sequence:        seq,
		ReservedValue:   reserved,
		Payload:         payload,
		Remote:          remote,
		ResponseChannel: response,
	}
}

func (p *Publication) publishLocalSpies(payload []byte, seq, reserved uint64, appendResult termAppend, endpoints []string) {
	if len(endpoints) == 0 {
		return
	}
	seen := make(map[string]struct{}, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint == "" {
			continue
		}
		if _, ok := seen[endpoint]; ok {
			continue
		}
		seen[endpoint] = struct{}{}
		msg := localSpyMessage(p.streamID, p.sessionID, seq, reserved, appendResult, payload, localSpyAddr(endpoint), p.responseChannel)
		dispatchLocalSpy([]localSpyKey{newLocalSpyKey(p.transportMode, p.streamID, endpoint)}, msg)
	}
}
