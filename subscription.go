package bunshin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
)

type Message struct {
	StreamID  uint32
	SessionID uint32
	Sequence  uint64
	Payload   []byte
	Remote    net.Addr
}

type Handler func(context.Context, Message) error

type SubscriptionConfig struct {
	StreamID         uint32
	LocalAddr        string
	ReadBufferBytes  int
	WriteBufferBytes int
}

type Subscription struct {
	conn      *net.UDPConn
	streamID  uint32
	closed    chan struct{}
	closeOnce sync.Once
	seenMu    sync.Mutex
	seen      map[streamSeq]struct{}
}

type streamSeq struct {
	sessionID uint32
	seq       uint64
}

func ListenSubscription(cfg SubscriptionConfig) (*Subscription, error) {
	if cfg.LocalAddr == "" {
		return nil, errors.New("local address is required")
	}
	if cfg.StreamID == 0 {
		cfg.StreamID = 1
	}

	local, err := net.ResolveUDPAddr("udp", cfg.LocalAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve local address: %w", err)
	}
	conn, err := net.ListenUDP("udp", local)
	if err != nil {
		return nil, fmt.Errorf("listen subscription socket: %w", err)
	}
	if cfg.ReadBufferBytes > 0 {
		_ = conn.SetReadBuffer(cfg.ReadBufferBytes)
	}
	if cfg.WriteBufferBytes > 0 {
		_ = conn.SetWriteBuffer(cfg.WriteBufferBytes)
	}

	return &Subscription{
		conn:     conn,
		streamID: cfg.StreamID,
		closed:   make(chan struct{}),
		seen:     make(map[streamSeq]struct{}),
	}, nil
}

func (s *Subscription) Serve(ctx context.Context, handler Handler) error {
	if handler == nil {
		return errors.New("handler is required")
	}

	errCh := make(chan error, 1)
	go func() {
		buf := make([]byte, maxFrameSize)
		for {
			n, remote, err := s.conn.ReadFromUDP(buf)
			if err != nil {
				select {
				case <-s.closed:
					errCh <- ErrClosed
				case <-ctx.Done():
					errCh <- ctx.Err()
				default:
					errCh <- err
				}
				return
			}

			f, err := decodeFrame(buf[:n])
			if err != nil || f.typ != frameData || f.streamID != s.streamID {
				continue
			}
			_ = s.ack(remote, f)

			key := streamSeq{sessionID: f.sessionID, seq: f.seq}
			if !s.markSeen(key) {
				continue
			}

			msg := Message{
				StreamID:  f.streamID,
				SessionID: f.sessionID,
				Sequence:  f.seq,
				Payload:   f.payload,
				Remote:    remote,
			}
			if err := handler(ctx, msg); err != nil {
				errCh <- err
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		_ = s.Close()
		return ctx.Err()
	case err := <-errCh:
		if errors.Is(err, ErrClosed) && ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
}

func (s *Subscription) LocalAddr() net.Addr {
	return s.conn.LocalAddr()
}

func (s *Subscription) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.closed)
		err = s.conn.Close()
	})
	return err
}

func (s *Subscription) ack(remote *net.UDPAddr, f frame) error {
	packet, err := encodeFrame(frame{
		typ:       frameAck,
		streamID:  f.streamID,
		sessionID: f.sessionID,
		seq:       f.seq,
	})
	if err != nil {
		return err
	}
	_, err = s.conn.WriteToUDP(packet, remote)
	return err
}

func (s *Subscription) markSeen(key streamSeq) bool {
	s.seenMu.Lock()
	defer s.seenMu.Unlock()
	if _, ok := s.seen[key]; ok {
		return false
	}
	s.seen[key] = struct{}{}
	return true
}
