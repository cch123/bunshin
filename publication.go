package bunshin

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var ErrClosed = errors.New("bunshin: closed")

type PublicationConfig struct {
	StreamID         uint32
	SessionID        uint32
	LocalAddr        string
	RemoteAddr       string
	RetransmitEvery  time.Duration
	MaxPayloadBytes  int
	ReadBufferBytes  int
	WriteBufferBytes int
}

type Publication struct {
	conn        *net.UDPConn
	remote      *net.UDPAddr
	streamID    uint32
	sessionID   uint32
	retransmit  time.Duration
	maxPayload  int
	nextSeq     atomic.Uint64
	closed      chan struct{}
	closeOnce   sync.Once
	pendingMu   sync.Mutex
	pendingAcks map[uint64]chan error
}

func DialPublication(cfg PublicationConfig) (*Publication, error) {
	if cfg.RemoteAddr == "" {
		return nil, errors.New("remote address is required")
	}
	if cfg.StreamID == 0 {
		cfg.StreamID = 1
	}
	if cfg.SessionID == 0 {
		cfg.SessionID = rand.Uint32()
	}
	if cfg.RetransmitEvery == 0 {
		cfg.RetransmitEvery = 10 * time.Millisecond
	}
	if cfg.MaxPayloadBytes == 0 {
		cfg.MaxPayloadBytes = maxFrameSize - headerLen
	}
	if cfg.MaxPayloadBytes < 0 || cfg.MaxPayloadBytes > maxFrameSize-headerLen {
		return nil, fmt.Errorf("invalid max payload bytes: %d", cfg.MaxPayloadBytes)
	}

	remote, err := net.ResolveUDPAddr("udp", cfg.RemoteAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve remote address: %w", err)
	}

	var local *net.UDPAddr
	if cfg.LocalAddr != "" {
		local, err = net.ResolveUDPAddr("udp", cfg.LocalAddr)
		if err != nil {
			return nil, fmt.Errorf("resolve local address: %w", err)
		}
	}

	conn, err := net.ListenUDP("udp", local)
	if err != nil {
		return nil, fmt.Errorf("listen publication socket: %w", err)
	}
	if cfg.ReadBufferBytes > 0 {
		_ = conn.SetReadBuffer(cfg.ReadBufferBytes)
	}
	if cfg.WriteBufferBytes > 0 {
		_ = conn.SetWriteBuffer(cfg.WriteBufferBytes)
	}

	p := &Publication{
		conn:        conn,
		remote:      remote,
		streamID:    cfg.StreamID,
		sessionID:   cfg.SessionID,
		retransmit:  cfg.RetransmitEvery,
		maxPayload:  cfg.MaxPayloadBytes,
		closed:      make(chan struct{}),
		pendingAcks: make(map[uint64]chan error),
	}
	go p.readAcks()
	return p, nil
}

func (p *Publication) Send(ctx context.Context, payload []byte) error {
	if len(payload) > p.maxPayload {
		return fmt.Errorf("payload too large: %d bytes", len(payload))
	}

	select {
	case <-p.closed:
		return ErrClosed
	default:
	}

	seq := p.nextSeq.Add(1)
	ack := make(chan error, 1)
	p.pendingMu.Lock()
	p.pendingAcks[seq] = ack
	p.pendingMu.Unlock()
	defer p.deletePending(seq)

	packet, err := encodeFrame(frame{
		typ:       frameData,
		streamID:  p.streamID,
		sessionID: p.sessionID,
		seq:       seq,
		payload:   payload,
	})
	if err != nil {
		return err
	}

	ticker := time.NewTicker(p.retransmit)
	defer ticker.Stop()

	if err := p.write(packet); err != nil {
		return err
	}
	for {
		select {
		case err := <-ack:
			return err
		case <-ticker.C:
			if err := p.write(packet); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-p.closed:
			return ErrClosed
		}
	}
}

func (p *Publication) LocalAddr() net.Addr {
	return p.conn.LocalAddr()
}

func (p *Publication) Close() error {
	var err error
	p.closeOnce.Do(func() {
		close(p.closed)
		err = p.conn.Close()
		p.pendingMu.Lock()
		for seq, ack := range p.pendingAcks {
			ack <- ErrClosed
			delete(p.pendingAcks, seq)
		}
		p.pendingMu.Unlock()
	})
	return err
}

func (p *Publication) write(packet []byte) error {
	_, err := p.conn.WriteToUDP(packet, p.remote)
	if err != nil {
		select {
		case <-p.closed:
			return ErrClosed
		default:
			return err
		}
	}
	return nil
}

func (p *Publication) readAcks() {
	buf := make([]byte, maxFrameSize)
	for {
		n, _, err := p.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		f, err := decodeFrame(buf[:n])
		if err != nil || f.typ != frameAck || f.streamID != p.streamID || f.sessionID != p.sessionID {
			continue
		}

		p.pendingMu.Lock()
		ack, ok := p.pendingAcks[f.seq]
		if ok {
			delete(p.pendingAcks, f.seq)
			ack <- nil
		}
		p.pendingMu.Unlock()
	}
}

func (p *Publication) deletePending(seq uint64) {
	p.pendingMu.Lock()
	delete(p.pendingAcks, seq)
	p.pendingMu.Unlock()
}
