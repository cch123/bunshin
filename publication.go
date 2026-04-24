package bunshin

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
)

var ErrClosed = errors.New("bunshin: closed")

type ProtocolError struct {
	Code    uint16
	Message string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("bunshin protocol error %d: %s", e.Code, e.Message)
}

type PublicationConfig struct {
	StreamID        uint32
	SessionID       uint32
	RemoteAddr      string
	MaxPayloadBytes int
	TLSConfig       *tls.Config
	QUICConfig      *quic.Config

	// Kept for API compatibility. QUIC owns retransmission and socket buffers.
	LocalAddr        string
	RetransmitEvery  time.Duration
	ReadBufferBytes  int
	WriteBufferBytes int
}

type Publication struct {
	conn       *quic.Conn
	streamID   uint32
	sessionID  uint32
	maxPayload int
	nextSeq    atomic.Uint64
	closed     chan struct{}
	closeOnce  sync.Once
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
	if cfg.MaxPayloadBytes == 0 {
		cfg.MaxPayloadBytes = maxFrameSize - headerLen
	}
	if cfg.MaxPayloadBytes < 0 || cfg.MaxPayloadBytes > maxFrameSize-headerLen {
		return nil, fmt.Errorf("invalid max payload bytes: %d", cfg.MaxPayloadBytes)
	}

	tlsConf := cfg.TLSConfig
	if tlsConf == nil {
		tlsConf = defaultClientTLSConfig()
	} else {
		tlsConf = tlsConf.Clone()
		if len(tlsConf.NextProtos) == 0 {
			tlsConf.NextProtos = []string{quicALPN}
		}
	}

	conn, err := quic.DialAddr(context.Background(), cfg.RemoteAddr, tlsConf, cfg.QUICConfig)
	if err != nil {
		return nil, fmt.Errorf("dial quic publication: %w", err)
	}

	p := &Publication{
		conn:       conn,
		streamID:   cfg.StreamID,
		sessionID:  cfg.SessionID,
		maxPayload: cfg.MaxPayloadBytes,
		closed:     make(chan struct{}),
	}
	if err := p.negotiate(context.Background()); err != nil {
		_ = conn.CloseWithError(0, "negotiation failed")
		return nil, err
	}
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

	resp, err := p.roundTrip(ctx, packet)
	if err != nil {
		return err
	}
	switch resp.typ {
	case frameAck:
		if resp.streamID != p.streamID || resp.sessionID != p.sessionID || resp.seq != seq {
			return &ProtocolError{Code: uint16(protocolErrorMalformedFrame), Message: "ack does not match data frame"}
		}
		return nil
	case frameError:
		return decodeProtocolError(resp.payload)
	default:
		return &ProtocolError{Code: uint16(protocolErrorUnsupportedType), Message: "unexpected response frame"}
	}
}

func (p *Publication) LocalAddr() net.Addr {
	return p.conn.LocalAddr()
}

func (p *Publication) Close() error {
	var err error
	p.closeOnce.Do(func() {
		close(p.closed)
		err = p.conn.CloseWithError(0, "closed")
	})
	return err
}

func (p *Publication) negotiate(ctx context.Context) error {
	packet, err := encodeFrame(frame{
		typ: frameHello,
		payload: encodeHelloPayload(helloPayload{
			minVersion: frameVersion,
			maxVersion: frameVersion,
		}),
	})
	if err != nil {
		return err
	}

	resp, err := p.roundTrip(ctx, packet)
	if err != nil {
		return err
	}
	switch resp.typ {
	case frameHello:
		hello, err := decodeHelloPayload(resp.payload)
		if err != nil {
			return err
		}
		if hello.minVersion > frameVersion || hello.maxVersion < frameVersion {
			return &ProtocolError{Code: uint16(protocolErrorUnsupportedVersion), Message: "peer does not support protocol version"}
		}
		return nil
	case frameError:
		return decodeProtocolError(resp.payload)
	default:
		return &ProtocolError{Code: uint16(protocolErrorUnsupportedType), Message: "unexpected negotiation response"}
	}
}

func (p *Publication) roundTrip(ctx context.Context, packet []byte) (frame, error) {
	stream, err := p.conn.OpenStreamSync(ctx)
	if err != nil {
		return frame{}, err
	}
	if _, err := stream.Write(packet); err != nil {
		_ = stream.Close()
		return frame{}, err
	}
	if err := stream.Close(); err != nil {
		return frame{}, err
	}

	resp, err := io.ReadAll(stream)
	if err != nil {
		return frame{}, err
	}
	return decodeFrame(resp)
}

func decodeProtocolError(payload []byte) error {
	protocolErr, err := decodeErrorPayload(payload)
	if err != nil {
		return err
	}
	return &ProtocolError{
		Code:    uint16(protocolErr.code),
		Message: protocolErr.message,
	}
}
