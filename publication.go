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

type ReservedValueSupplier func(payload []byte) uint64

type PublicationConfig struct {
	StreamID               uint32
	SessionID              uint32
	RemoteAddr             string
	MaxPayloadBytes        int
	MTUBytes               int
	TermBufferLength       int
	InitialTermID          int32
	PublicationWindowBytes int
	TLSConfig              *tls.Config
	QUICConfig             *quic.Config
	Metrics                *Metrics
	ReservedValue          ReservedValueSupplier

	// PacketConn is an advanced hook for tests and custom transports. When set, the caller owns closing it.
	PacketConn net.PacketConn

	// Kept for API compatibility. QUIC owns retransmission and socket buffers.
	LocalAddr        string
	RetransmitEvery  time.Duration
	ReadBufferBytes  int
	WriteBufferBytes int
}

type Publication struct {
	conn       *quic.Conn
	transport  *quic.Transport
	metrics    *Metrics
	streamID   uint32
	sessionID  uint32
	maxPayload int
	mtuPayload int
	reserved   ReservedValueSupplier
	terms      *termLog
	window     *publicationWindow
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
	if cfg.MTUBytes == 0 {
		cfg.MTUBytes = maxFrameSize
	}
	if cfg.MTUBytes <= headerLen || cfg.MTUBytes > maxFrameSize {
		return nil, fmt.Errorf("invalid MTU bytes: %d", cfg.MTUBytes)
	}
	mtuPayload := cfg.MTUBytes - headerLen
	if cfg.MaxPayloadBytes == 0 {
		cfg.MaxPayloadBytes = mtuPayload
	}
	if cfg.MaxPayloadBytes < 0 || cfg.MaxPayloadBytes > mtuPayload*maxFrameFragments {
		return nil, fmt.Errorf("invalid max payload bytes: %d", cfg.MaxPayloadBytes)
	}
	if cfg.TermBufferLength == 0 {
		cfg.TermBufferLength = minTermLength
	}
	terms, err := newTermLog(cfg.TermBufferLength, cfg.InitialTermID)
	if err != nil {
		return nil, err
	}
	if cfg.PublicationWindowBytes == 0 {
		cfg.PublicationWindowBytes = max(cfg.TermBufferLength, fragmentedWindowBytes(cfg.MaxPayloadBytes, mtuPayload))
	}
	window, err := newPublicationWindow(cfg.PublicationWindowBytes)
	if err != nil {
		return nil, err
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

	conn, transport, err := dialQUIC(context.Background(), cfg.RemoteAddr, tlsConf, cfg.QUICConfig, cfg.PacketConn)
	if err != nil {
		return nil, fmt.Errorf("dial quic publication: %w", err)
	}
	cfg.Metrics.incConnectionsOpened()

	p := &Publication{
		conn:       conn,
		transport:  transport,
		metrics:    cfg.Metrics,
		streamID:   cfg.StreamID,
		sessionID:  cfg.SessionID,
		maxPayload: cfg.MaxPayloadBytes,
		mtuPayload: mtuPayload,
		reserved:   cfg.ReservedValue,
		terms:      terms,
		window:     window,
		closed:     make(chan struct{}),
	}
	if err := p.negotiate(context.Background()); err != nil {
		_ = conn.CloseWithError(0, "negotiation failed")
		if transport != nil {
			_ = transport.Close()
		}
		return nil, err
	}
	return p, nil
}

func (p *Publication) Send(ctx context.Context, payload []byte) error {
	if len(payload) > p.maxPayload {
		p.metrics.incSendErrors()
		return fmt.Errorf("payload too large: %d bytes", len(payload))
	}

	select {
	case <-p.closed:
		p.metrics.incSendErrors()
		return ErrClosed
	default:
	}

	fragmentCount := countFragments(len(payload), p.mtuPayload)
	windowBytes := fragmentedWindowBytes(len(payload), p.mtuPayload)
	backPressured, err := p.window.reserve(ctx, windowBytes, p.closed)
	if backPressured {
		p.metrics.incBackPressureEvents()
	}
	if err != nil {
		p.metrics.incSendErrors()
		return err
	}
	defer p.window.release(windowBytes)

	seq := p.nextSeq.Add(1)
	var reserved uint64
	if p.reserved != nil {
		reserved = p.reserved(payload)
	}
	packet, appendResult, err := p.encodeDataPacket(seq, reserved, payload, fragmentCount)
	if err != nil {
		p.metrics.incSendErrors()
		return err
	}

	resp, err := p.roundTrip(ctx, packet)
	if err != nil {
		p.metrics.incSendErrors()
		return err
	}
	switch resp.typ {
	case frameAck:
		if resp.streamID != p.streamID || resp.sessionID != p.sessionID ||
			resp.termID != appendResult.TermID || resp.termOffset != appendResult.TermOffset || resp.seq != seq {
			p.metrics.incProtocolErrors()
			p.metrics.incSendErrors()
			return &ProtocolError{Code: uint16(protocolErrorMalformedFrame), Message: "ack does not match data frame"}
		}
		p.metrics.incMessagesSent(len(payload))
		p.metrics.incAcksReceived()
		return nil
	case frameError:
		p.metrics.incProtocolErrors()
		p.metrics.incSendErrors()
		return decodeProtocolError(resp.payload)
	default:
		p.metrics.incProtocolErrors()
		p.metrics.incSendErrors()
		return &ProtocolError{Code: uint16(protocolErrorUnsupportedType), Message: "unexpected response frame"}
	}
}

func (p *Publication) encodeDataPacket(seq, reserved uint64, payload []byte, fragmentCount int) ([]byte, termAppend, error) {
	packet := make([]byte, 0, fragmentedPacketBytes(len(payload), p.mtuPayload))
	var lastAppend termAppend

	for fragmentIndex := 0; fragmentIndex < fragmentCount; fragmentIndex++ {
		start := fragmentIndex * p.mtuPayload
		if start > len(payload) {
			start = len(payload)
		}
		end := min(start+p.mtuPayload, len(payload))
		fragmentPayload := payload[start:end]
		flags := frameFlag(0)
		if fragmentCount > 1 {
			flags = frameFlagFragment
		}
		frameLength := headerLen + len(fragmentPayload)

		appendResult, err := p.terms.append(frameLength, func(appendResult termAppend) error {
			encoded, encodeErr := encodeFrame(frame{
				typ:           frameData,
				flags:         flags,
				streamID:      p.streamID,
				sessionID:     p.sessionID,
				termID:        appendResult.TermID,
				termOffset:    appendResult.TermOffset,
				seq:           seq,
				reserved:      reserved,
				fragmentIndex: uint16(fragmentIndex),
				fragmentCount: uint16(fragmentCount),
				payload:       fragmentPayload,
			})
			if encodeErr != nil {
				return encodeErr
			}
			copy(appendResult.Bytes(), encoded)
			packet = append(packet, encoded...)
			return nil
		})
		if err != nil {
			return nil, termAppend{}, err
		}
		lastAppend = appendResult
	}

	return packet, lastAppend, nil
}

func countFragments(payloadLen, fragmentPayloadMax int) int {
	if payloadLen == 0 {
		return 1
	}
	return (payloadLen + fragmentPayloadMax - 1) / fragmentPayloadMax
}

func fragmentedWindowBytes(payloadLen, fragmentPayloadMax int) int {
	count := countFragments(payloadLen, fragmentPayloadMax)
	total := 0
	for i := 0; i < count; i++ {
		fragmentPayloadLen := fragmentPayloadMax
		if i == count-1 {
			remaining := payloadLen - i*fragmentPayloadMax
			if remaining < fragmentPayloadLen {
				fragmentPayloadLen = remaining
			}
		}
		total += align(headerLen+fragmentPayloadLen, termFrameAlignment)
	}
	return total
}

func fragmentedPacketBytes(payloadLen, fragmentPayloadMax int) int {
	count := countFragments(payloadLen, fragmentPayloadMax)
	return payloadLen + count*headerLen
}

func (p *Publication) LocalAddr() net.Addr {
	return p.conn.LocalAddr()
}

func (p *Publication) Close() error {
	var err error
	p.closeOnce.Do(func() {
		close(p.closed)
		err = p.conn.CloseWithError(0, "closed")
		if p.transport != nil {
			if transportErr := p.transport.Close(); err == nil {
				err = transportErr
			}
		}
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

func dialQUIC(ctx context.Context, remoteAddr string, tlsConf *tls.Config, quicConf *quic.Config, packetConn net.PacketConn) (*quic.Conn, *quic.Transport, error) {
	if packetConn == nil {
		conn, err := quic.DialAddr(ctx, remoteAddr, tlsConf, quicConf)
		return conn, nil, err
	}

	remote, err := net.ResolveUDPAddr("udp", remoteAddr)
	if err != nil {
		return nil, nil, err
	}
	transport := &quic.Transport{Conn: packetConn}
	conn, err := transport.Dial(ctx, remote, tlsConf, quicConf)
	if err != nil {
		_ = transport.Close()
		return nil, nil, err
	}
	return conn, transport, nil
}
