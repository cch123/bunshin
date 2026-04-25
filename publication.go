package bunshin

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
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
	Logger                 Logger
	ReservedValue          ReservedValueSupplier
	FlowControl            FlowControlStrategy

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
	logger     Logger
	streamID   uint32
	sessionID  uint32
	maxPayload int
	mtuPayload int
	reserved   ReservedValueSupplier
	terms      *termLog
	window     *publicationWindow
	flow       FlowControlStrategy
	flowWindow int
	flowLimit  int64
	flowMu     sync.Mutex
	nextSeq    atomic.Uint64
	closed     chan struct{}
	closeOnce  sync.Once
}

func DialPublication(cfg PublicationConfig) (*Publication, error) {
	normalized, err := normalizePublicationConfig(cfg)
	if err != nil {
		return nil, err
	}
	cfg = normalized.PublicationConfig

	terms, err := newTermLog(cfg.TermBufferLength, cfg.InitialTermID)
	if err != nil {
		return nil, err
	}
	window, err := newPublicationWindow(cfg.PublicationWindowBytes)
	if err != nil {
		return nil, err
	}

	conn, transport, err := dialQUIC(context.Background(), cfg.RemoteAddr, cfg.TLSConfig, cfg.QUICConfig, cfg.PacketConn)
	if err != nil {
		logEvent(context.Background(), cfg.Logger, LogEvent{
			Level:     LogLevelError,
			Component: "publication",
			Operation: "dial",
			Message:   "dial failed",
			Fields: map[string]any{
				"remote_addr": cfg.RemoteAddr,
				"stream_id":   cfg.StreamID,
				"session_id":  cfg.SessionID,
			},
			Err: err,
		})
		return nil, fmt.Errorf("dial quic publication: %w", err)
	}
	cfg.Metrics.incConnectionsOpened()

	p := &Publication{
		conn:       conn,
		transport:  transport,
		metrics:    cfg.Metrics,
		logger:     cfg.Logger,
		streamID:   cfg.StreamID,
		sessionID:  cfg.SessionID,
		maxPayload: cfg.MaxPayloadBytes,
		mtuPayload: normalized.mtuPayload,
		reserved:   cfg.ReservedValue,
		terms:      terms,
		window:     window,
		flow:       cfg.FlowControl,
		flowWindow: cfg.PublicationWindowBytes,
		flowLimit:  normalized.flowLimit,
		closed:     make(chan struct{}),
	}
	if err := p.negotiate(context.Background()); err != nil {
		p.log(context.Background(), LogLevelError, "negotiate", "negotiation failed", map[string]any{
			"remote_addr": cfg.RemoteAddr,
			"stream_id":   cfg.StreamID,
			"session_id":  cfg.SessionID,
		}, err)
		_ = conn.CloseWithError(0, "negotiation failed")
		if transport != nil {
			_ = transport.Close()
		}
		return nil, err
	}
	p.log(context.Background(), LogLevelInfo, "dial", "publication connected", map[string]any{
		"remote_addr": cfg.RemoteAddr,
		"stream_id":   cfg.StreamID,
		"session_id":  cfg.SessionID,
	}, nil)
	return p, nil
}

func (p *Publication) Send(ctx context.Context, payload []byte) error {
	if len(payload) > p.maxPayload {
		err := fmt.Errorf("payload too large: %d bytes", len(payload))
		p.metrics.incSendErrors()
		p.log(ctx, LogLevelWarn, "send", "send rejected", map[string]any{
			"bytes":     len(payload),
			"max_bytes": p.maxPayload,
		}, err)
		return err
	}

	select {
	case <-p.closed:
		p.metrics.incSendErrors()
		p.log(ctx, LogLevelWarn, "send", "send on closed publication", nil, ErrClosed)
		return ErrClosed
	default:
	}

	fragmentCount := countFragments(len(payload), p.mtuPayload)
	windowBytes := fragmentedWindowBytes(len(payload), p.mtuPayload)
	backPressured, err := p.window.reserve(ctx, windowBytes, p.closed)
	if backPressured {
		p.metrics.incBackPressureEvents()
		p.log(ctx, LogLevelWarn, "send", "publication back pressured", map[string]any{
			"bytes": windowBytes,
		}, nil)
	}
	if err != nil {
		p.metrics.incSendErrors()
		p.log(ctx, LogLevelWarn, "send", "window reservation failed", map[string]any{
			"bytes": windowBytes,
		}, err)
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
		p.log(ctx, LogLevelError, "send", "encode failed", map[string]any{
			"sequence": seq,
			"bytes":    len(payload),
		}, err)
		return err
	}

	resp, err := p.roundTrip(ctx, packet, fragmentCount)
	if err != nil {
		p.metrics.incSendErrors()
		p.log(ctx, LogLevelWarn, "send", "round trip failed", map[string]any{
			"sequence":  seq,
			"bytes":     len(payload),
			"fragments": fragmentCount,
		}, err)
		return err
	}
	switch resp.typ {
	case frameAck:
		if resp.streamID != p.streamID || resp.sessionID != p.sessionID ||
			resp.termID != appendResult.TermID || resp.termOffset != appendResult.TermOffset || resp.seq != seq {
			p.metrics.incFramesDropped(1)
			p.metrics.incProtocolErrors()
			p.metrics.incSendErrors()
			err := &ProtocolError{Code: uint16(protocolErrorMalformedFrame), Message: "ack does not match data frame"}
			p.log(ctx, LogLevelError, "send", "ack mismatch", map[string]any{
				"sequence": seq,
			}, err)
			return err
		}
		if err := p.updateFlowControl(appendResult.Position); err != nil {
			p.metrics.incSendErrors()
			p.log(ctx, LogLevelError, "send", "flow control update failed", map[string]any{
				"sequence": seq,
				"position": appendResult.Position,
			}, err)
			return err
		}
		p.metrics.incMessagesSent(len(payload))
		p.metrics.incAcksReceived()
		p.log(ctx, LogLevelDebug, "send", "message sent", map[string]any{
			"sequence":  seq,
			"bytes":     len(payload),
			"fragments": fragmentCount,
			"position":  appendResult.Position,
		}, nil)
		return nil
	case frameError:
		p.metrics.incProtocolErrors()
		p.metrics.incSendErrors()
		err := decodeProtocolError(resp.payload)
		p.log(ctx, LogLevelError, "send", "peer returned protocol error", map[string]any{
			"sequence": seq,
		}, err)
		return err
	default:
		p.metrics.incFramesDropped(1)
		p.metrics.incProtocolErrors()
		p.metrics.incSendErrors()
		err := &ProtocolError{Code: uint16(protocolErrorUnsupportedType), Message: "unexpected response frame"}
		p.log(ctx, LogLevelError, "send", "unexpected response frame", map[string]any{
			"sequence": seq,
			"type":     resp.typ,
		}, err)
		return err
	}
}

func (p *Publication) updateFlowControl(receiverPosition int64) error {
	p.flowMu.Lock()
	defer p.flowMu.Unlock()

	status := FlowControlStatus{
		ReceiverID:   remoteAddrString(p.conn.RemoteAddr()),
		Position:     receiverPosition,
		WindowLength: p.flowWindow,
		ObservedAt:   time.Now(),
	}
	p.flowLimit = p.flow.OnStatus(status, p.flowLimit)

	limit := int(p.flowLimit - receiverPosition)
	if limit <= 0 {
		limit = 1
	}
	return p.window.setLimit(limit)
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
		p.log(context.Background(), LogLevelInfo, "close", "publication closed", map[string]any{
			"stream_id":  p.streamID,
			"session_id": p.sessionID,
		}, err)
	})
	return err
}

func (p *Publication) log(ctx context.Context, level LogLevel, operation, message string, fields map[string]any, err error) {
	logEvent(ctx, p.logger, LogEvent{
		Level:     level,
		Component: "publication",
		Operation: operation,
		Message:   message,
		Fields:    fields,
		Err:       err,
	})
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

	resp, err := p.roundTrip(ctx, packet, 1)
	if err != nil {
		return err
	}
	switch resp.typ {
	case frameHello:
		hello, err := decodeHelloPayload(resp.payload)
		if err != nil {
			p.metrics.incFramesDropped(1)
			return err
		}
		if hello.minVersion > frameVersion || hello.maxVersion < frameVersion {
			p.metrics.incFramesDropped(1)
			return &ProtocolError{Code: uint16(protocolErrorUnsupportedVersion), Message: "peer does not support protocol version"}
		}
		return nil
	case frameError:
		return decodeProtocolError(resp.payload)
	default:
		p.metrics.incFramesDropped(1)
		return &ProtocolError{Code: uint16(protocolErrorUnsupportedType), Message: "unexpected negotiation response"}
	}
}

func (p *Publication) roundTrip(ctx context.Context, packet []byte, sentFrames int) (frame, error) {
	stream, err := p.conn.OpenStreamSync(ctx)
	if err != nil {
		return frame{}, err
	}
	stop := context.AfterFunc(ctx, func() {
		stream.CancelRead(0)
		stream.CancelWrite(0)
	})
	defer stop()
	if _, err := stream.Write(packet); err != nil {
		_ = stream.Close()
		return frame{}, contextErrorOr(ctx, err)
	}
	p.metrics.incFramesSent(sentFrames)
	if err := stream.Close(); err != nil {
		return frame{}, contextErrorOr(ctx, err)
	}

	resp, err := io.ReadAll(stream)
	if err != nil {
		return frame{}, contextErrorOr(ctx, err)
	}
	f, err := decodeFrame(resp)
	if err != nil {
		p.metrics.incFramesDropped(1)
		return frame{}, err
	}
	p.metrics.incFramesReceived(1)
	return f, nil
}

func contextErrorOr(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
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
