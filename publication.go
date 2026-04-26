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

const defaultUDPTransportMTUBytes = 1400

type TransportMode string

const (
	TransportQUIC TransportMode = "quic"
	TransportUDP  TransportMode = "udp"
	TransportIPC  TransportMode = "ipc"
)

type ProtocolError struct {
	Code    uint16
	Message string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("bunshin protocol error %d: %s", e.Code, e.Message)
}

type ReservedValueSupplier func(payload []byte) uint64

type TransportFeedback struct {
	Transport           TransportMode
	Remote              string
	StreamID            uint32
	SessionID           uint32
	Sequence            uint64
	RTT                 time.Duration
	RetransmittedFrames int
	ObservedAt          time.Time
}

type TransportFeedbackHandler func(TransportFeedback)

type UDPDestinationStatus struct {
	Endpoint            string        `json:"endpoint"`
	Remote              string        `json:"remote"`
	Active              bool          `json:"active"`
	LastSetupAt         time.Time     `json:"last_setup_at,omitempty"`
	LastStatusAt        time.Time     `json:"last_status_at,omitempty"`
	LastAckAt           time.Time     `json:"last_ack_at,omitempty"`
	LastFeedbackAt      time.Time     `json:"last_feedback_at,omitempty"`
	LastSequence        uint64        `json:"last_sequence,omitempty"`
	LastRTT             time.Duration `json:"last_rtt,omitempty"`
	RetransmittedFrames int           `json:"retransmitted_frames,omitempty"`
}

type PublicationConfig struct {
	Transport                 TransportMode
	StreamID                  uint32
	SessionID                 uint32
	RemoteAddr                string
	UDPDestinations           []string
	UDPMulticastInterface     string
	UDPNameResolutionInterval time.Duration
	UDPReceiverTimeout        time.Duration
	MaxPayloadBytes           int
	MTUBytes                  int
	UDPRetransmitBufferBytes  int
	TermBufferLength          int
	TermBufferDirectory       string
	InitialTermID             int32
	PublicationWindowBytes    int
	TLSConfig                 *tls.Config
	QUICConfig                *quic.Config
	Metrics                   *Metrics
	Logger                    Logger
	ReservedValue             ReservedValueSupplier
	FlowControl               FlowControlStrategy
	TransportFeedback         TransportFeedbackHandler
	ResponseChannel           ResponseChannel

	// PacketConn is an advanced hook for tests and custom transports. When set, the caller owns closing it.
	PacketConn net.PacketConn

	// Kept for API compatibility. QUIC owns retransmission and socket buffers.
	LocalAddr        string
	RetransmitEvery  time.Duration
	ReadBufferBytes  int
	WriteBufferBytes int
}

type Publication struct {
	transportMode             TransportMode
	conn                      *quic.Conn
	udpConn                   net.PacketConn
	udpOwnConn                bool
	udpRemote                 net.Addr
	udpDestinations           map[string]*udpDestination
	udpDestinationOrder       []string
	udpMulticastInterface     string
	udpNameResolutionInterval time.Duration
	udpNextNameResolution     time.Time
	udpReceiverTimeout        time.Duration
	udpMu                     sync.Mutex
	udpRetransmit             map[uint64]udpRetransmitEntry
	udpRetransmitOrder        []uint64
	udpRetransmitBytes        int
	udpRetransmitLimit        int
	transport                 *quic.Transport
	metrics                   *Metrics
	logger                    Logger
	streamID                  uint32
	sessionID                 uint32
	maxPayload                int
	mtuPayload                int
	reserved                  ReservedValueSupplier
	transportFeedback         TransportFeedbackHandler
	responseChannel           ResponseChannel
	spyEndpoint               string
	terms                     *termLog
	window                    *publicationWindow
	flow                      FlowControlStrategy
	flowWindow                int
	flowLimit                 int64
	flowMu                    sync.Mutex
	nextSeq                   atomic.Uint64
	closed                    chan struct{}
	closeOnce                 sync.Once
}

func DialPublication(cfg PublicationConfig) (*Publication, error) {
	normalized, err := normalizePublicationConfig(cfg)
	if err != nil {
		return nil, err
	}
	cfg = normalized.PublicationConfig

	terms, err := newMappedTermLog(cfg.TermBufferLength, cfg.InitialTermID, cfg.TermBufferDirectory)
	if err != nil {
		return nil, err
	}
	window, err := newPublicationWindow(cfg.PublicationWindowBytes)
	if err != nil {
		return nil, err
	}

	if cfg.Transport == TransportUDP {
		conn, remote, err := listenUDPTransport(cfg.LocalAddr, cfg.RemoteAddr, cfg.UDPMulticastInterface, cfg.PacketConn)
		if err != nil {
			logEvent(context.Background(), cfg.Logger, LogEvent{
				Level:     LogLevelError,
				Component: "publication",
				Operation: "dial",
				Message:   "udp dial failed",
				Fields: map[string]any{
					"local_addr":  cfg.LocalAddr,
					"remote_addr": cfg.RemoteAddr,
					"stream_id":   cfg.StreamID,
					"session_id":  cfg.SessionID,
				},
				Err: err,
			})
			return nil, fmt.Errorf("dial udp publication: %w", err)
		}
		destinations, destinationOrder, err := resolveUDPDestinations(cfg.RemoteAddr, remote, cfg.UDPDestinations)
		if err != nil {
			if cfg.PacketConn == nil {
				_ = conn.Close()
			}
			return nil, err
		}
		cfg.Metrics.incConnectionsOpened()
		p := &Publication{
			transportMode:             cfg.Transport,
			udpConn:                   conn,
			udpOwnConn:                cfg.PacketConn == nil,
			udpRemote:                 remote,
			udpDestinations:           destinations,
			udpDestinationOrder:       destinationOrder,
			udpMulticastInterface:     cfg.UDPMulticastInterface,
			udpNameResolutionInterval: cfg.UDPNameResolutionInterval,
			udpReceiverTimeout:        cfg.UDPReceiverTimeout,
			udpRetransmit:             make(map[uint64]udpRetransmitEntry),
			udpRetransmitLimit:        cfg.UDPRetransmitBufferBytes,
			metrics:                   cfg.Metrics,
			logger:                    cfg.Logger,
			streamID:                  cfg.StreamID,
			sessionID:                 cfg.SessionID,
			maxPayload:                cfg.MaxPayloadBytes,
			mtuPayload:                normalized.mtuPayload,
			reserved:                  cfg.ReservedValue,
			transportFeedback:         cfg.TransportFeedback,
			responseChannel:           cfg.ResponseChannel,
			spyEndpoint:               remoteAddrString(remote),
			terms:                     terms,
			window:                    window,
			flow:                      cfg.FlowControl,
			flowWindow:                cfg.PublicationWindowBytes,
			flowLimit:                 normalized.flowLimit,
			closed:                    make(chan struct{}),
		}
		p.log(context.Background(), LogLevelInfo, "dial", "udp publication ready", map[string]any{
			"local_addr":  p.LocalAddr().String(),
			"remote_addr": cfg.RemoteAddr,
			"stream_id":   cfg.StreamID,
			"session_id":  cfg.SessionID,
		}, nil)
		return p, nil
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
		transportMode:     cfg.Transport,
		conn:              conn,
		transport:         transport,
		metrics:           cfg.Metrics,
		logger:            cfg.Logger,
		streamID:          cfg.StreamID,
		sessionID:         cfg.SessionID,
		maxPayload:        cfg.MaxPayloadBytes,
		mtuPayload:        normalized.mtuPayload,
		reserved:          cfg.ReservedValue,
		transportFeedback: cfg.TransportFeedback,
		responseChannel:   cfg.ResponseChannel,
		spyEndpoint:       cfg.RemoteAddr,
		terms:             terms,
		window:            window,
		flow:              cfg.FlowControl,
		flowWindow:        cfg.PublicationWindowBytes,
		flowLimit:         normalized.flowLimit,
		closed:            make(chan struct{}),
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
	if p != nil && p.transportMode == TransportUDP {
		return p.sendUDP(ctx, payload)
	}
	return p.offerQUIC(ctx, payload, true).Err
}

func (p *Publication) offerQUIC(ctx context.Context, payload []byte, waitForWindow bool) PublicationOfferResult {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(payload) > p.maxPayload {
		err := fmt.Errorf("payload too large: %d bytes", len(payload))
		p.metrics.incSendErrors()
		p.log(ctx, LogLevelWarn, "send", "send rejected", map[string]any{
			"bytes":     len(payload),
			"max_bytes": p.maxPayload,
		}, err)
		return publicationOfferError(OfferPayloadTooLarge, err)
	}

	select {
	case <-p.closed:
		p.metrics.incSendErrors()
		p.log(ctx, LogLevelWarn, "send", "send on closed publication", nil, ErrClosed)
		return publicationOfferError(OfferClosed, ErrClosed)
	default:
	}

	firstPayloadOverhead, err := responseChannelPayloadOverhead(p.responseChannel)
	if err != nil {
		p.metrics.incSendErrors()
		p.log(ctx, LogLevelError, "send", "response channel encode failed", nil, err)
		return publicationOfferError(OfferFailed, err)
	}
	fragmentCount, err := countFragmentsWithFirstOverhead(len(payload), p.mtuPayload, firstPayloadOverhead)
	if err != nil {
		p.metrics.incSendErrors()
		p.log(ctx, LogLevelWarn, "send", "send rejected", map[string]any{
			"bytes": len(payload),
		}, err)
		return publicationOfferError(OfferPayloadTooLarge, err)
	}
	windowBytes := fragmentedWindowBytesWithFirstOverhead(len(payload), p.mtuPayload, firstPayloadOverhead)
	var backPressured bool
	if waitForWindow {
		backPressured, err = p.window.reserve(ctx, windowBytes, p.closed)
		if backPressured {
			p.metrics.incBackPressureEvents()
			p.log(ctx, LogLevelWarn, "send", "publication back pressured", map[string]any{
				"bytes": windowBytes,
			}, nil)
		}
	} else if err = p.window.tryReserve(windowBytes, p.closed); errors.Is(err, ErrBackPressure) {
		backPressured = true
		p.metrics.incBackPressureEvents()
	}
	if err != nil {
		p.metrics.incSendErrors()
		p.log(ctx, LogLevelWarn, "send", "window reservation failed", map[string]any{
			"bytes": windowBytes,
		}, err)
		if errors.Is(err, ErrClosed) {
			return publicationOfferError(OfferClosed, err)
		}
		if backPressured || errors.Is(err, ErrBackPressure) {
			return publicationOfferError(OfferBackPressured, err)
		}
		return publicationOfferError(OfferFailed, err)
	}
	defer p.window.release(windowBytes)

	seq := p.nextSeq.Add(1)
	var reserved uint64
	if p.reserved != nil {
		reserved = p.reserved(payload)
	}
	packet, appendResult, err := p.encodeDataPacket(seq, reserved, payload, fragmentCount, p.responseChannel, firstPayloadOverhead)
	if err != nil {
		p.metrics.incSendErrors()
		p.log(ctx, LogLevelError, "send", "encode failed", map[string]any{
			"sequence": seq,
			"bytes":    len(payload),
		}, err)
		return publicationOfferError(OfferFailed, err)
	}

	resp, err := p.roundTrip(ctx, packet, fragmentCount)
	if err != nil {
		p.metrics.incSendErrors()
		p.log(ctx, LogLevelWarn, "send", "round trip failed", map[string]any{
			"sequence":  seq,
			"bytes":     len(payload),
			"fragments": fragmentCount,
		}, err)
		return publicationOfferError(OfferFailed, err)
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
			return publicationOfferError(OfferFailed, err)
		}
		if err := p.updateFlowControl(appendResult.Position); err != nil {
			p.metrics.incSendErrors()
			p.log(ctx, LogLevelError, "send", "flow control update failed", map[string]any{
				"sequence": seq,
				"position": appendResult.Position,
			}, err)
			return publicationOfferError(OfferFailed, err)
		}
		p.metrics.incMessagesSent(len(payload))
		p.metrics.incAcksReceived()
		p.publishLocalSpies(payload, seq, reserved, appendResult, []string{p.spyEndpoint})
		p.log(ctx, LogLevelDebug, "send", "message sent", map[string]any{
			"sequence":  seq,
			"bytes":     len(payload),
			"fragments": fragmentCount,
			"position":  appendResult.Position,
		}, nil)
		return PublicationOfferResult{Status: OfferAccepted, Position: appendResult.Position}
	case frameError:
		p.metrics.incProtocolErrors()
		p.metrics.incSendErrors()
		err := decodeProtocolError(resp.payload)
		p.log(ctx, LogLevelError, "send", "peer returned protocol error", map[string]any{
			"sequence": seq,
		}, err)
		return publicationOfferError(OfferFailed, err)
	default:
		p.metrics.incFramesDropped(1)
		p.metrics.incProtocolErrors()
		p.metrics.incSendErrors()
		err := &ProtocolError{Code: uint16(protocolErrorUnsupportedType), Message: "unexpected response frame"}
		p.log(ctx, LogLevelError, "send", "unexpected response frame", map[string]any{
			"sequence": seq,
			"type":     resp.typ,
		}, err)
		return publicationOfferError(OfferFailed, err)
	}
}

func (p *Publication) updateFlowControl(receiverPosition int64) error {
	return p.updateFlowControlStatus(FlowControlStatus{
		ReceiverID:   p.receiverID(),
		Position:     receiverPosition,
		WindowLength: p.flowWindow,
		ObservedAt:   time.Now(),
	})
}

func (p *Publication) updateFlowControlStatus(status FlowControlStatus) error {
	p.flowMu.Lock()
	defer p.flowMu.Unlock()

	if status.ReceiverID == "" {
		status.ReceiverID = p.receiverID()
	}
	if status.WindowLength <= 0 {
		status.WindowLength = p.flowWindow
	}
	if status.ObservedAt.IsZero() {
		status.ObservedAt = time.Now()
	}
	p.flowLimit = p.flow.OnStatus(status, p.flowLimit)

	limit := int(p.flowLimit - status.Position)
	if limit <= 0 {
		limit = 1
	}
	return p.window.setLimit(limit)
}

func (p *Publication) receiverID() string {
	if p.conn != nil {
		return remoteAddrString(p.conn.RemoteAddr())
	}
	if p.udpRemote != nil {
		return remoteAddrString(p.udpRemote)
	}
	return ""
}

func (p *Publication) encodeDataPacket(seq, reserved uint64, payload []byte, fragmentCount int, response ResponseChannel, firstPayloadOverhead int) ([]byte, termAppend, error) {
	packet := make([]byte, 0, fragmentedPacketBytesWithFirstOverhead(len(payload), p.mtuPayload, firstPayloadOverhead))
	var lastAppend termAppend
	payloadOffset := 0

	for fragmentIndex := 0; fragmentIndex < fragmentCount; fragmentIndex++ {
		fragmentPayloadMax := p.mtuPayload
		if fragmentIndex == 0 {
			fragmentPayloadMax -= firstPayloadOverhead
		}
		if fragmentPayloadMax < 0 {
			return nil, termAppend{}, fmt.Errorf("response channel metadata exceeds MTU payload: %d bytes", firstPayloadOverhead)
		}
		remaining := len(payload) - payloadOffset
		fragmentPayloadLen := min(fragmentPayloadMax, remaining)
		fragmentPayload := payload[payloadOffset : payloadOffset+fragmentPayloadLen]
		payloadOffset += fragmentPayloadLen
		flags := frameFlag(0)
		if fragmentCount > 1 {
			flags = frameFlagFragment
		}
		if fragmentIndex == 0 && !response.IsZero() {
			var err error
			fragmentPayload, err = encodeDataPayload(response, fragmentPayload)
			if err != nil {
				return nil, termAppend{}, err
			}
			flags |= frameFlagResponseChannel
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
	if payloadOffset != len(payload) {
		return nil, termAppend{}, fmt.Errorf("payload fragmentation incomplete: wrote %d of %d bytes", payloadOffset, len(payload))
	}

	return packet, lastAppend, nil
}

func countFragments(payloadLen, fragmentPayloadMax int) int {
	count, _ := countFragmentsWithFirstOverhead(payloadLen, fragmentPayloadMax, 0)
	return count
}

func responseChannelPayloadOverhead(response ResponseChannel) (int, error) {
	if response.IsZero() {
		return 0, nil
	}
	payload, err := encodeResponseChannelPayload(response)
	if err != nil {
		return 0, err
	}
	if len(payload) > int(^uint16(0)) {
		return 0, fmt.Errorf("response channel payload too large: %d bytes", len(payload))
	}
	return 2 + len(payload), nil
}

func countFragmentsWithFirstOverhead(payloadLen, fragmentPayloadMax, firstPayloadOverhead int) (int, error) {
	if fragmentPayloadMax <= 0 {
		return 0, fmt.Errorf("invalid fragment payload max: %d", fragmentPayloadMax)
	}
	if firstPayloadOverhead < 0 || firstPayloadOverhead > fragmentPayloadMax {
		return 0, fmt.Errorf("response channel metadata exceeds MTU payload: %d bytes", firstPayloadOverhead)
	}
	if payloadLen == 0 {
		return 1, nil
	}
	firstFragmentPayloadMax := fragmentPayloadMax - firstPayloadOverhead
	if payloadLen <= firstFragmentPayloadMax {
		return 1, nil
	}
	remaining := payloadLen - firstFragmentPayloadMax
	count := 1 + (remaining+fragmentPayloadMax-1)/fragmentPayloadMax
	if count > maxFrameFragments {
		return 0, fmt.Errorf("payload requires too many fragments: %d", count)
	}
	return count, nil
}

func fragmentedWindowBytes(payloadLen, fragmentPayloadMax int) int {
	return fragmentedWindowBytesWithFirstOverhead(payloadLen, fragmentPayloadMax, 0)
}

func fragmentedWindowBytesWithFirstOverhead(payloadLen, fragmentPayloadMax, firstPayloadOverhead int) int {
	count, err := countFragmentsWithFirstOverhead(payloadLen, fragmentPayloadMax, firstPayloadOverhead)
	if err != nil {
		return 0
	}
	total := 0
	payloadOffset := 0
	for i := 0; i < count; i++ {
		currentPayloadMax := fragmentPayloadMax
		metadataBytes := 0
		if i == 0 {
			currentPayloadMax -= firstPayloadOverhead
			metadataBytes = firstPayloadOverhead
		}
		remaining := payloadLen - payloadOffset
		fragmentPayloadLen := min(currentPayloadMax, remaining)
		payloadOffset += fragmentPayloadLen
		total += align(headerLen+metadataBytes+fragmentPayloadLen, termFrameAlignment)
	}
	return total
}

func fragmentedPacketBytes(payloadLen, fragmentPayloadMax int) int {
	return fragmentedPacketBytesWithFirstOverhead(payloadLen, fragmentPayloadMax, 0)
}

func fragmentedPacketBytesWithFirstOverhead(payloadLen, fragmentPayloadMax, firstPayloadOverhead int) int {
	count, err := countFragmentsWithFirstOverhead(payloadLen, fragmentPayloadMax, firstPayloadOverhead)
	if err != nil {
		return 0
	}
	return payloadLen + firstPayloadOverhead + count*headerLen
}

func (p *Publication) LocalAddr() net.Addr {
	if p.udpConn != nil {
		return p.udpConn.LocalAddr()
	}
	return p.conn.LocalAddr()
}

func (p *Publication) ChannelURI() ChannelURI {
	if p == nil {
		return ChannelURI{}
	}
	if p.transportMode == TransportUDP {
		p.udpMu.Lock()
		defer p.udpMu.Unlock()
		ch := ChannelURI{
			Transport:              p.transportMode,
			NameResolutionInterval: p.udpNameResolutionInterval,
		}
		if len(p.udpDestinationOrder) > 0 {
			if destination := p.udpDestinations[p.udpDestinationOrder[0]]; destination != nil {
				ch.Endpoint = destination.endpoint
			}
			for _, key := range p.udpDestinationOrder[1:] {
				if destination := p.udpDestinations[key]; destination != nil {
					ch.AddDestination(destination.endpoint)
				}
			}
		}
		return ch
	}
	return ChannelURI{
		Transport: p.transportMode,
		Endpoint:  p.conn.RemoteAddr().String(),
	}
}

func (p *Publication) Close() error {
	var err error
	p.closeOnce.Do(func() {
		close(p.closed)
		if p.udpConn != nil {
			if p.udpOwnConn {
				err = p.udpConn.Close()
			} else {
				err = p.udpConn.SetReadDeadline(time.Now())
			}
		} else {
			err = p.conn.CloseWithError(0, "closed")
		}
		if p.transport != nil {
			if transportErr := p.transport.Close(); err == nil {
				err = transportErr
			}
		}
		if p.terms != nil {
			err = errors.Join(err, p.terms.close())
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

func (p *Publication) observeTransportFeedback(feedback TransportFeedback) {
	if feedback.Transport == "" {
		feedback.Transport = p.transportMode
	}
	if feedback.Remote == "" {
		feedback.Remote = p.receiverID()
	}
	if feedback.StreamID == 0 {
		feedback.StreamID = p.streamID
	}
	if feedback.SessionID == 0 {
		feedback.SessionID = p.sessionID
	}
	if feedback.ObservedAt.IsZero() {
		feedback.ObservedAt = time.Now()
	}
	if feedback.RTT > 0 {
		p.metrics.observeRTT(feedback.RTT)
	}
	if p.transportFeedback != nil {
		p.transportFeedback(feedback)
	}
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
