package bunshin

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

const quicALPN = "bunshin/4"

type Message struct {
	StreamID        uint32
	SessionID       uint32
	TermID          int32
	TermOffset      int32
	Position        int64
	Sequence        uint64
	ReservedValue   uint64
	Payload         []byte
	Remote          net.Addr
	ResponseChannel ResponseChannel
	Image           *Image
}

type Handler func(context.Context, Message) error

type SubscriptionConfig struct {
	Transport              TransportMode
	StreamID               uint32
	LocalAddr              string
	TLSConfig              *tls.Config
	QUICConfig             *quic.Config
	Metrics                *Metrics
	Logger                 Logger
	LossHandler            LossHandler
	AvailableImage         ImageHandler
	UnavailableImage       ImageHandler
	UDPMulticastInterface  string
	UDPNakRetryInterval    time.Duration
	LocalSpy               bool
	LocalSpyBuffer         int
	Archive                *Archive
	ReceiverWindowBytes    int
	TermBufferLength       int
	DriverDataRingCapacity int

	// PacketConn is an advanced hook for tests and custom transports. When set, LocalAddr is ignored and the caller owns closing it.
	PacketConn net.PacketConn

	// Kept for API compatibility. QUIC owns socket buffers.
	ReadBufferBytes  int
	WriteBufferBytes int
}

type UDPSubscriptionPeerStatus struct {
	Remote              string    `json:"remote"`
	Active              bool      `json:"active"`
	FramesReceived      int       `json:"frames_received,omitempty"`
	HelloFramesReceived int       `json:"hello_frames_received,omitempty"`
	DataFramesReceived  int       `json:"data_frames_received,omitempty"`
	HelloFramesSent     int       `json:"hello_frames_sent,omitempty"`
	StatusFramesSent    int       `json:"status_frames_sent,omitempty"`
	AckFramesSent       int       `json:"ack_frames_sent,omitempty"`
	NAKFramesSent       int       `json:"nak_frames_sent,omitempty"`
	NAKMessagesSent     uint64    `json:"nak_messages_sent,omitempty"`
	NAKRetriesSent      int       `json:"nak_retries_sent,omitempty"`
	ErrorFramesSent     int       `json:"error_frames_sent,omitempty"`
	LastFrameAt         time.Time `json:"last_frame_at,omitempty"`
	LastHelloAt         time.Time `json:"last_hello_at,omitempty"`
	LastDataAt          time.Time `json:"last_data_at,omitempty"`
	LastHelloSentAt     time.Time `json:"last_hello_sent_at,omitempty"`
	LastStatusAt        time.Time `json:"last_status_at,omitempty"`
	LastAckAt           time.Time `json:"last_ack_at,omitempty"`
	LastNAKAt           time.Time `json:"last_nak_at,omitempty"`
	LastNAKRetryAt      time.Time `json:"last_nak_retry_at,omitempty"`
	LastErrorAt         time.Time `json:"last_error_at,omitempty"`
	LastSequence        uint64    `json:"last_sequence,omitempty"`
	LastNAKFromSequence uint64    `json:"last_nak_from_sequence,omitempty"`
	LastNAKToSequence   uint64    `json:"last_nak_to_sequence,omitempty"`
}

type Subscription struct {
	transportMode            TransportMode
	listener                 *quic.Listener
	udpConn                  net.PacketConn
	udpOwnConn               bool
	udpMu                    sync.Mutex
	udpFragments             map[udpFragmentKey]*udpFragmentSet
	udpPeers                 map[string]*udpPeer
	udpNakRetryInterval      time.Duration
	localSpy                 bool
	localSpyID               uint64
	localSpyKey              localSpyKey
	localSpyCh               chan Message
	localSpyAddr             net.Addr
	pollMu                   sync.Mutex
	pollConn                 *quic.Conn
	imagesMu                 sync.Mutex
	images                   map[lossKey]*Image
	transport                *quic.Transport
	metrics                  *Metrics
	logger                   Logger
	loss                     *lossDetector
	availableImage           ImageHandler
	unavailableImage         ImageHandler
	archive                  *Archive
	ordered                  *orderedDelivery
	streamID                 uint32
	receiverWindow           int
	imageTermLength          int
	imagePositionBitsToShift uint
	closed                   chan struct{}
	closeOnce                sync.Once
}

func ListenSubscription(cfg SubscriptionConfig) (*Subscription, error) {
	cfg, err := normalizeSubscriptionConfig(cfg)
	if err != nil {
		return nil, err
	}

	if cfg.LocalSpy {
		sub := &Subscription{
			transportMode:            cfg.Transport,
			localSpy:                 true,
			localSpyKey:              newLocalSpyKey(cfg.Transport, cfg.StreamID, cfg.LocalAddr),
			localSpyCh:               make(chan Message, cfg.LocalSpyBuffer),
			localSpyAddr:             localSpyAddr(cfg.LocalAddr),
			metrics:                  cfg.Metrics,
			logger:                   cfg.Logger,
			loss:                     newLossDetector(cfg.Metrics, cfg.LossHandler),
			udpNakRetryInterval:      cfg.UDPNakRetryInterval,
			availableImage:           cfg.AvailableImage,
			unavailableImage:         cfg.UnavailableImage,
			archive:                  cfg.Archive,
			streamID:                 cfg.StreamID,
			receiverWindow:           cfg.ReceiverWindowBytes,
			imageTermLength:          cfg.TermBufferLength,
			imagePositionBitsToShift: termPositionBitsToShift(cfg.TermBufferLength),
			closed:                   make(chan struct{}),
		}
		sub.ordered = newOrderedDelivery(sub)
		registerLocalSpy(sub)
		sub.log(context.Background(), LogLevelInfo, "listen", "local spy subscription listening", map[string]any{
			"transport":  cfg.Transport,
			"local_addr": cfg.LocalAddr,
			"stream_id":  sub.streamID,
		}, nil)
		return sub, nil
	}

	if cfg.Transport == TransportUDP {
		conn, err := listenUDPSubscription(cfg.LocalAddr, cfg.UDPMulticastInterface, cfg.PacketConn)
		if err != nil {
			logEvent(context.Background(), cfg.Logger, LogEvent{
				Level:     LogLevelError,
				Component: "subscription",
				Operation: "listen",
				Message:   "udp listen failed",
				Fields: map[string]any{
					"local_addr": cfg.LocalAddr,
					"stream_id":  cfg.StreamID,
				},
				Err: err,
			})
			return nil, fmt.Errorf("listen udp subscription: %w", err)
		}
		sub := &Subscription{
			transportMode:            cfg.Transport,
			udpConn:                  conn,
			udpOwnConn:               cfg.PacketConn == nil,
			udpFragments:             make(map[udpFragmentKey]*udpFragmentSet),
			udpPeers:                 make(map[string]*udpPeer),
			metrics:                  cfg.Metrics,
			logger:                   cfg.Logger,
			loss:                     newLossDetector(cfg.Metrics, cfg.LossHandler),
			udpNakRetryInterval:      cfg.UDPNakRetryInterval,
			availableImage:           cfg.AvailableImage,
			unavailableImage:         cfg.UnavailableImage,
			archive:                  cfg.Archive,
			streamID:                 cfg.StreamID,
			receiverWindow:           cfg.ReceiverWindowBytes,
			imageTermLength:          cfg.TermBufferLength,
			imagePositionBitsToShift: termPositionBitsToShift(cfg.TermBufferLength),
			closed:                   make(chan struct{}),
		}
		sub.ordered = newOrderedDelivery(sub)
		sub.log(context.Background(), LogLevelInfo, "listen", "udp subscription listening", map[string]any{
			"local_addr": sub.LocalAddr().String(),
			"stream_id":  sub.streamID,
		}, nil)
		return sub, nil
	}

	listener, transport, err := listenQUIC(cfg.LocalAddr, cfg.TLSConfig, cfg.QUICConfig, cfg.PacketConn)
	if err != nil {
		logEvent(context.Background(), cfg.Logger, LogEvent{
			Level:     LogLevelError,
			Component: "subscription",
			Operation: "listen",
			Message:   "listen failed",
			Fields: map[string]any{
				"local_addr": cfg.LocalAddr,
				"stream_id":  cfg.StreamID,
			},
			Err: err,
		})
		return nil, fmt.Errorf("listen quic subscription: %w", err)
	}

	sub := &Subscription{
		transportMode:            cfg.Transport,
		listener:                 listener,
		transport:                transport,
		metrics:                  cfg.Metrics,
		logger:                   cfg.Logger,
		loss:                     newLossDetector(cfg.Metrics, cfg.LossHandler),
		udpNakRetryInterval:      cfg.UDPNakRetryInterval,
		availableImage:           cfg.AvailableImage,
		unavailableImage:         cfg.UnavailableImage,
		archive:                  cfg.Archive,
		streamID:                 cfg.StreamID,
		receiverWindow:           cfg.ReceiverWindowBytes,
		imageTermLength:          cfg.TermBufferLength,
		imagePositionBitsToShift: termPositionBitsToShift(cfg.TermBufferLength),
		closed:                   make(chan struct{}),
	}
	sub.ordered = newOrderedDelivery(sub)
	sub.log(context.Background(), LogLevelInfo, "listen", "subscription listening", map[string]any{
		"local_addr": sub.LocalAddr().String(),
		"stream_id":  sub.streamID,
	}, nil)
	return sub, nil
}

func (s *Subscription) Serve(ctx context.Context, handler Handler) error {
	if s != nil && s.localSpy {
		return s.serveLocalSpy(ctx, handler)
	}
	if s != nil && s.transportMode == TransportUDP {
		return s.serveUDP(ctx, handler)
	}
	if handler == nil {
		err := errors.New("handler is required")
		s.log(ctx, LogLevelWarn, "serve", "serve rejected", nil, err)
		return err
	}

	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	defer wg.Wait()

	go func() {
		for {
			conn, err := s.listener.Accept(ctx)
			if err != nil {
				select {
				case <-s.closed:
					errCh <- ErrClosed
				case <-ctx.Done():
					errCh <- ctx.Err()
				default:
					s.log(ctx, LogLevelError, "accept", "accept failed", nil, err)
					errCh <- err
				}
				return
			}
			s.metrics.incConnectionsAccepted()
			s.log(ctx, LogLevelDebug, "accept", "connection accepted", map[string]any{
				"remote_addr": conn.RemoteAddr().String(),
			}, nil)
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.serveConn(ctx, conn, handler)
			}()
		}
	}()

	select {
	case <-ctx.Done():
		_ = s.Close()
		s.log(ctx, LogLevelInfo, "serve", "serve context done", nil, ctx.Err())
		return ctx.Err()
	case err := <-errCh:
		if errors.Is(err, ErrClosed) && ctx.Err() != nil {
			s.log(ctx, LogLevelInfo, "serve", "serve context done", nil, ctx.Err())
			return ctx.Err()
		}
		if !errors.Is(err, ErrClosed) {
			s.log(ctx, LogLevelError, "serve", "serve stopped", nil, err)
		}
		return err
	}
}

func (s *Subscription) LocalAddr() net.Addr {
	if s.localSpy {
		return s.localSpyAddr
	}
	if s.udpConn != nil {
		return s.udpConn.LocalAddr()
	}
	return s.listener.Addr()
}

func (s *Subscription) ChannelURI() ChannelURI {
	if s == nil {
		return ChannelURI{}
	}
	if s.localSpy {
		return ChannelURI{
			Transport: s.transportMode,
			Endpoint:  s.localSpyKey.endpoint,
			Spy:       true,
		}
	}
	return ChannelURI{
		Transport: s.transportMode,
		Endpoint:  s.LocalAddr().String(),
	}
}

func (s *Subscription) LossReports() []LossReport {
	return s.loss.snapshot()
}

func (s *Subscription) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.closed)
		if s.localSpy {
			unregisterLocalSpy(s)
		} else if s.udpConn != nil {
			if s.udpOwnConn {
				err = s.udpConn.Close()
			} else {
				err = s.udpConn.SetReadDeadline(time.Now())
			}
		} else {
			err = s.listener.Close()
		}
		s.pollMu.Lock()
		pollConn := s.pollConn
		s.pollConn = nil
		s.pollMu.Unlock()
		if pollConn != nil {
			err = errors.Join(err, pollConn.CloseWithError(0, "closed"))
		}
		if s.transport != nil {
			if transportErr := s.transport.Close(); err == nil {
				err = transportErr
			}
		}
		s.closeImages(context.Background())
		s.log(context.Background(), LogLevelInfo, "close", "subscription closed", map[string]any{
			"stream_id": s.streamID,
		}, err)
	})
	return err
}

func (s *Subscription) serveConn(ctx context.Context, conn *quic.Conn, handler Handler) {
	defer conn.CloseWithError(0, "closed")
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go s.serveStream(ctx, conn.RemoteAddr(), stream, handler)
	}
}

func (s *Subscription) serveStream(ctx context.Context, remote net.Addr, stream *quic.Stream, handler Handler) {
	buf, err := io.ReadAll(stream)
	if err != nil {
		s.log(ctx, LogLevelWarn, "stream", "stream read failed", map[string]any{
			"remote_addr": remoteAddrString(remote),
		}, err)
		return
	}

	frames, err := decodeFramesView(buf)
	if err != nil {
		s.metrics.incReceiveErrors()
		s.metrics.incProtocolErrors()
		s.metrics.incFramesDropped(1)
		_ = s.writeError(stream, 0, 0, 0, protocolErrorMalformedFrame, err.Error())
		return
	}
	if len(frames) == 0 {
		s.metrics.incReceiveErrors()
		s.metrics.incProtocolErrors()
		s.metrics.incFramesDropped(1)
		_ = s.writeError(stream, 0, 0, 0, protocolErrorMalformedFrame, "empty frame stream")
		return
	}
	s.metrics.incFramesReceived(len(frames))

	f := frames[0]
	switch f.typ {
	case frameHello:
		if len(frames) != 1 {
			s.metrics.incReceiveErrors()
			s.metrics.incProtocolErrors()
			s.metrics.incFramesDropped(len(frames))
			_ = s.writeError(stream, f.streamID, f.sessionID, f.seq, protocolErrorMalformedFrame, "control stream contains multiple frames")
			return
		}
		_ = s.hello(stream, f)
	case frameData:
		s.data(ctx, remote, stream, frames, handler)
	default:
		s.metrics.incReceiveErrors()
		s.metrics.incProtocolErrors()
		s.metrics.incFramesDropped(len(frames))
		_ = s.writeError(stream, f.streamID, f.sessionID, f.seq, protocolErrorUnsupportedType, "unsupported frame type")
	}
}

func (s *Subscription) data(ctx context.Context, remote net.Addr, stream *quic.Stream, frames []frame, handler Handler) {
	_, _ = s.deliverQUICData(ctx, remote, stream, frames, handler)
}

func (s *Subscription) deliverQUICData(ctx context.Context, remote net.Addr, stream *quic.Stream, frames []frame, handler Handler) (bool, error) {
	payload, responseChannel, ackFrame, err := reassembleDataFrames(frames)
	if err != nil {
		s.metrics.incReceiveErrors()
		s.metrics.incProtocolErrors()
		s.metrics.incFramesDropped(len(frames))
		first := frames[0]
		_ = s.writeError(stream, first.streamID, first.sessionID, first.seq, protocolErrorMalformedFrame, err.Error())
		return false, err
	}
	first := frames[0]
	if first.streamID != s.streamID {
		s.metrics.incReceiveErrors()
		s.metrics.incProtocolErrors()
		s.metrics.incFramesDropped(len(frames))
		_ = s.writeError(stream, first.streamID, first.sessionID, first.seq, protocolErrorUnsupportedType, "unsupported stream id")
		return false, &ProtocolError{Code: uint16(protocolErrorUnsupportedType), Message: "unsupported stream id"}
	}

	s.loss.observe(first, remote)
	msg := Message{
		StreamID:        first.streamID,
		SessionID:       first.sessionID,
		TermID:          first.termID,
		TermOffset:      first.termOffset,
		Sequence:        first.seq,
		ReservedValue:   first.reserved,
		Payload:         payload,
		Remote:          remote,
		ResponseChannel: responseChannel,
	}
	var rawFrames [][]byte
	if s.archive != nil {
		rawFrames, err = encodeFrameBatch(frames)
		if err != nil {
			s.metrics.incReceiveErrors()
			s.metrics.incProtocolErrors()
			s.metrics.incFramesDropped(len(frames))
			_ = s.writeError(stream, first.streamID, first.sessionID, first.seq, protocolErrorMalformedFrame, err.Error())
			return false, err
		}
	}
	positionTermID, positionTermOffset, hasFramePosition := framePosition(ackFrame)
	if err := s.ordered.deliver(ctx, orderedMessage{
		ctx:                ctx,
		msg:                msg,
		rawFrames:          rawFrames,
		positionTermID:     positionTermID,
		positionTermOffset: positionTermOffset,
		hasFramePosition:   hasFramePosition,
		ack: func() error {
			return s.ack(stream, ackFrame)
		},
		fail: func(err error) error {
			return s.writeError(stream, msg.StreamID, msg.SessionID, msg.Sequence, protocolErrorMalformedFrame, err.Error())
		},
	}, handler); err != nil {
		if !errors.Is(err, ErrBackPressure) {
			s.metrics.incReceiveErrors()
		}
		return false, err
	}
	return true, nil
}

func (s *Subscription) handleMessage(ctx context.Context, item orderedMessage, handler Handler) error {
	msg := s.observeMessageImage(ctx, item)
	if s.archive != nil {
		var (
			record ArchiveRecord
			err    error
		)
		if len(item.rawFrames) > 0 {
			records, recordErr := s.archive.RecordFrames(item.rawFrames)
			err = recordErr
			if len(records) > 0 {
				record = records[0]
			}
		} else {
			record, err = s.archive.Record(msg)
		}
		if err != nil {
			s.metrics.incReceiveErrors()
			s.log(ctx, LogLevelError, "archive", "archive record failed", map[string]any{
				"stream_id":  msg.StreamID,
				"session_id": msg.SessionID,
				"sequence":   msg.Sequence,
			}, err)
			if item.fail != nil {
				_ = item.fail(err)
			}
			return err
		}
		s.log(ctx, LogLevelDebug, "archive", "message recorded", map[string]any{
			"stream_id":        msg.StreamID,
			"session_id":       msg.SessionID,
			"sequence":         msg.Sequence,
			"archive_position": record.Position,
		}, nil)
	}
	if err := handler(ctx, msg); err != nil {
		if errors.Is(err, ErrBackPressure) {
			s.metrics.incBackPressureEvents()
			s.log(ctx, LogLevelWarn, "handler", "receiver back pressured", map[string]any{
				"stream_id":   msg.StreamID,
				"session_id":  msg.SessionID,
				"sequence":    msg.Sequence,
				"bytes":       len(msg.Payload),
				"remote_addr": remoteAddrString(msg.Remote),
			}, err)
			return err
		}
		s.metrics.incReceiveErrors()
		s.log(ctx, LogLevelWarn, "handler", "handler failed", map[string]any{
			"stream_id":   msg.StreamID,
			"session_id":  msg.SessionID,
			"sequence":    msg.Sequence,
			"bytes":       len(msg.Payload),
			"remote_addr": remoteAddrString(msg.Remote),
		}, err)
		if item.fail != nil {
			_ = item.fail(err)
		}
		return err
	}
	s.metrics.incMessagesReceived(len(msg.Payload))
	if msg.Image != nil {
		msg.Image.update(msg.Position, msg.Sequence)
		msg.Image.completeRebuild(msg.Sequence)
	}
	if item.ack == nil {
		return nil
	}
	if err := item.ack(); err != nil {
		s.metrics.incReceiveErrors()
		s.log(ctx, LogLevelWarn, "ack", "ack failed", map[string]any{
			"stream_id":  msg.StreamID,
			"session_id": msg.SessionID,
			"sequence":   msg.Sequence,
		}, err)
		return err
	}
	s.log(ctx, LogLevelDebug, "deliver", "message delivered", map[string]any{
		"stream_id":   msg.StreamID,
		"session_id":  msg.SessionID,
		"sequence":    msg.Sequence,
		"bytes":       len(msg.Payload),
		"remote_addr": remoteAddrString(msg.Remote),
	}, nil)
	return nil
}

func (s *Subscription) observeMessageImage(ctx context.Context, item orderedMessage) Message {
	msg := item.msg
	if msg.Image == nil {
		msg.Image = s.imageForMessage(ctx, msg)
	}
	if msg.Position == 0 {
		msg.Position = s.messagePosition(msg.Image, item)
	}
	if msg.Image != nil {
		msg.Image.observe(msg.Position, msg.Sequence)
	}
	return msg
}

func (s *Subscription) messagePosition(image *Image, item orderedMessage) int64 {
	if item.hasFramePosition && image != nil {
		return computeTermPosition(item.positionTermID, item.positionTermOffset, s.imagePositionBitsToShift, image.InitialTermID)
	}
	if item.position > 0 {
		return item.position
	}
	return s.imageJoinPosition(item.msg)
}

func reassembleDataFrames(frames []frame) ([]byte, ResponseChannel, frame, error) {
	if len(frames) == 0 {
		return nil, ResponseChannel{}, frame{}, errors.New("empty data frame stream")
	}
	first := frames[0]
	if len(frames) == 1 {
		if first.typ != frameData {
			return nil, ResponseChannel{}, frame{}, errors.New("non-data frame in data stream")
		}
		if first.fragmentCount > 1 {
			return nil, ResponseChannel{}, frame{}, errors.New("incomplete fragmented data stream")
		}
		responseChannel, payload, err := decodeDataPayload(first)
		if err != nil {
			return nil, ResponseChannel{}, frame{}, err
		}
		return payload, responseChannel, first, nil
	}

	fragmentCount := int(first.fragmentCount)
	if fragmentCount != len(frames) || fragmentCount < 2 {
		return nil, ResponseChannel{}, frame{}, fmt.Errorf("invalid fragment count: got %d frames, header count %d", len(frames), first.fragmentCount)
	}
	parts := make([][]byte, fragmentCount)
	seen := make([]bool, fragmentCount)
	total := 0
	ackFrame := first
	var responseChannel ResponseChannel

	for _, f := range frames {
		if f.typ != frameData {
			return nil, ResponseChannel{}, frame{}, errors.New("non-data frame in fragmented data stream")
		}
		if f.streamID != first.streamID || f.sessionID != first.sessionID || f.seq != first.seq || f.reserved != first.reserved {
			return nil, ResponseChannel{}, frame{}, errors.New("fragment metadata mismatch")
		}
		if f.flags&frameFlagFragment == 0 {
			return nil, ResponseChannel{}, frame{}, errors.New("fragment flag missing")
		}
		if int(f.fragmentCount) != fragmentCount || int(f.fragmentIndex) >= fragmentCount {
			return nil, ResponseChannel{}, frame{}, errors.New("invalid fragment metadata")
		}
		if seen[f.fragmentIndex] {
			return nil, ResponseChannel{}, frame{}, errors.New("duplicate fragment")
		}
		seen[f.fragmentIndex] = true
		fragmentPayload := f.payload
		if f.flags&frameFlagResponseChannel != 0 {
			if f.fragmentIndex != 0 {
				return nil, ResponseChannel{}, frame{}, errors.New("response channel flag on non-initial fragment")
			}
			var err error
			responseChannel, fragmentPayload, err = decodeDataPayload(f)
			if err != nil {
				return nil, ResponseChannel{}, frame{}, err
			}
		}
		parts[f.fragmentIndex] = fragmentPayload
		total += len(fragmentPayload)
		if int(f.fragmentIndex) == fragmentCount-1 {
			ackFrame = f
		}
	}

	payload := make([]byte, 0, total)
	for i, part := range parts {
		if !seen[i] {
			return nil, ResponseChannel{}, frame{}, errors.New("missing fragment")
		}
		payload = append(payload, part...)
	}
	return payload, responseChannel, ackFrame, nil
}

func (s *Subscription) hello(stream *quic.Stream, f frame) error {
	hello, err := decodeHelloPayload(f.payload)
	if err != nil {
		s.metrics.incProtocolErrors()
		s.metrics.incFramesDropped(1)
		return s.writeError(stream, f.streamID, f.sessionID, f.seq, protocolErrorMalformedFrame, err.Error())
	}
	if hello.minVersion > frameVersion || hello.maxVersion < frameVersion {
		s.metrics.incProtocolErrors()
		s.metrics.incFramesDropped(1)
		return s.writeError(stream, f.streamID, f.sessionID, f.seq, protocolErrorUnsupportedVersion, "unsupported protocol version")
	}

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
	if _, err := stream.Write(packet); err != nil {
		return err
	}
	s.metrics.incFramesSent(1)
	return stream.Close()
}

func (s *Subscription) ack(stream *quic.Stream, f frame) error {
	packet, err := encodeFrame(frame{
		typ:        frameAck,
		streamID:   f.streamID,
		sessionID:  f.sessionID,
		termID:     f.termID,
		termOffset: f.termOffset,
		seq:        f.seq,
	})
	if err != nil {
		return err
	}
	if _, err := stream.Write(packet); err != nil {
		return err
	}
	s.metrics.incFramesSent(1)
	if err := stream.Close(); err != nil {
		return err
	}
	s.metrics.incAcksSent()
	return nil
}

func (s *Subscription) writeError(stream *quic.Stream, streamID, sessionID uint32, seq uint64, code protocolErrorCode, message string) error {
	s.log(context.Background(), LogLevelWarn, "protocol", "protocol error sent", map[string]any{
		"stream_id":  streamID,
		"session_id": sessionID,
		"sequence":   seq,
		"code":       code,
		"message":    message,
	}, nil)
	packet, err := encodeFrame(frame{
		typ:       frameError,
		streamID:  streamID,
		sessionID: sessionID,
		seq:       seq,
		payload: encodeErrorPayload(errorPayload{
			code:    code,
			message: message,
		}),
	})
	if err != nil {
		return err
	}
	if _, err := stream.Write(packet); err != nil {
		return err
	}
	s.metrics.incFramesSent(1)
	return stream.Close()
}

func (s *Subscription) log(ctx context.Context, level LogLevel, operation, message string, fields map[string]any, err error) {
	logEvent(ctx, s.logger, LogEvent{
		Level:     level,
		Component: "subscription",
		Operation: operation,
		Message:   message,
		Fields:    fields,
		Err:       err,
	})
}

func defaultClientTLSConfig() *tls.Config {
	return &tls.Config{
		NextProtos:         []string{quicALPN},
		InsecureSkipVerify: true,
	}
}

func defaultServerTLSConfig() (*tls.Config, error) {
	cert, err := generateSelfSignedCert()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{quicALPN},
	}, nil
}

func generateSelfSignedCert() (tls.Certificate, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate tls key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate tls serial: %w", err)
	}

	template := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: "bunshin.local",
		},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost", "bunshin.local"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create tls cert: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("load tls key pair: %w", err)
	}
	return cert, nil
}

func listenQUIC(localAddr string, tlsConf *tls.Config, quicConf *quic.Config, packetConn net.PacketConn) (*quic.Listener, *quic.Transport, error) {
	if packetConn == nil {
		listener, err := quic.ListenAddr(localAddr, tlsConf, quicConf)
		return listener, nil, err
	}

	transport := &quic.Transport{Conn: packetConn}
	listener, err := transport.Listen(tlsConf, quicConf)
	if err != nil {
		_ = transport.Close()
		return nil, nil, err
	}
	return listener, transport, nil
}
