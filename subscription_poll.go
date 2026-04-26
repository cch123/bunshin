package bunshin

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/quic-go/quic-go"
)

// Poll pulls at most one complete message from the subscription. Fragmented UDP
// messages may consume multiple DATA frames before the handler is invoked.
func (s *Subscription) Poll(ctx context.Context, handler Handler) (int, error) {
	return s.poll(ctx, 1, maxFrameFragments, handler, nil)
}

// PollN pulls messages until fragmentLimit DATA frames have been consumed or no
// more work can be made before ctx is done. The returned count is the number of
// complete messages delivered to handler.
func (s *Subscription) PollN(ctx context.Context, fragmentLimit int, handler Handler) (int, error) {
	return s.poll(ctx, fragmentLimit, fragmentLimit, handler, nil)
}

func (s *Subscription) poll(ctx context.Context, messageLimit, fragmentLimit int, handler Handler, shouldStop func() bool) (int, error) {
	if s == nil {
		return 0, ErrClosed
	}
	if messageLimit <= 0 || fragmentLimit <= 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if handler == nil {
		err := errors.New("handler is required")
		s.log(ctx, LogLevelWarn, "poll", "poll rejected", nil, err)
		return 0, err
	}

	if s.localSpy {
		return s.pollLocalSpy(ctx, messageLimit, fragmentLimit, handler, shouldStop)
	}
	if s.transportMode == TransportUDP {
		return s.pollUDP(ctx, messageLimit, fragmentLimit, handler, shouldStop)
	}
	return s.pollQUIC(ctx, messageLimit, fragmentLimit, handler, shouldStop)
}

func (s *Subscription) pollLocalSpy(ctx context.Context, messageLimit, fragmentLimit int, handler Handler, shouldStop func() bool) (int, error) {
	delivered := 0
	fragments := 0
	for delivered < messageLimit && fragments < fragmentLimit && !pollShouldStop(shouldStop) {
		select {
		case <-s.closed:
			if delivered > 0 || fragments > 0 {
				return delivered, nil
			}
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			return 0, ErrClosed
		case <-ctx.Done():
			if delivered > 0 || fragments > 0 {
				return delivered, nil
			}
			return 0, ctx.Err()
		case msg := <-s.localSpyCh:
			fragments++
			s.metrics.incFramesReceived(1)
			if err := s.ordered.deliver(ctx, orderedMessage{
				ctx:      ctx,
				msg:      msg,
				position: msg.Position,
			}, handler); err != nil {
				if !errors.Is(err, ErrBackPressure) {
					s.metrics.incReceiveErrors()
				}
				return delivered, err
			}
			delivered++
		}
	}
	return delivered, nil
}

func (s *Subscription) pollUDP(ctx context.Context, messageLimit, fragmentLimit int, handler Handler, shouldStop func() bool) (int, error) {
	delivered := 0
	fragments := 0
	buf := make([]byte, maxFrameSize)
	for delivered < messageLimit && fragments < fragmentLimit && !pollShouldStop(shouldStop) {
		if err := s.udpConn.SetReadDeadline(udpDeadline(ctx)); err != nil {
			return delivered, err
		}
		n, remote, err := s.udpConn.ReadFrom(buf)
		if err != nil {
			select {
			case <-s.closed:
				if delivered > 0 || fragments > 0 {
					return delivered, nil
				}
				if ctx.Err() != nil {
					return 0, ctx.Err()
				}
				return 0, ErrClosed
			default:
			}
			if ctx.Err() != nil {
				if delivered > 0 || fragments > 0 {
					return delivered, nil
				}
				return 0, ctx.Err()
			}
			if timeout, ok := err.(interface{ Timeout() bool }); ok && timeout.Timeout() {
				if delivered > 0 || fragments > 0 {
					return delivered, nil
				}
				continue
			}
			s.log(ctx, LogLevelError, "poll", "udp read failed", nil, err)
			return delivered, err
		}
		s.observeUDPPeer(remote)
		f, err := decodeFrame(buf[:n])
		if err != nil {
			s.metrics.incReceiveErrors()
			s.metrics.incProtocolErrors()
			s.metrics.incFramesDropped(1)
			_ = s.writeUDPError(remote, 0, 0, 0, protocolErrorMalformedFrame, err.Error())
			return delivered, err
		}
		s.metrics.incFramesReceived(1)
		switch f.typ {
		case frameHello:
			if err := s.writeUDPHello(remote, f); err != nil {
				return delivered, err
			}
		case frameData:
			fragments++
			ready, err := s.deliverUDPData(ctx, remote, f, handler)
			if err != nil {
				if !errors.Is(err, ErrBackPressure) {
					s.metrics.incReceiveErrors()
				}
				return delivered, err
			}
			if ready {
				delivered++
			}
		default:
			s.metrics.incReceiveErrors()
			s.metrics.incProtocolErrors()
			s.metrics.incFramesDropped(1)
			err := s.writeUDPError(remote, f.streamID, f.sessionID, f.seq, protocolErrorUnsupportedType, "unsupported frame type")
			if err != nil {
				return delivered, err
			}
			return delivered, &ProtocolError{Code: uint16(protocolErrorUnsupportedType), Message: "unsupported frame type"}
		}
	}
	return delivered, nil
}

func (s *Subscription) pollQUIC(ctx context.Context, messageLimit, fragmentLimit int, handler Handler, shouldStop func() bool) (int, error) {
	delivered := 0
	fragments := 0
	for delivered < messageLimit && fragments < fragmentLimit && !pollShouldStop(shouldStop) {
		conn, err := s.pollQUICConn(ctx)
		if err != nil {
			if delivered > 0 || fragments > 0 {
				return delivered, nil
			}
			return delivered, err
		}
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			select {
			case <-s.closed:
				s.clearPollQUICConn(conn)
				if delivered > 0 || fragments > 0 {
					return delivered, nil
				}
				if ctx.Err() != nil {
					return 0, ctx.Err()
				}
				return 0, ErrClosed
			default:
			}
			if ctx.Err() != nil {
				if delivered > 0 || fragments > 0 {
					return delivered, nil
				}
				return 0, ctx.Err()
			}
			s.clearPollQUICConn(conn)
			return delivered, err
		}
		dataFrames, ready, err := s.pollQUICStream(ctx, conn.RemoteAddr(), stream, handler)
		fragments += dataFrames
		if err != nil {
			return delivered, err
		}
		if ready {
			delivered++
		}
	}
	return delivered, nil
}

func pollShouldStop(shouldStop func() bool) bool {
	return shouldStop != nil && shouldStop()
}

func (s *Subscription) pollQUICConn(ctx context.Context) (*quic.Conn, error) {
	s.pollMu.Lock()
	conn := s.pollConn
	s.pollMu.Unlock()
	if conn != nil {
		return conn, nil
	}

	conn, err := s.listener.Accept(ctx)
	if err != nil {
		select {
		case <-s.closed:
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, ErrClosed
		default:
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}

	s.pollMu.Lock()
	if s.pollConn == nil {
		s.pollConn = conn
		s.pollMu.Unlock()
		s.metrics.incConnectionsAccepted()
		s.log(ctx, LogLevelDebug, "poll", "connection accepted", map[string]any{
			"remote_addr": conn.RemoteAddr().String(),
		}, nil)
		return conn, nil
	}
	existing := s.pollConn
	s.pollMu.Unlock()
	_ = conn.CloseWithError(0, "unused")
	return existing, nil
}

func (s *Subscription) clearPollQUICConn(conn *quic.Conn) {
	s.pollMu.Lock()
	if s.pollConn == conn {
		s.pollConn = nil
	}
	s.pollMu.Unlock()
}

func (s *Subscription) pollQUICStream(ctx context.Context, remote net.Addr, stream *quic.Stream, handler Handler) (int, bool, error) {
	buf, err := io.ReadAll(stream)
	if err != nil {
		s.log(ctx, LogLevelWarn, "poll", "stream read failed", map[string]any{
			"remote_addr": remoteAddrString(remote),
		}, err)
		return 0, false, err
	}

	frames, err := decodeFrames(buf)
	if err != nil {
		s.metrics.incReceiveErrors()
		s.metrics.incProtocolErrors()
		s.metrics.incFramesDropped(1)
		_ = s.writeError(stream, 0, 0, 0, protocolErrorMalformedFrame, err.Error())
		return 0, false, err
	}
	if len(frames) == 0 {
		s.metrics.incReceiveErrors()
		s.metrics.incProtocolErrors()
		s.metrics.incFramesDropped(1)
		err := errors.New("empty frame stream")
		_ = s.writeError(stream, 0, 0, 0, protocolErrorMalformedFrame, err.Error())
		return 0, false, err
	}
	s.metrics.incFramesReceived(len(frames))

	f := frames[0]
	switch f.typ {
	case frameHello:
		if len(frames) != 1 {
			s.metrics.incReceiveErrors()
			s.metrics.incProtocolErrors()
			s.metrics.incFramesDropped(len(frames))
			err := errors.New("control stream contains multiple frames")
			_ = s.writeError(stream, f.streamID, f.sessionID, f.seq, protocolErrorMalformedFrame, err.Error())
			return 0, false, err
		}
		return 0, false, s.hello(stream, f)
	case frameData:
		ready, err := s.deliverQUICData(ctx, remote, stream, frames, handler)
		return len(frames), ready, err
	default:
		s.metrics.incReceiveErrors()
		s.metrics.incProtocolErrors()
		s.metrics.incFramesDropped(len(frames))
		_ = s.writeError(stream, f.streamID, f.sessionID, f.seq, protocolErrorUnsupportedType, "unsupported frame type")
		return 0, false, &ProtocolError{Code: uint16(protocolErrorUnsupportedType), Message: "unsupported frame type"}
	}
}
