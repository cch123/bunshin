package bunshin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

const (
	defaultUDPTransportResponseTimeout = time.Second
	defaultUDPRetransmitBufferBytes    = 1024 * 1024
	maxUDPNakRange                     = 1024
	maxUDPStatusTermOffset             = 1<<31 - 1
)

type udpRetransmitEntry struct {
	datagrams [][]byte
	bytes     int
}

type udpFragmentKey struct {
	source    string
	streamID  uint32
	sessionID uint32
	seq       uint64
	reserved  uint64
}

type udpFragmentSet struct {
	frames   []frame
	received []bool
	count    int
}

func listenUDPTransport(localAddr, remoteAddr string, packetConn net.PacketConn) (net.PacketConn, net.Addr, error) {
	remote, err := net.ResolveUDPAddr("udp", remoteAddr)
	if err != nil {
		return nil, nil, err
	}
	if packetConn != nil {
		return packetConn, remote, nil
	}
	if localAddr == "" {
		localAddr = ":0"
	}
	conn, err := net.ListenPacket("udp", localAddr)
	if err != nil {
		return nil, nil, err
	}
	return conn, remote, nil
}

func listenUDPSubscription(localAddr string, packetConn net.PacketConn) (net.PacketConn, error) {
	if packetConn != nil {
		return packetConn, nil
	}
	return net.ListenPacket("udp", localAddr)
}

func (p *Publication) sendUDP(ctx context.Context, payload []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	p.udpMu.Lock()
	defer p.udpMu.Unlock()

	if len(payload) > p.maxPayload {
		err := fmt.Errorf("payload too large: %d bytes", len(payload))
		p.metrics.incSendErrors()
		p.log(ctx, LogLevelWarn, "send", "udp send rejected", map[string]any{
			"bytes":     len(payload),
			"max_bytes": p.maxPayload,
		}, err)
		return err
	}
	select {
	case <-p.closed:
		p.metrics.incSendErrors()
		p.log(ctx, LogLevelWarn, "send", "send on closed udp publication", nil, ErrClosed)
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
	frames, err := decodeFrames(packet)
	if err != nil {
		p.metrics.incSendErrors()
		return err
	}
	datagrams := make([][]byte, 0, len(frames))
	for _, f := range frames {
		encoded, err := encodeFrame(f)
		if err != nil {
			p.metrics.incSendErrors()
			return err
		}
		datagrams = append(datagrams, encoded)
	}
	p.cacheUDPRetransmit(seq, datagrams)
	sentAt := time.Now()
	if err := p.writeUDPDatagrams(p.udpRemote, datagrams); err != nil {
		p.metrics.incSendErrors()
		return err
	}

	resp, statusApplied, err := p.readUDPResponse(ctx, appendResult, seq)
	if err != nil {
		p.metrics.incSendErrors()
		return err
	}
	switch resp.typ {
	case frameAck:
		if !statusApplied {
			err = p.updateFlowControl(appendResult.Position)
		}
		if err != nil {
			p.metrics.incSendErrors()
			return err
		}
		p.metrics.incMessagesSent(len(payload))
		p.metrics.incAcksReceived()
		p.observeTransportFeedback(TransportFeedback{
			Transport: TransportUDP,
			Sequence:  seq,
			RTT:       time.Since(sentAt),
		})
		return nil
	case frameError:
		p.metrics.incProtocolErrors()
		p.metrics.incSendErrors()
		return decodeProtocolError(resp.payload)
	default:
		p.metrics.incFramesDropped(1)
		p.metrics.incProtocolErrors()
		p.metrics.incSendErrors()
		return &ProtocolError{Code: uint16(protocolErrorUnsupportedType), Message: "unexpected udp response frame"}
	}
}

func (p *Publication) readUDPResponse(ctx context.Context, appendResult termAppend, seq uint64) (frame, bool, error) {
	buf := make([]byte, maxFrameSize)
	statusApplied := false
	for {
		if err := p.udpConn.SetReadDeadline(udpDeadline(ctx)); err != nil {
			return frame{}, statusApplied, err
		}
		n, remote, err := p.udpConn.ReadFrom(buf)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return frame{}, statusApplied, ctxErr
			}
			return frame{}, statusApplied, err
		}
		if !sameUDPAddr(remote, p.udpRemote) {
			continue
		}
		f, err := decodeFrame(buf[:n])
		if err != nil {
			p.metrics.incFramesDropped(1)
			continue
		}
		p.metrics.incFramesReceived(1)
		switch f.typ {
		case frameStatus:
			if f.streamID != p.streamID || f.sessionID != p.sessionID {
				p.metrics.incFramesDropped(1)
				p.metrics.incProtocolErrors()
				continue
			}
			if f.seq != seq {
				p.metrics.incFramesDropped(1)
				continue
			}
			if err := p.applyUDPStatus(remote, f); err != nil {
				p.metrics.incFramesDropped(1)
				p.metrics.incProtocolErrors()
				return frame{}, statusApplied, err
			}
			statusApplied = true
			continue
		case frameAck:
			if f.streamID != p.streamID || f.sessionID != p.sessionID {
				p.metrics.incFramesDropped(1)
				p.metrics.incProtocolErrors()
				continue
			}
			if f.seq != seq {
				p.metrics.incFramesDropped(1)
				continue
			}
			if f.termID != appendResult.TermID || f.termOffset != appendResult.TermOffset {
				p.metrics.incFramesDropped(1)
				p.metrics.incProtocolErrors()
				continue
			}
		case frameError:
			if f.streamID != p.streamID || f.sessionID != p.sessionID {
				p.metrics.incFramesDropped(1)
				p.metrics.incProtocolErrors()
				continue
			}
			if f.seq != seq {
				p.metrics.incFramesDropped(1)
				continue
			}
		case frameNak:
			if f.streamID != p.streamID || f.sessionID != p.sessionID {
				p.metrics.incFramesDropped(1)
				p.metrics.incProtocolErrors()
				continue
			}
			if err := p.applyUDPNak(remote, f); err != nil {
				p.metrics.incFramesDropped(1)
				p.metrics.incProtocolErrors()
				return frame{}, statusApplied, err
			}
			continue
		}
		return f, statusApplied, nil
	}
}

func (p *Publication) writeUDPDatagrams(remote net.Addr, datagrams [][]byte) error {
	for _, datagram := range datagrams {
		if _, err := p.udpConn.WriteTo(datagram, remote); err != nil {
			return err
		}
		p.metrics.incFramesSent(1)
	}
	return nil
}

func (p *Publication) cacheUDPRetransmit(seq uint64, datagrams [][]byte) {
	if p.udpRetransmitLimit <= 0 || len(datagrams) == 0 {
		return
	}
	entry := udpRetransmitEntry{
		datagrams: make([][]byte, 0, len(datagrams)),
	}
	for _, datagram := range datagrams {
		clone := cloneBytes(datagram)
		entry.datagrams = append(entry.datagrams, clone)
		entry.bytes += len(clone)
	}
	if entry.bytes > p.udpRetransmitLimit {
		return
	}
	if existing, ok := p.udpRetransmit[seq]; ok {
		p.udpRetransmitBytes -= existing.bytes
	} else {
		p.udpRetransmitOrder = append(p.udpRetransmitOrder, seq)
	}
	p.udpRetransmit[seq] = entry
	p.udpRetransmitBytes += entry.bytes
	for p.udpRetransmitBytes > p.udpRetransmitLimit && len(p.udpRetransmitOrder) > 0 {
		evictSeq := p.udpRetransmitOrder[0]
		p.udpRetransmitOrder = p.udpRetransmitOrder[1:]
		evicted, ok := p.udpRetransmit[evictSeq]
		if !ok {
			continue
		}
		delete(p.udpRetransmit, evictSeq)
		p.udpRetransmitBytes -= evicted.bytes
	}
}

func (p *Publication) applyUDPStatus(remote net.Addr, f frame) error {
	payload, err := decodeStatusPayload(f.payload)
	if err != nil {
		return err
	}
	position, err := p.terms.position(f.termID, f.termOffset)
	if err != nil {
		return err
	}
	return p.updateFlowControlStatus(FlowControlStatus{
		ReceiverID:   remoteAddrString(remote),
		Position:     position,
		WindowLength: payload.windowLength,
		ObservedAt:   time.Now(),
	})
}

func (p *Publication) applyUDPNak(remote net.Addr, f frame) error {
	nak, err := decodeNakPayload(f.payload)
	if err != nil {
		return err
	}
	if nak.toSequence-nak.fromSequence+1 > maxUDPNakRange {
		return fmt.Errorf("nak range too large: %d-%d", nak.fromSequence, nak.toSequence)
	}
	for seq := nak.fromSequence; seq <= nak.toSequence; seq++ {
		entry := p.udpRetransmit[seq]
		if len(entry.datagrams) == 0 {
			continue
		}
		if err := p.writeUDPDatagrams(remote, entry.datagrams); err != nil {
			return err
		}
		p.metrics.incRetransmits(len(entry.datagrams))
		p.observeTransportFeedback(TransportFeedback{
			Transport:           TransportUDP,
			Remote:              remoteAddrString(remote),
			Sequence:            seq,
			RetransmittedFrames: len(entry.datagrams),
		})
	}
	return nil
}

func (s *Subscription) serveUDP(ctx context.Context, handler Handler) error {
	if handler == nil {
		err := errors.New("handler is required")
		s.log(ctx, LogLevelWarn, "serve", "udp serve rejected", nil, err)
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	stop := context.AfterFunc(ctx, func() {
		_ = s.Close()
	})
	defer stop()

	buf := make([]byte, maxFrameSize)
	for {
		if err := s.udpConn.SetReadDeadline(udpDeadline(ctx)); err != nil {
			return err
		}
		n, remote, err := s.udpConn.ReadFrom(buf)
		if err != nil {
			select {
			case <-s.closed:
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return ErrClosed
			default:
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if timeout, ok := err.(interface{ Timeout() bool }); ok && timeout.Timeout() {
				continue
			}
			s.log(ctx, LogLevelError, "udp", "udp read failed", nil, err)
			return err
		}
		s.observeUDPPeer(remote)
		f, err := decodeFrame(buf[:n])
		if err != nil {
			s.metrics.incReceiveErrors()
			s.metrics.incProtocolErrors()
			s.metrics.incFramesDropped(1)
			_ = s.writeUDPError(remote, 0, 0, 0, protocolErrorMalformedFrame, err.Error())
			continue
		}
		s.metrics.incFramesReceived(1)
		switch f.typ {
		case frameHello:
			_ = s.writeUDPHello(remote, f)
		case frameData:
			go func(remote net.Addr, f frame) {
				if err := s.dataUDP(ctx, remote, f, handler); err != nil {
					s.metrics.incReceiveErrors()
				}
			}(remote, f)
		default:
			s.metrics.incReceiveErrors()
			s.metrics.incProtocolErrors()
			s.metrics.incFramesDropped(1)
			_ = s.writeUDPError(remote, f.streamID, f.sessionID, f.seq, protocolErrorUnsupportedType, "unsupported frame type")
		}
	}
}

func (s *Subscription) observeUDPPeer(remote net.Addr) {
	if remote == nil {
		return
	}
	source := remoteAddrString(remote)
	s.udpMu.Lock()
	defer s.udpMu.Unlock()
	if _, ok := s.udpPeers[source]; ok {
		return
	}
	s.udpPeers[source] = struct{}{}
	s.metrics.incConnectionsAccepted()
}

func (s *Subscription) dataUDP(ctx context.Context, remote net.Addr, f frame, handler Handler) error {
	if f.streamID != s.streamID {
		s.metrics.incProtocolErrors()
		s.metrics.incFramesDropped(1)
		return s.writeUDPError(remote, f.streamID, f.sessionID, f.seq, protocolErrorUnsupportedType, "unsupported stream id")
	}
	observations := s.loss.observe(f, remote)
	for _, observation := range observations {
		if err := s.writeUDPNak(remote, observation); err != nil {
			return err
		}
	}

	payload, msgFrame, ackFrame, ready, err := s.collectUDPData(remote, f)
	if err != nil {
		s.metrics.incProtocolErrors()
		s.metrics.incFramesDropped(1)
		return s.writeUDPError(remote, f.streamID, f.sessionID, f.seq, protocolErrorMalformedFrame, err.Error())
	}
	if !ready {
		return nil
	}
	msg := Message{
		StreamID:      msgFrame.streamID,
		SessionID:     msgFrame.sessionID,
		TermID:        msgFrame.termID,
		TermOffset:    msgFrame.termOffset,
		Sequence:      msgFrame.seq,
		ReservedValue: msgFrame.reserved,
		Payload:       payload,
		Remote:        remote,
	}
	return s.ordered.deliver(ctx, orderedMessage{
		ctx: ctx,
		msg: msg,
		ack: func() error {
			return s.ackUDP(remote, ackFrame)
		},
		fail: func(err error) error {
			return s.writeUDPError(remote, msg.StreamID, msg.SessionID, msg.Sequence, protocolErrorMalformedFrame, err.Error())
		},
	}, handler)
}

func (s *Subscription) collectUDPData(remote net.Addr, f frame) ([]byte, frame, frame, bool, error) {
	if f.fragmentCount <= 1 {
		return cloneBytes(f.payload), f, f, true, nil
	}
	if f.flags&frameFlagFragment == 0 {
		return nil, frame{}, frame{}, false, errors.New("fragment flag missing")
	}
	fragmentCount := int(f.fragmentCount)
	if int(f.fragmentIndex) >= fragmentCount {
		return nil, frame{}, frame{}, false, errors.New("invalid fragment metadata")
	}
	key := udpFragmentKey{
		source:    remoteAddrString(remote),
		streamID:  f.streamID,
		sessionID: f.sessionID,
		seq:       f.seq,
		reserved:  f.reserved,
	}

	s.udpMu.Lock()
	defer s.udpMu.Unlock()
	set := s.udpFragments[key]
	if set == nil {
		set = &udpFragmentSet{
			frames:   make([]frame, fragmentCount),
			received: make([]bool, fragmentCount),
		}
		s.udpFragments[key] = set
	}
	if len(set.frames) != fragmentCount {
		delete(s.udpFragments, key)
		return nil, frame{}, frame{}, false, errors.New("fragment count changed")
	}
	index := int(f.fragmentIndex)
	if !set.received[index] {
		set.frames[index] = cloneUDPFrame(f)
		set.received[index] = true
		set.count++
	}
	if set.count < fragmentCount {
		return nil, frame{}, frame{}, false, nil
	}

	payload := make([]byte, 0)
	for i := 0; i < fragmentCount; i++ {
		if !set.received[i] {
			return nil, frame{}, frame{}, false, errors.New("missing fragment")
		}
		payload = append(payload, set.frames[i].payload...)
	}
	msgFrame := set.frames[0]
	ackFrame := set.frames[fragmentCount-1]
	delete(s.udpFragments, key)
	return payload, msgFrame, ackFrame, true, nil
}

func (s *Subscription) ackUDP(remote net.Addr, f frame) error {
	if err := s.writeUDPStatus(remote, f); err != nil {
		return err
	}
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
	if _, err := s.udpConn.WriteTo(packet, remote); err != nil {
		return err
	}
	s.metrics.incFramesSent(1)
	s.metrics.incAcksSent()
	return nil
}

func (s *Subscription) writeUDPStatus(remote net.Addr, f frame) error {
	termOffset, err := udpReceiverPositionTermOffset(f)
	if err != nil {
		return err
	}
	packet, err := encodeFrame(frame{
		typ:        frameStatus,
		streamID:   f.streamID,
		sessionID:  f.sessionID,
		termID:     f.termID,
		termOffset: termOffset,
		seq:        f.seq,
		payload: encodeStatusPayload(statusPayload{
			windowLength: s.receiverWindow,
		}),
	})
	if err != nil {
		return err
	}
	if _, err := s.udpConn.WriteTo(packet, remote); err != nil {
		return err
	}
	s.metrics.incFramesSent(1)
	return nil
}

func (s *Subscription) writeUDPNak(remote net.Addr, observation LossObservation) error {
	for from := observation.FromSequence; from <= observation.ToSequence; {
		to := from + maxUDPNakRange - 1
		if to > observation.ToSequence || to < from {
			to = observation.ToSequence
		}
		packet, err := encodeFrame(frame{
			typ:       frameNak,
			streamID:  observation.StreamID,
			sessionID: observation.SessionID,
			seq:       to,
			payload: encodeNakPayload(nakPayload{
				fromSequence: from,
				toSequence:   to,
			}),
		})
		if err != nil {
			return err
		}
		if _, err := s.udpConn.WriteTo(packet, remote); err != nil {
			return err
		}
		s.metrics.incFramesSent(1)
		if to == observation.ToSequence {
			return nil
		}
		from = to + 1
	}
	return nil
}

func (s *Subscription) writeUDPHello(remote net.Addr, f frame) error {
	hello, err := decodeHelloPayload(f.payload)
	if err != nil {
		return s.writeUDPError(remote, f.streamID, f.sessionID, f.seq, protocolErrorMalformedFrame, err.Error())
	}
	if hello.minVersion > frameVersion || hello.maxVersion < frameVersion {
		return s.writeUDPError(remote, f.streamID, f.sessionID, f.seq, protocolErrorUnsupportedVersion, "unsupported protocol version")
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
	if _, err := s.udpConn.WriteTo(packet, remote); err != nil {
		return err
	}
	s.metrics.incFramesSent(1)
	return nil
}

func (s *Subscription) writeUDPError(remote net.Addr, streamID, sessionID uint32, seq uint64, code protocolErrorCode, message string) error {
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
	if _, err := s.udpConn.WriteTo(packet, remote); err != nil {
		return err
	}
	s.metrics.incFramesSent(1)
	return nil
}

func udpDeadline(ctx context.Context) time.Time {
	deadline := time.Now().Add(defaultUDPTransportResponseTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		return ctxDeadline
	}
	return deadline
}

func sameUDPAddr(a, b net.Addr) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.String() == b.String()
}

func cloneUDPFrame(f frame) frame {
	f.payload = cloneBytes(f.payload)
	return f
}

func udpReceiverPositionTermOffset(f frame) (int32, error) {
	if f.termOffset < 0 {
		return 0, fmt.Errorf("invalid term offset: %d", f.termOffset)
	}
	nextOffset := int64(f.termOffset) + int64(align(headerLen+len(f.payload), termFrameAlignment))
	if nextOffset > maxUDPStatusTermOffset {
		return 0, fmt.Errorf("invalid receiver position term offset: %d", nextOffset)
	}
	return int32(nextOffset), nil
}
