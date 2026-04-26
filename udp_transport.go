package bunshin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
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

type udpPendingDestination struct {
	addr      net.Addr
	multicast bool
}

type udpDestination struct {
	endpoint string
	addr     net.Addr
}

func listenUDPTransport(localAddr, remoteAddr, multicastInterface string, packetConn net.PacketConn) (net.PacketConn, net.Addr, error) {
	remote, err := net.ResolveUDPAddr("udp", remoteAddr)
	if err != nil {
		return nil, nil, err
	}
	ifi, err := udpMulticastInterface(multicastInterface)
	if err != nil {
		return nil, nil, err
	}
	if packetConn != nil {
		if err := configureUDPMulticastPublisher(packetConn, remote, ifi); err != nil {
			return nil, nil, err
		}
		return packetConn, remote, nil
	}
	if localAddr == "" {
		localAddr = ":0"
	}
	network := "udp"
	if remote.IP != nil && remote.IP.IsMulticast() {
		network = udpNetwork(remote)
	}
	conn, err := net.ListenPacket(network, localAddr)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil && packetConn == nil && conn != nil {
			_ = conn.Close()
		}
	}()
	if err = configureUDPMulticastPublisher(conn, remote, ifi); err != nil {
		return nil, nil, err
	}
	return conn, remote, nil
}

func listenUDPSubscription(localAddr, multicastInterface string, packetConn net.PacketConn) (net.PacketConn, error) {
	if packetConn != nil {
		return packetConn, nil
	}
	addr, err := net.ResolveUDPAddr("udp", localAddr)
	if err != nil {
		return nil, err
	}
	if addr.IP != nil && addr.IP.IsMulticast() {
		ifi, err := udpMulticastInterface(multicastInterface)
		if err != nil {
			return nil, err
		}
		return net.ListenMulticastUDP(udpNetwork(addr), ifi, addr)
	}
	return net.ListenPacket("udp", localAddr)
}

func configureUDPMulticastPublisher(conn net.PacketConn, remote net.Addr, ifi *net.Interface) error {
	udpRemote, ok := remote.(*net.UDPAddr)
	if !ok || udpRemote.IP == nil || !udpRemote.IP.IsMulticast() {
		return nil
	}
	udpConn, ok := conn.(*net.UDPConn)
	if !ok {
		return nil
	}
	if udpRemote.IP.To4() != nil {
		ipv4Conn := ipv4.NewPacketConn(udpConn)
		if ifi != nil {
			if err := ipv4Conn.SetMulticastInterface(ifi); err != nil {
				return err
			}
		}
		if err := ipv4Conn.SetMulticastLoopback(true); err != nil {
			return err
		}
		return ipv4Conn.SetMulticastTTL(1)
	}
	ipv6Conn := ipv6.NewPacketConn(udpConn)
	if ifi != nil {
		if err := ipv6Conn.SetMulticastInterface(ifi); err != nil {
			return err
		}
	}
	if err := ipv6Conn.SetMulticastLoopback(true); err != nil {
		return err
	}
	return ipv6Conn.SetMulticastHopLimit(1)
}

func udpMulticastInterface(name string) (*net.Interface, error) {
	if name == "" {
		return nil, nil
	}
	return net.InterfaceByName(name)
}

func udpNetwork(addr *net.UDPAddr) string {
	if addr != nil && addr.IP != nil && addr.IP.To4() != nil {
		return "udp4"
	}
	if addr != nil && addr.IP != nil && addr.IP.To16() != nil {
		return "udp6"
	}
	return "udp"
}

func isMulticastUDPAddr(addr net.Addr) bool {
	udpAddr, ok := addr.(*net.UDPAddr)
	return ok && udpAddr.IP != nil && udpAddr.IP.IsMulticast()
}

func resolveUDPDestinations(primaryEndpoint string, primary net.Addr, extra []string) (map[string]*udpDestination, []string, error) {
	destinations := make(map[string]*udpDestination, 1+len(extra))
	order := make([]string, 0, 1+len(extra))
	if primary != nil {
		key := udpDestinationKey(primaryEndpoint, primary)
		destinations[key] = &udpDestination{
			endpoint: key,
			addr:     primary,
		}
		order = append(order, key)
	}
	for _, raw := range extra {
		remote, err := net.ResolveUDPAddr("udp", raw)
		if err != nil {
			return nil, nil, err
		}
		key := udpDestinationKey(raw, remote)
		if _, ok := destinations[key]; ok {
			continue
		}
		destinations[key] = &udpDestination{
			endpoint: key,
			addr:     remote,
		}
		order = append(order, key)
	}
	return destinations, order, nil
}

func udpDestinationKey(endpoint string, remote net.Addr) string {
	if endpoint != "" {
		return endpoint
	}
	return remoteAddrString(remote)
}

func (p *Publication) AddDestination(remoteAddr string) error {
	if p == nil || p.transportMode != TransportUDP {
		return fmt.Errorf("%w: add destination requires UDP transport", ErrInvalidConfig)
	}
	remote, err := net.ResolveUDPAddr("udp", remoteAddr)
	if err != nil {
		return err
	}
	select {
	case <-p.closed:
		return ErrClosed
	default:
	}

	p.udpMu.Lock()
	defer p.udpMu.Unlock()
	if p.udpDestinations == nil {
		p.udpDestinations = make(map[string]*udpDestination)
	}
	if err := p.configureUDPDestination(remote); err != nil {
		return err
	}
	key := udpDestinationKey(remoteAddr, remote)
	if _, ok := p.udpDestinations[key]; ok {
		return nil
	}
	p.udpDestinations[key] = &udpDestination{
		endpoint: key,
		addr:     remote,
	}
	p.udpDestinationOrder = append(p.udpDestinationOrder, key)
	if p.udpRemote == nil {
		p.udpRemote = remote
	}
	return nil
}

func (p *Publication) RemoveDestination(remoteAddr string) error {
	if p == nil || p.transportMode != TransportUDP {
		return fmt.Errorf("%w: remove destination requires UDP transport", ErrInvalidConfig)
	}
	remote, err := net.ResolveUDPAddr("udp", remoteAddr)
	if err != nil {
		return err
	}
	select {
	case <-p.closed:
		return ErrClosed
	default:
	}

	p.udpMu.Lock()
	defer p.udpMu.Unlock()
	key, ok := p.udpDestinationKeyForRemoval(remoteAddr, remote)
	if !ok {
		return nil
	}
	removed := p.udpDestinations[key]
	removedPrimary := removed != nil && remoteAddrString(p.udpRemote) == remoteAddrString(removed.addr)
	delete(p.udpDestinations, key)
	for i, existing := range p.udpDestinationOrder {
		if existing == key {
			p.udpDestinationOrder = append(p.udpDestinationOrder[:i], p.udpDestinationOrder[i+1:]...)
			break
		}
	}
	if removedPrimary {
		p.udpRemote = nil
		if len(p.udpDestinationOrder) > 0 {
			if destination := p.udpDestinations[p.udpDestinationOrder[0]]; destination != nil {
				p.udpRemote = destination.addr
			}
		}
	}
	return nil
}

func (p *Publication) udpDestinationKeyForRemoval(endpoint string, remote net.Addr) (string, bool) {
	if _, ok := p.udpDestinations[endpoint]; ok {
		return endpoint, true
	}
	remoteID := remoteAddrString(remote)
	for key, destination := range p.udpDestinations {
		if destination != nil && remoteAddrString(destination.addr) == remoteID {
			return key, true
		}
	}
	return "", false
}

func (p *Publication) Destinations() []string {
	if p == nil || p.transportMode != TransportUDP {
		return nil
	}
	p.udpMu.Lock()
	defer p.udpMu.Unlock()
	destinations := make([]string, 0, len(p.udpDestinationOrder))
	for _, key := range p.udpDestinationOrder {
		if destination := p.udpDestinations[key]; destination != nil && destination.addr != nil {
			destinations = append(destinations, remoteAddrString(destination.addr))
		}
	}
	return destinations
}

func (p *Publication) udpDestinationSnapshot() []net.Addr {
	destinations := make([]net.Addr, 0, len(p.udpDestinationOrder))
	for _, key := range p.udpDestinationOrder {
		destination := p.udpDestinations[key]
		if destination != nil && destination.addr != nil {
			destinations = append(destinations, destination.addr)
		}
	}
	return destinations
}

func (p *Publication) DestinationEndpoints() []string {
	if p == nil || p.transportMode != TransportUDP {
		return nil
	}
	p.udpMu.Lock()
	defer p.udpMu.Unlock()

	endpoints := make([]string, 0, len(p.udpDestinationOrder))
	for _, key := range p.udpDestinationOrder {
		if destination := p.udpDestinations[key]; destination != nil {
			endpoints = append(endpoints, destination.endpoint)
		}
	}
	return endpoints
}

func (p *Publication) ReResolveDestinations() error {
	if p == nil || p.transportMode != TransportUDP {
		return fmt.Errorf("%w: re-resolve destinations requires UDP transport", ErrInvalidConfig)
	}
	select {
	case <-p.closed:
		return ErrClosed
	default:
	}
	p.udpMu.Lock()
	defer p.udpMu.Unlock()
	return p.reResolveUDPDestinationsLocked(time.Now(), true)
}

func (p *Publication) reResolveUDPDestinationsIfDueLocked(now time.Time) error {
	if p.udpNameResolutionInterval <= 0 {
		return nil
	}
	if !p.udpNextNameResolution.IsZero() && now.Before(p.udpNextNameResolution) {
		return nil
	}
	return p.reResolveUDPDestinationsLocked(now, false)
}

func (p *Publication) reResolveUDPDestinationsLocked(now time.Time, force bool) error {
	if len(p.udpDestinationOrder) == 0 {
		return nil
	}
	for _, key := range p.udpDestinationOrder {
		destination := p.udpDestinations[key]
		if destination == nil {
			continue
		}
		remote, err := net.ResolveUDPAddr("udp", destination.endpoint)
		if err != nil {
			return err
		}
		if err := p.configureUDPDestination(remote); err != nil {
			return err
		}
		destination.addr = remote
	}
	if first := p.udpDestinations[p.udpDestinationOrder[0]]; first != nil {
		p.udpRemote = first.addr
	}
	if p.udpNameResolutionInterval > 0 && (!force || !now.IsZero()) {
		p.udpNextNameResolution = now.Add(p.udpNameResolutionInterval)
	}
	return nil
}

func (p *Publication) configureUDPDestination(remote net.Addr) error {
	if p == nil || p.udpConn == nil {
		return nil
	}
	ifi, err := udpMulticastInterface(p.udpMulticastInterface)
	if err != nil {
		return err
	}
	return configureUDPMulticastPublisher(p.udpConn, remote, ifi)
}

func (p *Publication) sendUDP(ctx context.Context, payload []byte) error {
	return p.offerUDP(ctx, payload, true).Err
}

func (p *Publication) offerUDP(ctx context.Context, payload []byte, waitForWindow bool) PublicationOfferResult {
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
		return publicationOfferError(OfferPayloadTooLarge, err)
	}
	select {
	case <-p.closed:
		p.metrics.incSendErrors()
		p.log(ctx, LogLevelWarn, "send", "send on closed udp publication", nil, ErrClosed)
		return publicationOfferError(OfferClosed, ErrClosed)
	default:
	}
	if err := p.reResolveUDPDestinationsIfDueLocked(time.Now()); err != nil {
		p.metrics.incSendErrors()
		return publicationOfferError(OfferFailed, err)
	}
	destinations := p.udpDestinationSnapshot()
	if len(destinations) == 0 {
		err := fmt.Errorf("%w: UDP publication has no destinations", ErrInvalidConfig)
		p.metrics.incSendErrors()
		return publicationOfferError(OfferFailed, err)
	}

	firstPayloadOverhead, err := responseChannelPayloadOverhead(p.responseChannel)
	if err != nil {
		p.metrics.incSendErrors()
		return publicationOfferError(OfferFailed, err)
	}
	fragmentCount, err := countFragmentsWithFirstOverhead(len(payload), p.mtuPayload, firstPayloadOverhead)
	if err != nil {
		p.metrics.incSendErrors()
		return publicationOfferError(OfferPayloadTooLarge, err)
	}
	windowBytes := fragmentedWindowBytesWithFirstOverhead(len(payload), p.mtuPayload, firstPayloadOverhead)
	var backPressured bool
	if waitForWindow {
		backPressured, err = p.window.reserve(ctx, windowBytes, p.closed)
		if backPressured {
			p.metrics.incBackPressureEvents()
		}
	} else if err = p.window.tryReserve(windowBytes, p.closed); errors.Is(err, ErrBackPressure) {
		backPressured = true
		p.metrics.incBackPressureEvents()
	}
	if err != nil {
		p.metrics.incSendErrors()
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
		return publicationOfferError(OfferFailed, err)
	}
	frames, err := decodeFrames(packet)
	if err != nil {
		p.metrics.incSendErrors()
		return publicationOfferError(OfferFailed, err)
	}
	datagrams := make([][]byte, 0, len(frames))
	for _, f := range frames {
		encoded, err := encodeFrame(f)
		if err != nil {
			p.metrics.incSendErrors()
			return publicationOfferError(OfferFailed, err)
		}
		datagrams = append(datagrams, encoded)
	}
	p.cacheUDPRetransmit(seq, datagrams)
	sentAt := time.Now()
	for _, remote := range destinations {
		if err := p.writeUDPDatagrams(remote, datagrams); err != nil {
			p.metrics.incSendErrors()
			return publicationOfferError(OfferFailed, err)
		}
	}

	acks, err := p.readUDPResponses(ctx, appendResult, seq, destinations, sentAt)
	if err != nil {
		p.metrics.incSendErrors()
		return publicationOfferError(OfferFailed, err)
	}
	p.metrics.incMessagesSent(len(payload))
	for i := 0; i < acks; i++ {
		p.metrics.incAcksReceived()
	}
	p.publishLocalSpies(payload, seq, reserved, appendResult, p.udpDestinationEndpointSnapshot())
	return PublicationOfferResult{Status: OfferAccepted, Position: appendResult.Position}
}

func (p *Publication) readUDPResponses(ctx context.Context, appendResult termAppend, seq uint64, destinations []net.Addr, sentAt time.Time) (int, error) {
	buf := make([]byte, maxFrameSize)
	pending := make(map[string]udpPendingDestination, len(destinations))
	statusApplied := make(map[string]bool, len(destinations))
	for _, remote := range destinations {
		pending[remoteAddrString(remote)] = udpPendingDestination{
			addr:      remote,
			multicast: isMulticastUDPAddr(remote),
		}
	}
	acks := 0
	for {
		if len(pending) == 0 {
			return acks, nil
		}
		if err := p.udpConn.SetReadDeadline(udpDeadline(ctx)); err != nil {
			return acks, err
		}
		n, remote, err := p.udpConn.ReadFrom(buf)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return acks, ctxErr
			}
			return acks, err
		}
		remoteID := remoteAddrString(remote)
		pendingID := remoteID
		if _, ok := pending[pendingID]; !ok {
			pendingID = pendingMulticastDestinationForRemote(pending, remote)
		}
		if pendingID == "" {
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
				return acks, err
			}
			statusApplied[remoteID] = true
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
			if !statusApplied[remoteID] {
				if err := p.updateFlowControlStatus(FlowControlStatus{
					ReceiverID:   remoteID,
					Position:     appendResult.Position,
					WindowLength: p.flowWindow,
					ObservedAt:   time.Now(),
				}); err != nil {
					return acks, err
				}
			}
			delete(pending, pendingID)
			acks++
			p.observeTransportFeedback(TransportFeedback{
				Transport: TransportUDP,
				Remote:    remoteID,
				Sequence:  seq,
				RTT:       time.Since(sentAt),
			})
			continue
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
			p.metrics.incProtocolErrors()
			return acks, decodeProtocolError(f.payload)
		case frameNak:
			if f.streamID != p.streamID || f.sessionID != p.sessionID {
				p.metrics.incFramesDropped(1)
				p.metrics.incProtocolErrors()
				continue
			}
			if err := p.applyUDPNak(remote, f); err != nil {
				p.metrics.incFramesDropped(1)
				p.metrics.incProtocolErrors()
				return acks, err
			}
			continue
		default:
			p.metrics.incFramesDropped(1)
			p.metrics.incProtocolErrors()
			continue
		}
	}
}

func pendingMulticastDestinationForRemote(pending map[string]udpPendingDestination, remote net.Addr) string {
	remoteUDP, _ := remote.(*net.UDPAddr)
	for id, destination := range pending {
		if !destination.multicast {
			continue
		}
		destinationUDP, _ := destination.addr.(*net.UDPAddr)
		if remoteUDP == nil || destinationUDP == nil || remoteUDP.Port == destinationUDP.Port {
			return id
		}
	}
	return ""
}

func (p *Publication) udpDestinationEndpointSnapshot() []string {
	endpoints := make([]string, 0, len(p.udpDestinationOrder))
	for _, key := range p.udpDestinationOrder {
		if destination := p.udpDestinations[key]; destination != nil {
			endpoints = append(endpoints, destination.endpoint)
		}
	}
	return endpoints
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
					if !errors.Is(err, ErrBackPressure) {
						s.metrics.incReceiveErrors()
					}
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
	_, err := s.deliverUDPData(ctx, remote, f, handler)
	return err
}

func (s *Subscription) deliverUDPData(ctx context.Context, remote net.Addr, f frame, handler Handler) (bool, error) {
	if f.streamID != s.streamID {
		s.metrics.incProtocolErrors()
		s.metrics.incFramesDropped(1)
		err := s.writeUDPError(remote, f.streamID, f.sessionID, f.seq, protocolErrorUnsupportedType, "unsupported stream id")
		if err != nil {
			return false, err
		}
		return false, &ProtocolError{Code: uint16(protocolErrorUnsupportedType), Message: "unsupported stream id"}
	}
	observations := s.loss.observe(f, remote)
	for _, observation := range observations {
		if err := s.writeUDPNak(remote, observation); err != nil {
			return false, err
		}
	}

	payload, responseChannel, msgFrame, ackFrame, ready, err := s.collectUDPData(remote, f)
	if err != nil {
		s.metrics.incProtocolErrors()
		s.metrics.incFramesDropped(1)
		writeErr := s.writeUDPError(remote, f.streamID, f.sessionID, f.seq, protocolErrorMalformedFrame, err.Error())
		if writeErr != nil {
			return false, writeErr
		}
		return false, err
	}
	if !ready {
		return false, nil
	}
	msg := Message{
		StreamID:        msgFrame.streamID,
		SessionID:       msgFrame.sessionID,
		TermID:          msgFrame.termID,
		TermOffset:      msgFrame.termOffset,
		Sequence:        msgFrame.seq,
		ReservedValue:   msgFrame.reserved,
		Payload:         payload,
		Remote:          remote,
		ResponseChannel: responseChannel,
	}
	positionTermID, positionTermOffset, hasFramePosition := framePosition(ackFrame)
	if err := s.ordered.deliver(ctx, orderedMessage{
		ctx:                ctx,
		msg:                msg,
		positionTermID:     positionTermID,
		positionTermOffset: positionTermOffset,
		hasFramePosition:   hasFramePosition,
		ack: func() error {
			return s.ackUDP(remote, ackFrame)
		},
		fail: func(err error) error {
			return s.writeUDPError(remote, msg.StreamID, msg.SessionID, msg.Sequence, protocolErrorMalformedFrame, err.Error())
		},
	}, handler); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Subscription) collectUDPData(remote net.Addr, f frame) ([]byte, ResponseChannel, frame, frame, bool, error) {
	if f.fragmentCount <= 1 {
		responseChannel, payload, err := decodeDataPayload(f)
		if err != nil {
			return nil, ResponseChannel{}, frame{}, frame{}, false, err
		}
		return cloneBytes(payload), responseChannel, f, f, true, nil
	}
	if f.flags&frameFlagFragment == 0 {
		return nil, ResponseChannel{}, frame{}, frame{}, false, errors.New("fragment flag missing")
	}
	fragmentCount := int(f.fragmentCount)
	if int(f.fragmentIndex) >= fragmentCount {
		return nil, ResponseChannel{}, frame{}, frame{}, false, errors.New("invalid fragment metadata")
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
		return nil, ResponseChannel{}, frame{}, frame{}, false, errors.New("fragment count changed")
	}
	index := int(f.fragmentIndex)
	if !set.received[index] {
		set.frames[index] = cloneUDPFrame(f)
		set.received[index] = true
		set.count++
	}
	if set.count < fragmentCount {
		return nil, ResponseChannel{}, frame{}, frame{}, false, nil
	}

	payload, responseChannel, ackFrame, err := reassembleDataFrames(set.frames)
	if err != nil {
		delete(s.udpFragments, key)
		return nil, ResponseChannel{}, frame{}, frame{}, false, err
	}
	msgFrame := set.frames[0]
	delete(s.udpFragments, key)
	return payload, responseChannel, msgFrame, ackFrame, true, nil
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
