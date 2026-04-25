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

const quicALPN = "bunshin/3"

type Message struct {
	StreamID      uint32
	SessionID     uint32
	TermID        int32
	TermOffset    int32
	Sequence      uint64
	ReservedValue uint64
	Payload       []byte
	Remote        net.Addr
}

type Handler func(context.Context, Message) error

type SubscriptionConfig struct {
	StreamID   uint32
	LocalAddr  string
	TLSConfig  *tls.Config
	QUICConfig *quic.Config
	Metrics    *Metrics

	// PacketConn is an advanced hook for tests and custom transports. When set, LocalAddr is ignored and the caller owns closing it.
	PacketConn net.PacketConn

	// Kept for API compatibility. QUIC owns socket buffers.
	ReadBufferBytes  int
	WriteBufferBytes int
}

type Subscription struct {
	listener  *quic.Listener
	transport *quic.Transport
	metrics   *Metrics
	streamID  uint32
	closed    chan struct{}
	closeOnce sync.Once
}

func ListenSubscription(cfg SubscriptionConfig) (*Subscription, error) {
	if cfg.LocalAddr == "" && cfg.PacketConn == nil {
		return nil, errors.New("local address is required")
	}
	if cfg.StreamID == 0 {
		cfg.StreamID = 1
	}

	tlsConf := cfg.TLSConfig
	var err error
	if tlsConf == nil {
		tlsConf, err = defaultServerTLSConfig()
		if err != nil {
			return nil, err
		}
	} else {
		tlsConf = tlsConf.Clone()
		if len(tlsConf.NextProtos) == 0 {
			tlsConf.NextProtos = []string{quicALPN}
		}
	}

	listener, transport, err := listenQUIC(cfg.LocalAddr, tlsConf, cfg.QUICConfig, cfg.PacketConn)
	if err != nil {
		return nil, fmt.Errorf("listen quic subscription: %w", err)
	}

	return &Subscription{
		listener:  listener,
		transport: transport,
		metrics:   cfg.Metrics,
		streamID:  cfg.StreamID,
		closed:    make(chan struct{}),
	}, nil
}

func (s *Subscription) Serve(ctx context.Context, handler Handler) error {
	if handler == nil {
		return errors.New("handler is required")
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
					errCh <- err
				}
				return
			}
			s.metrics.incConnectionsAccepted()
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
		return ctx.Err()
	case err := <-errCh:
		if errors.Is(err, ErrClosed) && ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
}

func (s *Subscription) LocalAddr() net.Addr {
	return s.listener.Addr()
}

func (s *Subscription) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.closed)
		err = s.listener.Close()
		if s.transport != nil {
			if transportErr := s.transport.Close(); err == nil {
				err = transportErr
			}
		}
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
		return
	}

	f, err := decodeFrame(buf)
	if err != nil {
		s.metrics.incReceiveErrors()
		s.metrics.incProtocolErrors()
		_ = s.writeError(stream, 0, 0, 0, protocolErrorMalformedFrame, err.Error())
		return
	}

	switch f.typ {
	case frameHello:
		_ = s.hello(stream, f)
	case frameData:
		if f.streamID != s.streamID {
			s.metrics.incReceiveErrors()
			s.metrics.incProtocolErrors()
			_ = s.writeError(stream, f.streamID, f.sessionID, f.seq, protocolErrorUnsupportedType, "unsupported stream id")
			return
		}
		msg := Message{
			StreamID:      f.streamID,
			SessionID:     f.sessionID,
			TermID:        f.termID,
			TermOffset:    f.termOffset,
			Sequence:      f.seq,
			ReservedValue: f.reserved,
			Payload:       f.payload,
			Remote:        remote,
		}
		if err := handler(ctx, msg); err != nil {
			s.metrics.incReceiveErrors()
			_ = s.writeError(stream, f.streamID, f.sessionID, f.seq, protocolErrorMalformedFrame, err.Error())
			return
		}
		s.metrics.incMessagesReceived(len(f.payload))
		if err := s.ack(stream, f); err != nil {
			s.metrics.incReceiveErrors()
		}
	default:
		s.metrics.incReceiveErrors()
		s.metrics.incProtocolErrors()
		_ = s.writeError(stream, f.streamID, f.sessionID, f.seq, protocolErrorUnsupportedType, "unsupported frame type")
	}
}

func (s *Subscription) hello(stream *quic.Stream, f frame) error {
	hello, err := decodeHelloPayload(f.payload)
	if err != nil {
		s.metrics.incProtocolErrors()
		return s.writeError(stream, f.streamID, f.sessionID, f.seq, protocolErrorMalformedFrame, err.Error())
	}
	if hello.minVersion > frameVersion || hello.maxVersion < frameVersion {
		s.metrics.incProtocolErrors()
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
	if err := stream.Close(); err != nil {
		return err
	}
	s.metrics.incAcksSent()
	return nil
}

func (s *Subscription) writeError(stream *quic.Stream, streamID, sessionID uint32, seq uint64, code protocolErrorCode, message string) error {
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
	return stream.Close()
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
