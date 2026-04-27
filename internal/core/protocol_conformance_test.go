package core

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestProtocolConformanceUDPHelloVersionNegotiation(t *testing.T) {
	resp := exchangeUDPControlFrame(t, frame{
		typ:       frameHello,
		streamID:  11,
		sessionID: 12,
		seq:       13,
		payload: encodeHelloPayload(helloPayload{
			minVersion: 1,
			maxVersion: frameVersion + 1,
		}),
	})
	if resp.typ != frameHello {
		t.Fatalf("response type = %d, want HELLO", resp.typ)
	}
	hello, err := decodeHelloPayload(resp.payload)
	if err != nil {
		t.Fatal(err)
	}
	if hello.minVersion != frameVersion || hello.maxVersion != frameVersion {
		t.Fatalf("negotiated hello = %#v, want exact version %d", hello, frameVersion)
	}
}

func TestProtocolConformanceUDPHelloRejectsFutureOnlyVersionRange(t *testing.T) {
	resp := exchangeUDPControlFrame(t, frame{
		typ:       frameHello,
		streamID:  21,
		sessionID: 22,
		seq:       23,
		payload: encodeHelloPayload(helloPayload{
			minVersion: frameVersion + 1,
			maxVersion: frameVersion + 2,
		}),
	})
	assertProtocolErrorFrame(t, resp, 21, 22, 23, protocolErrorUnsupportedVersion)
}

func TestProtocolConformanceUDPUnknownFrameTypeReturnsErrorFrame(t *testing.T) {
	resp := exchangeUDPControlFrame(t, frame{
		typ:       frameType(99),
		streamID:  31,
		sessionID: 32,
		seq:       33,
	})
	assertProtocolErrorFrame(t, resp, 31, 32, 33, protocolErrorUnsupportedType)
}

func TestProtocolConformanceFutureHeaderVersionIsRejected(t *testing.T) {
	packet, err := encodeFrame(frame{
		typ:     frameHello,
		payload: encodeHelloPayload(helloPayload{minVersion: frameVersion, maxVersion: frameVersion}),
	})
	if err != nil {
		t.Fatal(err)
	}
	packet[4] = frameVersion + 1

	if version, ok := frameHeaderVersion(packet); !ok || version != frameVersion+1 {
		t.Fatalf("frameHeaderVersion() = %d, %v; want future version", version, ok)
	}
	if _, err := decodeFrame(packet); err == nil {
		t.Fatal("decodeFrame() err = nil, want unsupported version")
	}

	resp := exchangeRawUDPControlPacket(t, packet)
	assertProtocolErrorFrame(t, resp, 0, 0, 0, protocolErrorMalformedFrame)
}

func TestProtocolConformanceFutureErrorCodesRoundTrip(t *testing.T) {
	const futureCode protocolErrorCode = 99
	payload := encodeErrorPayload(errorPayload{
		code:    futureCode,
		message: "future error",
	})
	decoded := decodeProtocolError(payload)
	protocolErr, ok := decoded.(*ProtocolError)
	if !ok {
		t.Fatalf("decoded error = %T, want *ProtocolError", decoded)
	}
	if protocolErr.Code != uint16(futureCode) || protocolErr.Message != "future error" {
		t.Fatalf("decoded protocol error = %#v", protocolErr)
	}
}

func exchangeUDPControlFrame(t *testing.T, f frame) frame {
	t.Helper()
	packet, err := encodeFrame(f)
	if err != nil {
		t.Fatal(err)
	}
	return exchangeRawUDPControlPacket(t, packet)
}

func exchangeRawUDPControlPacket(t *testing.T, packet []byte) frame {
	t.Helper()
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: TransportUDP,
		StreamID:  777,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- sub.Serve(ctx, func(context.Context, Message) error {
			return nil
		})
	}()

	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.WriteTo(packet, sub.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, maxFrameSize)
	n, _, err := conn.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := decodeFrame(buf[:n])
	if err != nil {
		t.Fatal(err)
	}

	cancel()
	_ = sub.Close()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
			t.Fatalf("subscription Serve() err = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for subscription to stop")
	}
	return resp
}

func assertProtocolErrorFrame(t *testing.T, f frame, streamID, sessionID uint32, seq uint64, code protocolErrorCode) {
	t.Helper()
	if f.typ != frameError {
		t.Fatalf("response type = %d, want ERROR", f.typ)
	}
	if f.streamID != streamID || f.sessionID != sessionID || f.seq != seq {
		t.Fatalf("error frame metadata = stream=%d session=%d seq=%d", f.streamID, f.sessionID, f.seq)
	}
	payload, err := decodeErrorPayload(f.payload)
	if err != nil {
		t.Fatal(err)
	}
	if payload.code != code || payload.message == "" {
		t.Fatalf("error payload = %#v, want code %d with message", payload, code)
	}
}
