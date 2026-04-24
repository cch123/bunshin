package bunshin

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkPublicationSubscriptionPacketLoss(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverConn := newLossyPacketConn(b, 16, 32)
	defer serverConn.Close()

	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:   110,
		PacketConn: serverConn,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer sub.Close()

	go func() {
		_ = sub.Serve(ctx, func(context.Context, Message) error {
			return nil
		})
	}()

	clientConn := newLossyPacketConn(b, 16, 32)
	defer clientConn.Close()

	pub, err := DialPublication(PublicationConfig{
		StreamID:   110,
		SessionID:  210,
		RemoteAddr: sub.LocalAddr().String(),
		PacketConn: clientConn,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer pub.Close()

	payload := make([]byte, 256)
	sendCtx, sendCancel := context.WithTimeout(ctx, 30*time.Second)
	defer sendCancel()

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pub.Send(sendCtx, payload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(clientConn.Dropped()+serverConn.Dropped()), "dropped_packets")
}

func TestTransportRecoversFromPacketLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	serverConn := newLossyPacketConn(t, 4, 16)
	defer serverConn.Close()

	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:   111,
		PacketConn: serverConn,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	received := make(chan Message, 64)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		})
	}()

	clientConn := newLossyPacketConn(t, 4, 16)
	defer clientConn.Close()

	pub, err := DialPublication(PublicationConfig{
		StreamID:   111,
		SessionID:  211,
		RemoteAddr: sub.LocalAddr().String(),
		PacketConn: clientConn,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	const messages = 32
	for i := 0; i < messages; i++ {
		if err := pub.Send(ctx, []byte("payload")); err != nil {
			t.Fatal(err)
		}
	}

	if got := len(received); got != messages {
		t.Fatalf("received %d messages, want %d", got, messages)
	}
	if dropped := clientConn.Dropped() + serverConn.Dropped(); dropped == 0 {
		t.Fatal("loss harness did not drop any packets")
	}
}

type lossyPacketConn struct {
	net.PacketConn
	skipFirst uint64
	dropEvery uint64
	writes    atomic.Uint64
	dropped   atomic.Uint64
}

func newLossyPacketConn(tb testing.TB, skipFirst, dropEvery uint64) *lossyPacketConn {
	tb.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	return &lossyPacketConn{
		PacketConn: conn,
		skipFirst:  skipFirst,
		dropEvery:  dropEvery,
	}
}

func (c *lossyPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	count := c.writes.Add(1)
	if c.dropEvery > 0 && count > c.skipFirst && (count-c.skipFirst)%c.dropEvery == 0 {
		c.dropped.Add(1)
		return len(p), nil
	}
	return c.PacketConn.WriteTo(p, addr)
}

func (c *lossyPacketConn) Dropped() uint64 {
	return c.dropped.Load()
}

func (c *lossyPacketConn) SetReadBuffer(bytes int) error {
	conn, ok := c.PacketConn.(interface{ SetReadBuffer(int) error })
	if !ok {
		return nil
	}
	return conn.SetReadBuffer(bytes)
}

func (c *lossyPacketConn) SetWriteBuffer(bytes int) error {
	conn, ok := c.PacketConn.(interface{ SetWriteBuffer(int) error })
	if !ok {
		return nil
	}
	return conn.SetWriteBuffer(bytes)
}
