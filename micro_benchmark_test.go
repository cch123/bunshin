package bunshin

import (
	"context"
	"testing"
	"time"
)

func BenchmarkFrameEncode(b *testing.B) {
	payload := make([]byte, 256)
	f := frame{
		typ:        frameData,
		streamID:   1,
		sessionID:  2,
		termID:     3,
		termOffset: 4,
		seq:        5,
		payload:    payload,
	}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.seq = uint64(i + 1)
		if _, err := encodeFrame(f); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFrameDecode(b *testing.B) {
	payload := make([]byte, 256)
	packet, err := encodeFrame(frame{
		typ:        frameData,
		streamID:   1,
		sessionID:  2,
		termID:     3,
		termOffset: 4,
		seq:        5,
		payload:    payload,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := decodeFrame(packet); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublicationSubscriptionSendUDP(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: TransportUDP,
		StreamID:  102,
		LocalAddr: "127.0.0.1:0",
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

	pub, err := DialPublication(PublicationConfig{
		Transport:  TransportUDP,
		StreamID:   102,
		SessionID:  202,
		RemoteAddr: sub.LocalAddr().String(),
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
	start := time.Now()
	for i := 0; i < b.N; i++ {
		if err := pub.Send(sendCtx, payload); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msg/s")
	}
}

func BenchmarkSubscriptionHandlerDispatch(b *testing.B) {
	metrics := &Metrics{}
	sub := &Subscription{
		transportMode:            TransportUDP,
		metrics:                  metrics,
		loss:                     newLossDetector(metrics, nil),
		streamID:                 103,
		imageTermLength:          minTermLength,
		imagePositionBitsToShift: termPositionBitsToShift(minTermLength),
		closed:                   make(chan struct{}),
	}
	payload := make([]byte, 256)
	item := orderedMessage{
		msg: Message{
			StreamID:  103,
			SessionID: 203,
			Remote:    driverNetAddr("benchmark"),
			Payload:   payload,
		},
		position: 1,
	}
	handler := func(context.Context, Message) error {
		return nil
	}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item.msg.Sequence = uint64(i + 1)
		item.position = int64(i + 1)
		if err := sub.handleMessage(context.Background(), item, handler); err != nil {
			b.Fatal(err)
		}
	}
}
