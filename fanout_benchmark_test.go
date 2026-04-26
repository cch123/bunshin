package bunshin

import (
	"context"
	"testing"
	"time"
)

func BenchmarkUDPFanout(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const subscribers = 4
	subs := make([]*Subscription, 0, subscribers)
	for i := 0; i < subscribers; i++ {
		sub, err := ListenSubscription(SubscriptionConfig{
			Transport: TransportUDP,
			StreamID:  171,
			LocalAddr: "127.0.0.1:0",
		})
		if err != nil {
			b.Fatal(err)
		}
		defer sub.Close()
		subs = append(subs, sub)
		go func(sub *Subscription) {
			_ = sub.Serve(ctx, func(context.Context, Message) error {
				return nil
			})
		}(sub)
	}

	pub, err := DialPublication(PublicationConfig{
		Transport:  TransportUDP,
		StreamID:   171,
		SessionID:  271,
		RemoteAddr: subs[0].LocalAddr().String(),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer pub.Close()
	for _, sub := range subs[1:] {
		if err := pub.AddDestination(sub.LocalAddr().String()); err != nil {
			b.Fatal(err)
		}
	}

	sendCtx, sendCancel := context.WithTimeout(ctx, 30*time.Second)
	defer sendCancel()
	payload := make([]byte, 256)
	b.SetBytes(int64(len(payload) * subscribers))
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
		b.ReportMetric(float64(b.N*subscribers)/elapsed.Seconds(), "delivered-msg/s")
	}
}

func BenchmarkUDPMultiDestinationChurn(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subA, err := ListenSubscription(SubscriptionConfig{
		Transport: TransportUDP,
		StreamID:  172,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		b.Fatal(err)
	}
	defer subA.Close()
	subB, err := ListenSubscription(SubscriptionConfig{
		Transport: TransportUDP,
		StreamID:  172,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		b.Fatal(err)
	}
	defer subB.Close()
	for _, sub := range []*Subscription{subA, subB} {
		go func(sub *Subscription) {
			_ = sub.Serve(ctx, func(context.Context, Message) error {
				return nil
			})
		}(sub)
	}

	pub, err := DialPublication(PublicationConfig{
		Transport:  TransportUDP,
		StreamID:   172,
		SessionID:  272,
		RemoteAddr: subA.LocalAddr().String(),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer pub.Close()

	sendCtx, sendCancel := context.WithTimeout(ctx, 30*time.Second)
	defer sendCancel()
	payload := make([]byte, 128)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pub.AddDestination(subB.LocalAddr().String()); err != nil {
			b.Fatal(err)
		}
		if err := pub.Send(sendCtx, payload); err != nil {
			b.Fatal(err)
		}
		if err := pub.RemoveDestination(subB.LocalAddr().String()); err != nil {
			b.Fatal(err)
		}
	}
}
