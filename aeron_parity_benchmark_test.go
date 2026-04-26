package bunshin

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type aeronParityBenchmarkWorkload struct {
	name         string
	payloadBytes int
}

var aeronParityBenchmarkWorkloads = []aeronParityBenchmarkWorkload{
	{name: "payload_256B", payloadBytes: 256},
	{name: "payload_1024B", payloadBytes: 1024},
}

func BenchmarkAeronParityTransportBaseline(b *testing.B) {
	for _, workload := range aeronParityBenchmarkWorkloads {
		workload := workload
		b.Run(workload.name, func(b *testing.B) {
			b.Run("quic", func(b *testing.B) {
				benchmarkAeronParityPublicationSubscription(b, TransportQUIC, workload.payloadBytes)
			})
			b.Run("udp", func(b *testing.B) {
				benchmarkAeronParityPublicationSubscription(b, TransportUDP, workload.payloadBytes)
			})
			b.Run("ipc_ring", func(b *testing.B) {
				benchmarkAeronParityIPCRing(b, workload.payloadBytes)
			})
		})
	}
}

func benchmarkAeronParityPublicationSubscription(b *testing.B, transport TransportMode, payloadBytes int) {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamID := uint32(8100 + payloadBytes)
	sessionID := uint32(9100 + payloadBytes)
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: transport,
		StreamID:  streamID,
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
		Transport:  transport,
		StreamID:   streamID,
		SessionID:  sessionID,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer pub.Close()

	payload := make([]byte, payloadBytes)
	sendCtx, sendCancel := context.WithTimeout(ctx, 30*time.Second)
	defer sendCancel()

	b.SetBytes(int64(payloadBytes))
	b.ReportAllocs()
	b.ResetTimer()
	started := time.Now()
	for i := 0; i < b.N; i++ {
		if err := pub.Send(sendCtx, payload); err != nil {
			b.Fatal(err)
		}
	}
	reportAeronParityThroughput(b, b.N, started)
}

func benchmarkAeronParityIPCRing(b *testing.B, payloadBytes int) {
	b.Helper()
	ring, err := OpenIPCRing(IPCRingConfig{
		Path:     fmt.Sprintf("%s/ipc-%d.ring", b.TempDir(), payloadBytes),
		Capacity: 1 << 20,
		Reset:    true,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer ring.Close()

	payload := make([]byte, payloadBytes)
	b.SetBytes(int64(payloadBytes))
	b.ReportAllocs()
	b.ResetTimer()
	started := time.Now()
	for i := 0; i < b.N; i++ {
		if err := ring.Offer(payload); err != nil {
			b.Fatal(err)
		}
		if _, err := ring.Poll(func(got []byte) error {
			if len(got) != len(payload) {
				b.Fatalf("payload length = %d, want %d", len(got), len(payload))
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
	reportAeronParityThroughput(b, b.N, started)
}

func reportAeronParityThroughput(b *testing.B, messages int, started time.Time) {
	b.Helper()
	elapsed := time.Since(started)
	b.StopTimer()
	if elapsed > 0 {
		b.ReportMetric(float64(messages)/elapsed.Seconds(), "msg/s")
	}
}
