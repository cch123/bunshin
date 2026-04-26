package bunshin

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func BenchmarkPublicationSubscriptionMemoryProfile(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: TransportUDP,
		StreamID:  173,
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
		StreamID:   173,
		SessionID:  273,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer pub.Close()

	payload := make([]byte, 256)
	sendCtx, sendCancel := context.WithTimeout(ctx, 30*time.Second)
	defer sendCancel()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pub.Send(sendCtx, payload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	runtime.ReadMemStats(&after)
	reportMemoryProfileMetrics(b, before, after)
}

func BenchmarkArchiveMemoryProfile(b *testing.B) {
	archive, err := OpenArchive(ArchiveConfig{Path: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer archive.Close()

	payload := make([]byte, 256)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := archive.Record(Message{
			StreamID:  174,
			SessionID: 274,
			Sequence:  uint64(i + 1),
			Payload:   payload,
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	runtime.ReadMemStats(&after)
	reportMemoryProfileMetrics(b, before, after)
}

func BenchmarkMediaDriverMemoryProfile(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	driver, err := StartMediaDriver(DriverConfig{})
	if err != nil {
		b.Fatal(err)
	}
	defer driver.Close()

	client, err := driver.NewClient(context.Background(), "benchmark")
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close(context.Background())
	sub, err := client.AddSubscription(context.Background(), SubscriptionConfig{
		StreamID:  175,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		b.Fatal(err)
	}
	defer sub.Close(context.Background())
	go func() {
		_ = sub.Serve(ctx, func(context.Context, Message) error {
			return nil
		})
	}()
	pub, err := client.AddPublication(context.Background(), PublicationConfig{
		StreamID:   175,
		SessionID:  275,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer pub.Close(context.Background())

	payload := make([]byte, 256)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pub.Send(ctx, payload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	runtime.ReadMemStats(&after)
	reportMemoryProfileMetrics(b, before, after)
}

func reportMemoryProfileMetrics(b *testing.B, before, after runtime.MemStats) {
	b.Helper()
	if after.NumGC >= before.NumGC {
		b.ReportMetric(float64(after.NumGC-before.NumGC), "gc-cycles")
	}
	if after.HeapAlloc >= before.HeapAlloc {
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc), "heap-delta-bytes")
	}
	if after.TotalAlloc >= before.TotalAlloc {
		b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc), "total-alloc-bytes")
	}
}
