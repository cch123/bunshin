package bunshin

import (
	"context"
	"sort"
	"testing"
	"time"
)

func BenchmarkPublicationSubscriptionSend(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  100,
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
		StreamID:   100,
		SessionID:  200,
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
	for i := 0; i < b.N; i++ {
		if err := pub.Send(sendCtx, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func TestTransportLatencyPercentiles(t *testing.T) {
	if testing.Short() {
		t.Skip("latency percentile test is skipped in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  101,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	go func() {
		_ = sub.Serve(ctx, func(context.Context, Message) error {
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:   101,
		SessionID:  201,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	const samples = 256
	payload := make([]byte, 256)
	latencies := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		start := time.Now()
		if err := pub.Send(ctx, payload); err != nil {
			t.Fatal(err)
		}
		latencies = append(latencies, time.Since(start))
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	t.Logf("quic transport latency: p50=%s p95=%s p99=%s max=%s samples=%d",
		percentile(latencies, 50),
		percentile(latencies, 95),
		percentile(latencies, 99),
		latencies[len(latencies)-1],
		len(latencies),
	)
}

func percentile(values []time.Duration, p int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	idx := (len(values)*p + 99) / 100
	if idx <= 0 {
		idx = 1
	}
	if idx > len(values) {
		idx = len(values)
	}
	return values[idx-1]
}
