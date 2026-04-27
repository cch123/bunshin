package core

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"
)

type transportLatencyBenchmarkOutput struct {
	Name         string `json:"name"`
	Transport    string `json:"transport"`
	PayloadBytes int    `json:"payload_bytes"`
	Samples      int    `json:"samples"`
	P50Nanos     int64  `json:"p50_nanos"`
	P95Nanos     int64  `json:"p95_nanos"`
	P99Nanos     int64  `json:"p99_nanos"`
	P999Nanos    int64  `json:"p999_nanos"`
	MaxNanos     int64  `json:"max_nanos"`
}

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

func BenchmarkTransportLatencyQUIC(b *testing.B) {
	benchmarkTransportLatency(b, TransportQUIC)
}

func BenchmarkTransportLatencyUDP(b *testing.B) {
	benchmarkTransportLatency(b, TransportUDP)
}

func BenchmarkIPCRingLatency(b *testing.B) {
	ring, err := OpenIPCRing(IPCRingConfig{
		Path:     b.TempDir() + "/latency.ring",
		Capacity: 1 << 20,
		Reset:    true,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer ring.Close()

	payload := make([]byte, 256)
	latencies := make([]time.Duration, 0, b.N)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
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
		latencies = append(latencies, time.Since(start))
	}
	b.StopTimer()
	reportLatencyBenchmarkMetrics(b, latencies)
}

func BenchmarkIPCRingThroughput(b *testing.B) {
	ring, err := OpenIPCRing(IPCRingConfig{
		Path:     b.TempDir() + "/throughput.ring",
		Capacity: 1 << 20,
		Reset:    true,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer ring.Close()

	payload := make([]byte, 256)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
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
	elapsed := time.Since(start)
	b.StopTimer()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msg/s")
	}
}

func BenchmarkMappedTermBufferAppend(b *testing.B) {
	terms, err := newMappedTermLog(minTermLength, 0, b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer terms.close()

	payload := make([]byte, 256)
	b.SetBytes(int64(headerLen + len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		seq := uint64(i + 1)
		if _, err := terms.append(headerLen+len(payload), func(appendResult termAppend) error {
			encoded, err := encodeFrame(frame{
				typ:        frameData,
				streamID:   180,
				sessionID:  280,
				termID:     appendResult.TermID,
				termOffset: appendResult.TermOffset,
				seq:        seq,
				payload:    payload,
			})
			if err != nil {
				return err
			}
			copy(appendResult.Bytes(), encoded)
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "appends/s")
	}
}

func benchmarkTransportLatency(b *testing.B, transport TransportMode) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: transport,
		StreamID:  105,
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
		StreamID:   105,
		SessionID:  205,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer pub.Close()

	payload := make([]byte, 256)
	latencies := make([]time.Duration, 0, b.N)
	sendCtx, sendCancel := context.WithTimeout(ctx, 30*time.Second)
	defer sendCancel()

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	started := time.Now()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if err := pub.Send(sendCtx, payload); err != nil {
			b.Fatal(err)
		}
		latencies = append(latencies, time.Since(start))
	}
	elapsed := time.Since(started)
	b.StopTimer()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msg/s")
	}
	reportLatencyBenchmarkMetrics(b, latencies)
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
	output := transportLatencyBenchmarkOutput{
		Name:         "transport_latency",
		Transport:    string(TransportQUIC),
		PayloadBytes: len(payload),
		Samples:      len(latencies),
		P50Nanos:     percentile(latencies, 50).Nanoseconds(),
		P95Nanos:     percentile(latencies, 95).Nanoseconds(),
		P99Nanos:     percentile(latencies, 99).Nanoseconds(),
		P999Nanos:    percentilePermille(latencies, 999).Nanoseconds(),
		MaxNanos:     latencies[len(latencies)-1].Nanoseconds(),
	}
	data, err := json.Marshal(output)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("transport benchmark output: %s", data)
}

func reportLatencyBenchmarkMetrics(b *testing.B, latencies []time.Duration) {
	b.Helper()
	if len(latencies) == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	b.ReportMetric(float64(percentile(latencies, 50).Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(percentile(latencies, 95).Nanoseconds()), "p95-ns")
	b.ReportMetric(float64(percentile(latencies, 99).Nanoseconds()), "p99-ns")
	b.ReportMetric(float64(percentilePermille(latencies, 999).Nanoseconds()), "p999-ns")
	b.ReportMetric(float64(latencies[len(latencies)-1].Nanoseconds()), "max-ns")
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

func percentilePermille(values []time.Duration, p int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	idx := (len(values)*p + 999) / 1000
	if idx <= 0 {
		idx = 1
	}
	if idx > len(values) {
		idx = len(values)
	}
	return values[idx-1]
}
