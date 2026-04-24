package bunshin

import (
	"context"
	"testing"

	"github.com/quic-go/quic-go"
)

func TestMetricsNilSnapshot(t *testing.T) {
	var metrics *Metrics
	if got := metrics.Snapshot(); got != (MetricsSnapshot{}) {
		t.Fatalf("nil metrics snapshot = %#v, want zero", got)
	}
}

func TestQUICConfigWithQLog(t *testing.T) {
	t.Setenv("QLOGDIR", t.TempDir())

	base := &quic.Config{
		MaxIncomingStreams: 7,
	}
	cfg := QUICConfigWithQLog(base)
	if cfg == base {
		t.Fatal("expected cloned QUIC config")
	}
	if cfg.MaxIncomingStreams != base.MaxIncomingStreams {
		t.Fatalf("MaxIncomingStreams = %d, want %d", cfg.MaxIncomingStreams, base.MaxIncomingStreams)
	}
	if base.Tracer != nil {
		t.Fatal("base config tracer was mutated")
	}
	if cfg.Tracer == nil {
		t.Fatal("qlog tracer was not configured")
	}

	trace := cfg.Tracer(context.Background(), true, quic.ConnectionIDFromBytes([]byte{1, 2, 3, 4}))
	if trace == nil {
		t.Fatal("qlog tracer returned nil with QLOGDIR set")
	}
	producer := trace.AddProducer()
	if producer == nil {
		t.Fatal("qlog trace did not create producer")
	}
	if err := producer.Close(); err != nil {
		t.Fatal(err)
	}
}
