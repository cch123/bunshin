package bunshin

import (
	"context"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

func TestMetricsNilSnapshot(t *testing.T) {
	var metrics *Metrics
	if got := metrics.Snapshot(); got != (MetricsSnapshot{}) {
		t.Fatalf("nil metrics snapshot = %#v, want zero", got)
	}
	if got := metrics.CounterSnapshots(); len(got) != len(metricsCounterDefinitions) {
		t.Fatalf("nil metrics counter snapshots = %d, want %d", len(got), len(metricsCounterDefinitions))
	}
}

func TestMetricsCounterSnapshotsExposeStableCounters(t *testing.T) {
	metrics := &Metrics{}
	metrics.incConnectionsOpened()
	metrics.incMessagesSent(42)
	metrics.incFramesSent(2)
	metrics.observeRTT(3 * time.Millisecond)

	counters := metrics.CounterSnapshots()
	if len(counters) != len(metricsCounterDefinitions) {
		t.Fatalf("counter snapshot count = %d, want %d", len(counters), len(metricsCounterDefinitions))
	}
	assertCounterSnapshot(t, counters, CounterMetricsConnectionsOpened, CounterScopeMetrics, "connections_opened", 1)
	assertCounterSnapshot(t, counters, CounterMetricsMessagesSent, CounterScopeMetrics, "messages_sent", 1)
	assertCounterSnapshot(t, counters, CounterMetricsBytesSent, CounterScopeMetrics, "bytes_sent", 42)
	assertCounterSnapshot(t, counters, CounterMetricsFramesSent, CounterScopeMetrics, "frames_sent", 2)
	assertCounterSnapshot(t, counters, CounterMetricsRTTLatestNanos, CounterScopeMetrics, "rtt_latest_nanos", uint64(3*time.Millisecond))
}

func TestDriverCounterSnapshotsExposeStableCounters(t *testing.T) {
	counters := DriverCounters{
		CommandsProcessed:   4,
		ClientsRegistered:   2,
		SubscriptionsClosed: 1,
		DutyCycles:          3,
		DutyCycleNanos:      700,
		Stalls:              1,
	}

	snapshots := counters.CounterSnapshots()
	if len(snapshots) != len(driverCounterDefinitions) {
		t.Fatalf("driver counter snapshot count = %d, want %d", len(snapshots), len(driverCounterDefinitions))
	}
	assertCounterSnapshot(t, snapshots, CounterDriverCommandsProcessed, CounterScopeDriver, "driver_commands_processed", 4)
	assertCounterSnapshot(t, snapshots, CounterDriverClientsRegistered, CounterScopeDriver, "driver_clients_registered", 2)
	assertCounterSnapshot(t, snapshots, CounterDriverSubscriptionsClosed, CounterScopeDriver, "driver_subscriptions_closed", 1)
	assertCounterSnapshot(t, snapshots, CounterDriverDutyCycles, CounterScopeDriver, "driver_duty_cycles", 3)
	assertCounterSnapshot(t, snapshots, CounterDriverDutyCycleNanos, CounterScopeDriver, "driver_duty_cycle_nanos", 700)
	assertCounterSnapshot(t, snapshots, CounterDriverStalls, CounterScopeDriver, "driver_stalls", 1)
}

func TestDriverStatusCounterSnapshotsExposeStableCounters(t *testing.T) {
	counters := DriverStatusCounters{
		ActiveClients:       2,
		ChannelEndpoints:    3,
		ActivePublications:  1,
		ActiveSubscriptions: 1,
		AvailableImages:     1,
		UnavailableImages:   1,
		LaggingImages:       1,
	}

	snapshots := counters.CounterSnapshots()
	if len(snapshots) != len(driverStatusCounterDefinitions) {
		t.Fatalf("driver status counter snapshot count = %d, want %d", len(snapshots), len(driverStatusCounterDefinitions))
	}
	assertCounterSnapshot(t, snapshots, CounterDriverActiveClients, CounterScopeDriver, "driver_active_clients", 2)
	assertCounterSnapshot(t, snapshots, CounterDriverChannelEndpoints, CounterScopeDriver, "driver_channel_endpoints", 3)
	assertCounterSnapshot(t, snapshots, CounterDriverAvailableImages, CounterScopeDriver, "driver_available_images", 1)
	assertCounterSnapshot(t, snapshots, CounterDriverLaggingImages, CounterScopeDriver, "driver_lagging_images", 1)
}

func TestBuildDriverCounterSnapshotsCombinesDriverStatusAndMetrics(t *testing.T) {
	metrics := &Metrics{}
	metrics.incMessagesReceived(7)
	status := DriverStatusCounters{ActiveClients: 2}

	counters := buildDriverCounterSnapshots(DriverCounters{CommandsFailed: 1}, status, metrics)
	if len(counters) != len(driverCounterDefinitions)+len(driverStatusCounterDefinitions)+len(metricsCounterDefinitions) {
		t.Fatalf("combined counter count = %d, want %d", len(counters), len(driverCounterDefinitions)+len(driverStatusCounterDefinitions)+len(metricsCounterDefinitions))
	}
	assertCounterSnapshot(t, counters, CounterDriverCommandsFailed, CounterScopeDriver, "driver_commands_failed", 1)
	assertCounterSnapshot(t, counters, CounterDriverActiveClients, CounterScopeDriver, "driver_active_clients", 2)
	assertCounterSnapshot(t, counters, CounterMetricsMessagesReceived, CounterScopeMetrics, "messages_received", 1)
	assertCounterSnapshot(t, counters, CounterMetricsBytesReceived, CounterScopeMetrics, "bytes_received", 7)
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

func assertCounterSnapshot(t *testing.T, counters []CounterSnapshot, typeID CounterTypeID, scope CounterScope, name string, value uint64) {
	t.Helper()
	for _, counter := range counters {
		if counter.TypeID == typeID {
			if counter.Scope != scope || counter.Name != name || counter.Value != value || counter.Label == "" {
				t.Fatalf("counter %d = %#v, want scope=%s name=%s value=%d", typeID, counter, scope, name, value)
			}
			return
		}
	}
	t.Fatalf("counter %d not found in %#v", typeID, counters)
}
