package bunshin

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestLossDetectorReportsSequenceGap(t *testing.T) {
	metrics := &Metrics{}
	var observations []LossObservation
	detector := newLossDetector(metrics, func(observation LossObservation) {
		observations = append(observations, observation)
	})
	remote := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40456}

	detector.observe(frame{streamID: 10, sessionID: 20, seq: 1}, remote)
	detector.observe(frame{streamID: 10, sessionID: 20, seq: 3}, remote)

	if len(observations) != 1 {
		t.Fatalf("observations = %d, want 1", len(observations))
	}
	observation := observations[0]
	if observation.StreamID != 10 || observation.SessionID != 20 || observation.Source != remote.String() ||
		observation.FromSequence != 2 || observation.ToSequence != 2 || observation.MissingMessages != 1 {
		t.Fatalf("unexpected observation: %#v", observation)
	}
	if observation.ObservedAt.IsZero() {
		t.Fatal("observation time was not set")
	}

	reports := detector.snapshot()
	if len(reports) != 1 {
		t.Fatalf("reports = %d, want 1", len(reports))
	}
	report := reports[0]
	if report.StreamID != 10 || report.SessionID != 20 || report.Source != remote.String() ||
		report.ObservationCount != 1 || report.MissingMessages != 1 ||
		report.FirstObservation.IsZero() || report.LastObservation.IsZero() {
		t.Fatalf("unexpected report: %#v", report)
	}

	detector.observe(frame{streamID: 10, sessionID: 20, seq: 2}, remote)
	if len(observations) != 1 {
		t.Fatalf("observations after late fill = %d, want 1", len(observations))
	}
	snapshot := metrics.Snapshot()
	if snapshot.LossGapEvents != 1 || snapshot.LossGapMessages != 1 {
		t.Fatalf("unexpected metrics: %#v", snapshot)
	}
}

func TestLossDetectorReportsLargeGapAsSingleObservation(t *testing.T) {
	metrics := &Metrics{}
	detector := newLossDetector(metrics, nil)
	remote := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40456}

	detector.observe(frame{streamID: 10, sessionID: 20, seq: 1}, remote)
	detector.observe(frame{streamID: 10, sessionID: 20, seq: 1_000_001}, remote)

	reports := detector.snapshot()
	if len(reports) != 1 {
		t.Fatalf("reports = %d, want 1", len(reports))
	}
	report := reports[0]
	if report.ObservationCount != 1 || report.MissingMessages != 999_999 {
		t.Fatalf("unexpected report: %#v", report)
	}
}

func TestSubscriptionReportsSequenceGap(t *testing.T) {
	subMetrics := &Metrics{}
	var observations []LossObservation
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  101,
		LocalAddr: "127.0.0.1:0",
		Metrics:   subMetrics,
		LossHandler: func(observation LossObservation) {
			observations = append(observations, observation)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	received := make(chan Message, 2)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:   101,
		SessionID:  202,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	for _, seq := range []uint64{1, 3} {
		packet, err := encodeFrame(frame{
			typ:       frameData,
			streamID:  101,
			sessionID: 202,
			seq:       seq,
			payload:   []byte("payload"),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pub.roundTrip(ctx, packet); err != nil {
			t.Fatal(err)
		}
	}

	for range 2 {
		select {
		case <-received:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	reports := sub.LossReports()
	if len(reports) != 1 {
		t.Fatalf("loss reports = %d, want 1", len(reports))
	}
	report := reports[0]
	if report.StreamID != 101 || report.SessionID != 202 || report.ObservationCount != 1 || report.MissingMessages != 1 {
		t.Fatalf("unexpected loss report: %#v", report)
	}
	if len(observations) != 1 || observations[0].FromSequence != 2 || observations[0].ToSequence != 2 {
		t.Fatalf("unexpected observations: %#v", observations)
	}
	snapshot := subMetrics.Snapshot()
	if snapshot.LossGapEvents != 1 || snapshot.LossGapMessages != 1 {
		t.Fatalf("unexpected subscription metrics: %#v", snapshot)
	}
}
