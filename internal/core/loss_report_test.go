package core

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
	observations := make(chan LossObservation, 1)
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  101,
		LocalAddr: "127.0.0.1:0",
		Metrics:   subMetrics,
		LossHandler: func(observation LossObservation) {
			observations <- observation
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	received := make(chan Message, 3)
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

	if err := sendDataFrame(ctx, pub, 101, 202, 1, []byte("payload")); err != nil {
		t.Fatal(err)
	}

	thirdErr := make(chan error, 1)
	go func() {
		thirdErr <- sendDataFrame(ctx, pub, 101, 202, 3, []byte("payload"))
	}()

	var observation LossObservation
	select {
	case observation = <-observations:
		if observation.FromSequence != 2 || observation.ToSequence != 2 {
			t.Fatalf("unexpected observation: %#v", observation)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	if err := sendDataFrame(ctx, pub, 101, 202, 2, []byte("payload")); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-thirdErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	for _, want := range []uint64{1, 2, 3} {
		select {
		case msg := <-received:
			if msg.Sequence != want {
				t.Fatalf("received sequence = %d, want %d", msg.Sequence, want)
			}
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
	snapshot := subMetrics.Snapshot()
	if snapshot.LossGapEvents != 1 || snapshot.LossGapMessages != 1 {
		t.Fatalf("unexpected subscription metrics: %#v", snapshot)
	}
}
