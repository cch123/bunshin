package bunshin

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPublicationSubscription(t *testing.T) {
	pubMetrics := &Metrics{}
	subMetrics := &Metrics{}
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  99,
		LocalAddr: "127.0.0.1:0",
		Metrics:   subMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	received := make(chan Message, 1)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:        99,
		SessionID:       1234,
		InitialTermID:   77,
		RemoteAddr:      sub.LocalAddr().String(),
		RetransmitEvery: time.Millisecond,
		Metrics:         pubMetrics,
		ReservedValue: func(payload []byte) uint64 {
			return uint64(len(payload))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Send(ctx, []byte("payload")); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-received:
		if msg.StreamID != 99 || msg.SessionID != 1234 || msg.TermID != 77 || msg.TermOffset != 0 ||
			msg.Sequence != 1 || string(msg.Payload) != "payload" {
			t.Fatalf("unexpected message: %#v", msg)
		}
		if msg.ReservedValue != uint64(len("payload")) {
			t.Fatalf("reserved value = %d, want %d", msg.ReservedValue, len("payload"))
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	pubSnapshot := pubMetrics.Snapshot()
	if pubSnapshot.ConnectionsOpened != 1 || pubSnapshot.MessagesSent != 1 || pubSnapshot.BytesSent != uint64(len("payload")) ||
		pubSnapshot.AcksReceived != 1 || pubSnapshot.FramesSent != 2 || pubSnapshot.FramesReceived != 2 ||
		pubSnapshot.FramesDropped != 0 || pubSnapshot.Retransmits != 0 {
		t.Fatalf("unexpected publication metrics: %#v", pubSnapshot)
	}
	subSnapshot := subMetrics.Snapshot()
	if subSnapshot.ConnectionsAccepted != 1 || subSnapshot.MessagesReceived != 1 || subSnapshot.BytesReceived != uint64(len("payload")) ||
		subSnapshot.AcksSent != 1 || subSnapshot.FramesSent != 2 || subSnapshot.FramesReceived != 2 ||
		subSnapshot.FramesDropped != 0 || subSnapshot.Retransmits != 0 {
		t.Fatalf("unexpected subscription metrics: %#v", subSnapshot)
	}
}

func TestPublicationSubscriptionFragmentsLargePayload(t *testing.T) {
	pubMetrics := &Metrics{}
	subMetrics := &Metrics{}
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  102,
		LocalAddr: "127.0.0.1:0",
		Metrics:   subMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	received := make(chan Message, 1)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:               102,
		SessionID:              204,
		RemoteAddr:             sub.LocalAddr().String(),
		MaxPayloadBytes:        16,
		MTUBytes:               headerLen + 5,
		PublicationWindowBytes: 256,
		Metrics:                pubMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	payload := []byte("abcdefghijklmnop")
	if err := pub.Send(ctx, payload); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-received:
		if msg.StreamID != 102 || msg.SessionID != 204 || msg.Sequence != 1 || string(msg.Payload) != string(payload) {
			t.Fatalf("unexpected fragmented message: %#v", msg)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	pubSnapshot := pubMetrics.Snapshot()
	if pubSnapshot.MessagesSent != 1 || pubSnapshot.BytesSent != uint64(len(payload)) || pubSnapshot.AcksReceived != 1 {
		t.Fatalf("unexpected publication metrics: %#v", pubSnapshot)
	}
	subSnapshot := subMetrics.Snapshot()
	if subSnapshot.MessagesReceived != 1 || subSnapshot.BytesReceived != uint64(len(payload)) || subSnapshot.AcksSent != 1 {
		t.Fatalf("unexpected subscription metrics: %#v", subSnapshot)
	}
}

func TestUDPPublicationSubscription(t *testing.T) {
	pubMetrics := &Metrics{}
	subMetrics := &Metrics{}
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: TransportUDP,
		StreamID:  112,
		LocalAddr: "127.0.0.1:0",
		Metrics:   subMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	received := make(chan Message, 1)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		Transport:     TransportUDP,
		StreamID:      112,
		SessionID:     214,
		InitialTermID: 9,
		RemoteAddr:    sub.LocalAddr().String(),
		Metrics:       pubMetrics,
		ReservedValue: func(payload []byte) uint64 {
			return uint64(len(payload) * 2)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Send(ctx, []byte("udp payload")); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-received:
		if msg.StreamID != 112 || msg.SessionID != 214 || msg.TermID != 9 || msg.TermOffset != 0 ||
			msg.Sequence != 1 || string(msg.Payload) != "udp payload" {
			t.Fatalf("unexpected udp message: %#v", msg)
		}
		if msg.ReservedValue != uint64(len("udp payload")*2) {
			t.Fatalf("reserved value = %d, want %d", msg.ReservedValue, len("udp payload")*2)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	pubSnapshot := pubMetrics.Snapshot()
	if pubSnapshot.ConnectionsOpened != 1 || pubSnapshot.MessagesSent != 1 || pubSnapshot.BytesSent != uint64(len("udp payload")) ||
		pubSnapshot.AcksReceived != 1 || pubSnapshot.FramesSent != 1 || pubSnapshot.FramesReceived != 2 ||
		pubSnapshot.FramesDropped != 0 || pubSnapshot.SendErrors != 0 {
		t.Fatalf("unexpected udp publication metrics: %#v", pubSnapshot)
	}
	subSnapshot := subMetrics.Snapshot()
	if subSnapshot.ConnectionsAccepted != 1 || subSnapshot.MessagesReceived != 1 || subSnapshot.BytesReceived != uint64(len("udp payload")) ||
		subSnapshot.AcksSent != 1 || subSnapshot.FramesSent != 2 || subSnapshot.FramesReceived != 1 ||
		subSnapshot.FramesDropped != 0 || subSnapshot.ReceiveErrors != 0 {
		t.Fatalf("unexpected udp subscription metrics: %#v", subSnapshot)
	}
}

func TestUDPPublicationSubscriptionFragmentsLargePayload(t *testing.T) {
	pubMetrics := &Metrics{}
	subMetrics := &Metrics{}
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: TransportUDP,
		StreamID:  113,
		LocalAddr: "127.0.0.1:0",
		Metrics:   subMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	received := make(chan Message, 1)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		Transport:              TransportUDP,
		StreamID:               113,
		SessionID:              215,
		RemoteAddr:             sub.LocalAddr().String(),
		MaxPayloadBytes:        16,
		MTUBytes:               headerLen + 5,
		PublicationWindowBytes: 256,
		Metrics:                pubMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	payload := []byte("abcdefghijklmnop")
	if err := pub.Send(ctx, payload); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-received:
		if msg.StreamID != 113 || msg.SessionID != 215 || msg.TermOffset != 0 || msg.Sequence != 1 || string(msg.Payload) != string(payload) {
			t.Fatalf("unexpected fragmented udp message: %#v", msg)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	pubSnapshot := pubMetrics.Snapshot()
	if pubSnapshot.MessagesSent != 1 || pubSnapshot.BytesSent != uint64(len(payload)) ||
		pubSnapshot.AcksReceived != 1 || pubSnapshot.FramesSent != 4 || pubSnapshot.FramesReceived != 2 {
		t.Fatalf("unexpected udp publication metrics: %#v", pubSnapshot)
	}
	subSnapshot := subMetrics.Snapshot()
	if subSnapshot.ConnectionsAccepted != 1 || subSnapshot.MessagesReceived != 1 || subSnapshot.BytesReceived != uint64(len(payload)) ||
		subSnapshot.AcksSent != 1 || subSnapshot.FramesSent != 2 || subSnapshot.FramesReceived != 4 {
		t.Fatalf("unexpected udp subscription metrics: %#v", subSnapshot)
	}
}

func TestUDPPublicationFlowControlReceivesStatusFrame(t *testing.T) {
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport:           TransportUDP,
		StreamID:            114,
		LocalAddr:           "127.0.0.1:0",
		ReceiverWindowBytes: 96,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		_ = sub.Serve(ctx, func(context.Context, Message) error {
			return nil
		})
	}()

	flow := &recordingFlowControl{status: make(chan FlowControlStatus, 1)}
	pub, err := DialPublication(PublicationConfig{
		Transport:              TransportUDP,
		StreamID:               114,
		SessionID:              216,
		RemoteAddr:             sub.LocalAddr().String(),
		PublicationWindowBytes: 64,
		FlowControl:            flow,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Send(ctx, []byte("payload")); err != nil {
		t.Fatal(err)
	}

	select {
	case status := <-flow.status:
		if status.Position != 64 || status.WindowLength != 96 || status.ReceiverID == "" {
			t.Fatalf("unexpected udp flow control status: %#v", status)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestUDPPublicationReportsTransportFeedback(t *testing.T) {
	pubMetrics := &Metrics{}
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: TransportUDP,
		StreamID:  116,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		_ = sub.Serve(ctx, func(context.Context, Message) error {
			return nil
		})
	}()

	feedback := make(chan TransportFeedback, 1)
	pub, err := DialPublication(PublicationConfig{
		Transport:  TransportUDP,
		StreamID:   116,
		SessionID:  218,
		RemoteAddr: sub.LocalAddr().String(),
		Metrics:    pubMetrics,
		TransportFeedback: func(observation TransportFeedback) {
			feedback <- observation
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Send(ctx, []byte("payload")); err != nil {
		t.Fatal(err)
	}

	select {
	case observation := <-feedback:
		if observation.Transport != TransportUDP || observation.StreamID != 116 ||
			observation.SessionID != 218 || observation.Sequence != 1 ||
			observation.RTT <= 0 || observation.Remote == "" {
			t.Fatalf("unexpected transport feedback: %#v", observation)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	snapshot := pubMetrics.Snapshot()
	if snapshot.RTTMeasurements != 1 || snapshot.RTTLatest <= 0 || snapshot.RTTMin <= 0 || snapshot.RTTMax <= 0 {
		t.Fatalf("unexpected rtt metrics: %#v", snapshot)
	}
}

func TestUDPNakRepairRetransmitsCachedFrames(t *testing.T) {
	pubMetrics := &Metrics{}
	subMetrics := &Metrics{}
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: TransportUDP,
		StreamID:  115,
		LocalAddr: "127.0.0.1:0",
		Metrics:   subMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	delivered := make(chan uint64, 2)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			delivered <- msg.Sequence
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		Transport:  TransportUDP,
		StreamID:   115,
		SessionID:  217,
		RemoteAddr: sub.LocalAddr().String(),
		Metrics:    pubMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	_, firstDatagrams := udpDatagramsForTest(t, pub, 1, []byte("one"))
	pub.cacheUDPRetransmit(1, firstDatagrams)
	secondAppend, secondDatagrams := udpDatagramsForTest(t, pub, 2, []byte("two"))
	pub.cacheUDPRetransmit(2, secondDatagrams)
	if err := pub.writeUDPDatagrams(pub.udpRemote, secondDatagrams); err != nil {
		t.Fatal(err)
	}

	resp, statusApplied, err := pub.readUDPResponse(ctx, secondAppend, 2)
	if err != nil {
		t.Fatal(err)
	}
	if resp.typ != frameAck || !statusApplied {
		t.Fatalf("udp response = %#v statusApplied=%v, want ack with status", resp, statusApplied)
	}

	for _, want := range []uint64{1, 2} {
		select {
		case got := <-delivered:
			if got != want {
				t.Fatalf("delivered sequence = %d, want %d", got, want)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	pubSnapshot := pubMetrics.Snapshot()
	if pubSnapshot.Retransmits != 1 {
		t.Fatalf("publication retransmits = %d, want 1: %#v", pubSnapshot.Retransmits, pubSnapshot)
	}
	subSnapshot := subMetrics.Snapshot()
	if subSnapshot.LossGapEvents != 1 || subSnapshot.LossGapMessages != 1 {
		t.Fatalf("unexpected subscription loss metrics: %#v", subSnapshot)
	}
}

func TestPublicationFlowControlReceivesAckStatus(t *testing.T) {
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  103,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		_ = sub.Serve(ctx, func(context.Context, Message) error {
			return nil
		})
	}()

	flow := &recordingFlowControl{status: make(chan FlowControlStatus, 1)}
	pub, err := DialPublication(PublicationConfig{
		StreamID:               103,
		RemoteAddr:             sub.LocalAddr().String(),
		PublicationWindowBytes: 64,
		FlowControl:            flow,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Send(ctx, []byte("payload")); err != nil {
		t.Fatal(err)
	}

	select {
	case status := <-flow.status:
		if status.Position != 64 || status.WindowLength != 64 || status.ReceiverID == "" {
			t.Fatalf("unexpected flow control status: %#v", status)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestSubscriptionDeliversMessagesInSequenceOrder(t *testing.T) {
	lossObserved := make(chan LossObservation, 1)
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  104,
		LocalAddr: "127.0.0.1:0",
		LossHandler: func(observation LossObservation) {
			lossObserved <- observation
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	delivered := make(chan uint64, 2)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			delivered <- msg.Sequence
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:   104,
		SessionID:  205,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	secondErr := make(chan error, 1)
	go func() {
		secondErr <- sendDataFrame(ctx, pub, 104, 205, 2, []byte("two"))
	}()

	select {
	case observation := <-lossObserved:
		if observation.FromSequence != 1 || observation.ToSequence != 1 {
			t.Fatalf("unexpected loss observation: %#v", observation)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	select {
	case seq := <-delivered:
		t.Fatalf("delivered sequence %d before sequence 1 arrived", seq)
	case <-time.After(25 * time.Millisecond):
	}

	if err := sendDataFrame(ctx, pub, 104, 205, 1, []byte("one")); err != nil {
		t.Fatal(err)
	}

	for _, want := range []uint64{1, 2} {
		select {
		case got := <-delivered:
			if got != want {
				t.Fatalf("delivered sequence = %d, want %d", got, want)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	select {
	case err := <-secondErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestPublicationBackPressureWaitsForAckCapacity(t *testing.T) {
	pubMetrics := &Metrics{}
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  100,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	received := make(chan Message, 1)
	releaseHandler := make(chan struct{})
	go func() {
		_ = sub.Serve(ctx, func(ctx context.Context, msg Message) error {
			received <- msg
			select {
			case <-releaseHandler:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:               100,
		RemoteAddr:             sub.LocalAddr().String(),
		PublicationWindowBytes: 64,
		Metrics:                pubMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	firstErr := make(chan error, 1)
	go func() {
		firstErr <- pub.Send(ctx, []byte("payload"))
	}()

	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer waitCancel()
	if err := pub.Send(waitCtx, []byte("payload")); !errors.Is(err, ErrBackPressure) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second send err = %v, want back pressure deadline", err)
	}

	close(releaseHandler)
	select {
	case err := <-firstErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	snapshot := pubMetrics.Snapshot()
	if snapshot.BackPressureEvents != 1 || snapshot.SendErrors != 1 || snapshot.MessagesSent != 1 {
		t.Fatalf("unexpected publication metrics: %#v", snapshot)
	}
}

type recordingFlowControl struct {
	status chan FlowControlStatus
}

func (f *recordingFlowControl) InitialLimit(int) int64 {
	return 64
}

func (f *recordingFlowControl) OnStatus(status FlowControlStatus, _ int64) int64 {
	f.status <- status
	return status.Position + int64(status.WindowLength)
}

func (f *recordingFlowControl) OnIdle(_ time.Time, senderLimit, _ int64) int64 {
	return senderLimit
}

func sendDataFrame(ctx context.Context, pub *Publication, streamID, sessionID uint32, seq uint64, payload []byte) error {
	packet, err := encodeFrame(frame{
		typ:       frameData,
		streamID:  streamID,
		sessionID: sessionID,
		seq:       seq,
		payload:   payload,
	})
	if err != nil {
		return err
	}
	_, err = pub.roundTrip(ctx, packet, 1)
	return err
}

func udpDatagramsForTest(t *testing.T, pub *Publication, seq uint64, payload []byte) (termAppend, [][]byte) {
	t.Helper()
	packet, appendResult, err := pub.encodeDataPacket(seq, 0, payload, countFragments(len(payload), pub.mtuPayload))
	if err != nil {
		t.Fatal(err)
	}
	frames, err := decodeFrames(packet)
	if err != nil {
		t.Fatal(err)
	}
	datagrams := make([][]byte, 0, len(frames))
	for _, f := range frames {
		encoded, err := encodeFrame(f)
		if err != nil {
			t.Fatal(err)
		}
		datagrams = append(datagrams, encoded)
	}
	return appendResult, datagrams
}
