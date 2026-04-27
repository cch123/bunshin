package core

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
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

func TestPublicationOfferReturnsPosition(t *testing.T) {
	testPublicationOfferReturnsPosition(t, TransportQUIC, 100)
}

func TestUDPPublicationOfferReturnsPosition(t *testing.T) {
	testPublicationOfferReturnsPosition(t, TransportUDP, 101)
}

func TestPublicationOfferVectored(t *testing.T) {
	testPublicationOfferVectored(t, TransportQUIC, 103)
}

func TestUDPPublicationOfferVectored(t *testing.T) {
	testPublicationOfferVectored(t, TransportUDP, 104)
}

func TestPublicationOfferBackPressured(t *testing.T) {
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  105,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		_ = sub.Serve(ctx, func(context.Context, Message) error {
			return nil
		})
	}()

	pubMetrics := &Metrics{}
	pub, err := DialPublication(PublicationConfig{
		StreamID:               105,
		RemoteAddr:             sub.LocalAddr().String(),
		PublicationWindowBytes: headerLen,
		Metrics:                pubMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	result := pub.Offer(ctx, []byte("payload"))
	if result.Status != OfferBackPressured || !errors.Is(result.Err, ErrBackPressure) || result.Position != 0 {
		t.Fatalf("Offer() = %#v, want back pressured", result)
	}
	if snapshot := pubMetrics.Snapshot(); snapshot.BackPressureEvents != 1 || snapshot.SendErrors != 1 {
		t.Fatalf("unexpected offer backpressure metrics: %#v", snapshot)
	}
}

func TestPublicationClaimCommit(t *testing.T) {
	testPublicationClaimCommit(t, TransportQUIC, 134)
}

func TestUDPPublicationClaimCommit(t *testing.T) {
	testPublicationClaimCommit(t, TransportUDP, 135)
}

func TestPublicationClaimAbort(t *testing.T) {
	pub, err := DialPublication(PublicationConfig{
		Transport:  TransportUDP,
		StreamID:   136,
		RemoteAddr: "127.0.0.1:1",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	claim, err := pub.Claim(8)
	if err != nil {
		t.Fatal(err)
	}
	copy(claim.Buffer(), "discard")
	if err := claim.Abort(); err != nil {
		t.Fatal(err)
	}
	if got := claim.Buffer(); got != nil {
		t.Fatalf("aborted claim buffer = %#v, want nil", got)
	}
	result := claim.Commit(context.Background())
	if result.Status != OfferClosed || !errors.Is(result.Err, ErrPublicationClaimAborted) {
		t.Fatalf("aborted claim Commit() = %#v, want aborted", result)
	}
}

func TestExclusivePublicationSendOfferAndClaim(t *testing.T) {
	testExclusivePublicationSendOfferAndClaim(t, TransportQUIC, 137)
}

func TestUDPExclusivePublicationSendOfferAndClaim(t *testing.T) {
	testExclusivePublicationSendOfferAndClaim(t, TransportUDP, 138)
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

func testPublicationOfferVectored(t *testing.T, transport TransportMode, streamID uint32) {
	t.Helper()
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: transport,
		StreamID:  streamID,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	received := make(chan Message, 1)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		Transport:  transport,
		StreamID:   streamID,
		SessionID:  203,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	result := pub.OfferVectored(ctx, []byte("vec"), []byte("-"), []byte("payload"))
	if result.Status != OfferAccepted || result.Position <= 0 || result.Err != nil {
		t.Fatalf("OfferVectored() = %#v, want accepted position", result)
	}
	expectMessagePayload(t, ctx, received, "vec-payload")
}

func testPublicationOfferReturnsPosition(t *testing.T, transport TransportMode, streamID uint32) {
	t.Helper()
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: transport,
		StreamID:  streamID,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	received := make(chan Message, 1)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		Transport:  transport,
		StreamID:   streamID,
		SessionID:  202,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	result := pub.Offer(ctx, []byte("offer-payload"))
	if result.Status != OfferAccepted || result.Position <= 0 || result.Err != nil {
		t.Fatalf("Offer() = %#v, want accepted position", result)
	}
	expectMessagePayload(t, ctx, received, "offer-payload")
}

func testPublicationClaimCommit(t *testing.T, transport TransportMode, streamID uint32) {
	t.Helper()
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: transport,
		StreamID:  streamID,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	received := make(chan Message, 1)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		Transport:  transport,
		StreamID:   streamID,
		SessionID:  233,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	claim, err := pub.Claim(len("claimed-payload"))
	if err != nil {
		t.Fatal(err)
	}
	copy(claim.Buffer(), "claimed-payload")
	result := claim.Commit(ctx)
	if result.Status != OfferAccepted || result.Position <= 0 || result.Err != nil {
		t.Fatalf("claim Commit() = %#v, want accepted position", result)
	}
	expectMessagePayload(t, ctx, received, "claimed-payload")
	if again := claim.Commit(ctx); again.Status != OfferClosed || !errors.Is(again.Err, ErrPublicationClaimClosed) {
		t.Fatalf("second claim Commit() = %#v, want closed", again)
	}
}

func testExclusivePublicationSendOfferAndClaim(t *testing.T, transport TransportMode, streamID uint32) {
	t.Helper()
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: transport,
		StreamID:  streamID,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	received := make(chan Message, 3)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		})
	}()

	pub, err := DialExclusivePublication(PublicationConfig{
		Transport:  transport,
		StreamID:   streamID,
		SessionID:  234,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Send(ctx, []byte("exclusive-send")); err != nil {
		t.Fatal(err)
	}
	result := pub.OfferVectored(ctx, []byte("exclusive"), []byte("-offer"))
	if result.Status != OfferAccepted || result.Position <= 0 || result.Err != nil {
		t.Fatalf("OfferVectored() = %#v, want accepted", result)
	}
	claim, err := pub.Claim(len("exclusive-claim"))
	if err != nil {
		t.Fatal(err)
	}
	copy(claim.Buffer(), "exclusive-claim")
	result = claim.Commit(ctx)
	if result.Status != OfferAccepted || result.Position <= 0 || result.Err != nil {
		t.Fatalf("claim Commit() = %#v, want accepted", result)
	}

	for _, want := range []string{"exclusive-send", "exclusive-offer", "exclusive-claim"} {
		msg := expectMessagePayload(t, ctx, received, want)
		if msg.StreamID != streamID || msg.SessionID != 234 {
			t.Fatalf("unexpected exclusive message: %#v", msg)
		}
	}
	if pub.LocalAddr() == nil {
		t.Fatal("exclusive local addr is nil")
	}
	if ch := pub.ChannelURI(); ch.Transport != transport || ch.Endpoint == "" {
		t.Fatalf("unexpected exclusive channel uri: %#v", ch)
	}
}

func TestSubscriptionPollReceivesPublication(t *testing.T) {
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  129,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	received := make(chan Message, 1)
	polled := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		n, err := sub.Poll(ctx, func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		})
		polled <- struct {
			n   int
			err error
		}{n: n, err: err}
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:   129,
		SessionID:  228,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Send(ctx, []byte("poll-payload")); err != nil {
		t.Fatal(err)
	}

	select {
	case result := <-polled:
		if result.err != nil || result.n != 1 {
			t.Fatalf("Poll() = %d, %v; want 1, nil", result.n, result.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	msg := expectMessagePayload(t, ctx, received, "poll-payload")
	if msg.StreamID != 129 || msg.SessionID != 228 || msg.Sequence != 1 {
		t.Fatalf("unexpected polled message: %#v", msg)
	}
}

func TestUDPPollNFragmentLimitDefersIncompleteMessage(t *testing.T) {
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: TransportUDP,
		StreamID:  130,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pub, err := DialPublication(PublicationConfig{
		Transport:              TransportUDP,
		StreamID:               130,
		SessionID:              229,
		RemoteAddr:             sub.LocalAddr().String(),
		MaxPayloadBytes:        16,
		MTUBytes:               headerLen + 5,
		PublicationWindowBytes: 256,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	payload := []byte("abcdefghijklmnop")
	sendErr := make(chan error, 1)
	go func() {
		sendErr <- pub.Send(ctx, payload)
	}()

	handlerCalled := false
	n, err := sub.PollN(ctx, 1, func(context.Context, Message) error {
		handlerCalled = true
		return nil
	})
	if err != nil || n != 0 {
		t.Fatalf("first PollN() = %d, %v; want 0, nil", n, err)
	}
	if handlerCalled {
		t.Fatal("handler ran before all UDP fragments were available")
	}
	select {
	case err := <-sendErr:
		t.Fatalf("Send() completed before fragmented message was delivered: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	received := make(chan Message, 1)
	n, err = sub.PollN(ctx, 4, func(_ context.Context, msg Message) error {
		received <- msg
		return nil
	})
	if err != nil || n != 1 {
		t.Fatalf("second PollN() = %d, %v; want 1, nil", n, err)
	}
	msg := expectMessagePayload(t, ctx, received, string(payload))
	if msg.StreamID != 130 || msg.SessionID != 229 || msg.Sequence != 1 {
		t.Fatalf("unexpected polled udp message: %#v", msg)
	}
	select {
	case err := <-sendErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestLocalSpyPollReceivesPublication(t *testing.T) {
	server, err := ListenSubscription(SubscriptionConfig{
		StreamID:  131,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		_ = server.Serve(ctx, func(context.Context, Message) error {
			return nil
		})
	}()

	spy, err := ListenSubscription(SubscriptionConfig{
		StreamID:  131,
		LocalAddr: server.LocalAddr().String(),
		LocalSpy:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer spy.Close()

	pub, err := DialPublication(PublicationConfig{
		StreamID:   131,
		SessionID:  230,
		RemoteAddr: server.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Send(ctx, []byte("spy-poll")); err != nil {
		t.Fatal(err)
	}

	received := make(chan Message, 1)
	n, err := spy.Poll(ctx, func(_ context.Context, msg Message) error {
		received <- msg
		return nil
	})
	if err != nil || n != 1 {
		t.Fatalf("spy Poll() = %d, %v; want 1, nil", n, err)
	}
	msg := expectMessagePayload(t, ctx, received, "spy-poll")
	if msg.StreamID != 131 || msg.SessionID != 230 || msg.Remote.String() != server.LocalAddr().String() {
		t.Fatalf("unexpected spy poll message: %#v", msg)
	}
}

func TestLocalSpyPollBackPressureIsNotReceiveError(t *testing.T) {
	server, err := ListenSubscription(SubscriptionConfig{
		StreamID:  232,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		_ = server.Serve(ctx, func(context.Context, Message) error {
			return nil
		})
	}()

	spyMetrics := &Metrics{}
	spy, err := ListenSubscription(SubscriptionConfig{
		StreamID:  232,
		LocalAddr: server.LocalAddr().String(),
		LocalSpy:  true,
		Metrics:   spyMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer spy.Close()

	pub, err := DialPublication(PublicationConfig{
		StreamID:   232,
		SessionID:  330,
		RemoteAddr: server.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Send(ctx, []byte("spy-back-pressure")); err != nil {
		t.Fatal(err)
	}
	n, err := spy.Poll(ctx, func(context.Context, Message) error {
		return ErrBackPressure
	})
	if n != 0 || !errors.Is(err, ErrBackPressure) {
		t.Fatalf("spy Poll() = %d, %v; want 0, %v", n, err, ErrBackPressure)
	}
	if snapshot := spyMetrics.Snapshot(); snapshot.BackPressureEvents != 1 || snapshot.ReceiveErrors != 0 {
		t.Fatalf("unexpected spy metrics after back pressure: %#v", snapshot)
	}
}

func TestSubscriptionImageLifecycle(t *testing.T) {
	available := make(chan *Image, 1)
	unavailable := make(chan *Image, 1)
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  132,
		LocalAddr: "127.0.0.1:0",
		AvailableImage: func(_ context.Context, image *Image) {
			available <- image
		},
		UnavailableImage: func(_ context.Context, image *Image) {
			unavailable <- image
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	delivered := make(chan *Image, 1)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			if msg.Image == nil {
				return errors.New("missing image")
			}
			if msg.Position <= msg.Image.JoinPosition {
				return errors.New("message position did not advance image")
			}
			delivered <- msg.Image
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:      132,
		SessionID:     231,
		InitialTermID: 42,
		RemoteAddr:    sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Send(ctx, []byte("image-payload")); err != nil {
		t.Fatal(err)
	}

	var image *Image
	select {
	case image = <-available:
		if image.StreamID != 132 || image.SessionID != 231 || image.InitialTermID != 42 ||
			image.TermBufferLength != minTermLength || image.Source == "" {
			t.Fatalf("unexpected available image: %#v", image.Snapshot())
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case deliveredImage := <-delivered:
		if deliveredImage != image {
			t.Fatal("handler received a different image instance")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if image.CurrentPosition() <= image.JoinPosition || image.LastSequence() != 1 {
		t.Fatalf("image was not advanced: %#v", image.Snapshot())
	}
	snapshots := sub.Images()
	if len(snapshots) != 1 || snapshots[0].CurrentPosition != image.CurrentPosition() ||
		snapshots[0].JoinPosition != image.JoinPosition {
		t.Fatalf("unexpected image snapshots: %#v", snapshots)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case closedImage := <-unavailable:
		if closedImage != image || closedImage.UnavailableAt().IsZero() {
			t.Fatalf("unexpected unavailable image: %#v", closedImage.Snapshot())
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestSubscriptionLagReports(t *testing.T) {
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  139,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	handlerStarted := make(chan Message, 1)
	releaseHandler := make(chan struct{})
	go func() {
		_ = sub.Serve(ctx, func(ctx context.Context, msg Message) error {
			handlerStarted <- msg
			select {
			case <-releaseHandler:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:   139,
		SessionID:  235,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	sendErr := make(chan error, 1)
	go func() {
		sendErr <- pub.Send(ctx, []byte("lagging"))
	}()

	select {
	case msg := <-handlerStarted:
		if msg.Image == nil || msg.Position <= msg.Image.JoinPosition {
			t.Fatalf("unexpected lag test message: %#v", msg)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	reports := sub.LagReports()
	if len(reports) != 1 || reports[0].StreamID != 139 || reports[0].SessionID != 235 ||
		reports[0].ObservedPosition <= reports[0].CurrentPosition ||
		reports[0].LagBytes != reports[0].ObservedPosition-reports[0].CurrentPosition ||
		reports[0].LastObservedSequence != 1 || reports[0].LastSequence != 0 {
		t.Fatalf("unexpected lag report while handler is blocked: %#v", reports)
	}

	close(releaseHandler)
	select {
	case err := <-sendErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	reports = sub.LagReports()
	if len(reports) != 1 || reports[0].LagBytes != 0 || reports[0].CurrentPosition != reports[0].ObservedPosition ||
		reports[0].LastSequence != 1 || reports[0].LastObservedSequence != 1 {
		t.Fatalf("unexpected lag report after handler finished: %#v", reports)
	}
}

func TestControlledPollActions(t *testing.T) {
	server, err := ListenSubscription(SubscriptionConfig{
		StreamID:  133,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		_ = server.Serve(ctx, func(context.Context, Message) error {
			return nil
		})
	}()

	spy, err := ListenSubscription(SubscriptionConfig{
		StreamID:  133,
		LocalAddr: server.LocalAddr().String(),
		LocalSpy:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer spy.Close()

	pub, err := DialPublication(PublicationConfig{
		StreamID:   133,
		SessionID:  232,
		RemoteAddr: server.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	for _, payload := range [][]byte{[]byte("one"), []byte("two"), []byte("three")} {
		if err := pub.Send(ctx, payload); err != nil {
			t.Fatal(err)
		}
	}

	var payloads []string
	n, err := spy.ControlledPollN(ctx, 10, func(_ context.Context, msg Message) ControlledPollAction {
		payloads = append(payloads, string(msg.Payload))
		switch len(payloads) {
		case 1:
			return ControlledPollContinue
		case 2:
			return ControlledPollCommit
		default:
			return ControlledPollBreak
		}
	})
	if err != nil || n != 3 {
		t.Fatalf("ControlledPollN() = %d, %v; want 3, nil", n, err)
	}
	if fmt.Sprint(payloads) != "[one two three]" {
		t.Fatalf("controlled payloads = %#v", payloads)
	}

	if err := pub.Send(ctx, []byte("four")); err != nil {
		t.Fatal(err)
	}
	n, err = spy.ControlledPoll(ctx, func(context.Context, Message) ControlledPollAction {
		return ControlledPollAbort
	})
	if !errors.Is(err, ErrControlledPollAbort) || n != 0 {
		t.Fatalf("ControlledPoll() = %d, %v; want abort", n, err)
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
		pubSnapshot.AcksReceived != 1 || pubSnapshot.FramesSent != 2 || pubSnapshot.FramesReceived != 3 ||
		pubSnapshot.FramesDropped != 0 || pubSnapshot.SendErrors != 0 {
		t.Fatalf("unexpected udp publication metrics: %#v", pubSnapshot)
	}
	subSnapshot := subMetrics.Snapshot()
	if subSnapshot.ConnectionsAccepted != 1 || subSnapshot.MessagesReceived != 1 || subSnapshot.BytesReceived != uint64(len("udp payload")) ||
		subSnapshot.AcksSent != 1 || subSnapshot.FramesSent != 3 || subSnapshot.FramesReceived != 2 ||
		subSnapshot.FramesDropped != 0 || subSnapshot.ReceiveErrors != 0 {
		t.Fatalf("unexpected udp subscription metrics: %#v", subSnapshot)
	}
}

func TestPublicationResponseChannel(t *testing.T) {
	testPublicationResponseChannel(t, TransportQUIC, 118, 119)
}

func TestUDPPublicationResponseChannel(t *testing.T) {
	testPublicationResponseChannel(t, TransportUDP, 120, 121)
}

func TestLocalSpySubscriptionObservesPublication(t *testing.T) {
	testLocalSpySubscriptionObservesPublication(t, TransportQUIC, 122)
}

func TestUDPLocalSpySubscriptionObservesPublication(t *testing.T) {
	testLocalSpySubscriptionObservesPublication(t, TransportUDP, 123)
}

func TestUDPPublicationReResolveDestinations(t *testing.T) {
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: TransportUDP,
		StreamID:  124,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	received := make(chan Message, 1)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		Transport:                 TransportUDP,
		StreamID:                  124,
		SessionID:                 227,
		RemoteAddr:                sub.LocalAddr().String(),
		UDPNameResolutionInterval: time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	endpoints := pub.DestinationEndpoints()
	if len(endpoints) != 1 || endpoints[0] != sub.LocalAddr().String() {
		t.Fatalf("destination endpoints = %#v", endpoints)
	}
	if err := pub.ReResolveDestinations(); err != nil {
		t.Fatal(err)
	}
	if got := pub.Destinations(); len(got) != 1 || got[0] != sub.LocalAddr().String() {
		t.Fatalf("resolved destinations = %#v", got)
	}
	if err := pub.Send(ctx, []byte("resolved")); err != nil {
		t.Fatal(err)
	}
	expectMessagePayload(t, ctx, received, "resolved")
	if ch := pub.ChannelURI(); ch.Transport != TransportUDP || ch.Endpoint != sub.LocalAddr().String() ||
		ch.NameResolutionInterval != time.Nanosecond {
		t.Fatalf("unexpected publication channel uri: %#v", ch)
	}
}

func TestUDPChannelURIWildcardPort(t *testing.T) {
	ch, err := ParseChannelURI("bunshin:udp?endpoint=127.0.0.1%3A0")
	if err != nil {
		t.Fatal(err)
	}
	sub, err := ListenSubscription(ch.SubscriptionConfig(SubscriptionConfig{StreamID: 125}))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	bound := sub.ChannelURI()
	if bound.Transport != TransportUDP || bound.Endpoint == "" || bound.Endpoint == "127.0.0.1:0" {
		t.Fatalf("unexpected bound channel uri: %#v", bound)
	}
	addr, err := net.ResolveUDPAddr("udp", bound.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if addr.Port == 0 {
		t.Fatalf("wildcard port was not resolved: %#v", bound)
	}
}

func TestUDPMulticastPublicationSubscription(t *testing.T) {
	ifi := multicastTestInterface(t)
	group := fmt.Sprintf("239.255.0.1:%d", freeUDPPort(t))
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport:             TransportUDP,
		StreamID:              126,
		LocalAddr:             group,
		UDPMulticastInterface: ifi.Name,
	})
	if err != nil {
		t.Skipf("multicast unavailable: %v", err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	received := make(chan Message, 1)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		Transport:             TransportUDP,
		StreamID:              126,
		SessionID:             224,
		RemoteAddr:            group,
		UDPMulticastInterface: ifi.Name,
	})
	if err != nil {
		if isMulticastUnavailable(err) {
			t.Skipf("multicast unavailable: %v", err)
		}
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Send(ctx, []byte("multicast")); err != nil {
		if isMulticastUnavailable(err) {
			t.Skipf("multicast loopback unavailable: %v", err)
		}
		t.Fatal(err)
	}
	select {
	case msg := <-received:
		if msg.StreamID != 126 || msg.SessionID != 224 || string(msg.Payload) != "multicast" {
			t.Fatalf("unexpected multicast message: %#v", msg)
		}
	case <-ctx.Done():
		t.Skipf("multicast loopback unavailable: %v", ctx.Err())
	}
}

func isMulticastUnavailable(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "no route to host") ||
		strings.Contains(message, "network is unreachable") ||
		strings.Contains(message, "can't assign requested address")
}

func TestPublicationResponseChannelFragments(t *testing.T) {
	testPublicationResponseChannelPayload(t, TransportQUIC, 127, 128, []byte("abcdefghijklmnopqrstuvwxyz"), func(cfg *PublicationConfig) {
		cfg.MTUBytes = headerLen + 32
		cfg.MaxPayloadBytes = 64
		cfg.PublicationWindowBytes = 1024
	})
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
		pubSnapshot.AcksReceived != 1 || pubSnapshot.FramesSent != 5 || pubSnapshot.FramesReceived != 3 {
		t.Fatalf("unexpected udp publication metrics: %#v", pubSnapshot)
	}
	subSnapshot := subMetrics.Snapshot()
	if subSnapshot.ConnectionsAccepted != 1 || subSnapshot.MessagesReceived != 1 || subSnapshot.BytesReceived != uint64(len(payload)) ||
		subSnapshot.AcksSent != 1 || subSnapshot.FramesSent != 3 || subSnapshot.FramesReceived != 5 {
		t.Fatalf("unexpected udp subscription metrics: %#v", subSnapshot)
	}
}

func testPublicationResponseChannel(t *testing.T, transport TransportMode, requestStream, responseStream uint32) {
	t.Helper()
	testPublicationResponseChannelPayload(t, transport, requestStream, responseStream, []byte("hello"), nil)
}

func testPublicationResponseChannelPayload(t *testing.T, transport TransportMode, requestStream, responseStream uint32, payload []byte, configureClient func(*PublicationConfig)) {
	t.Helper()
	server, err := ListenSubscription(SubscriptionConfig{
		Transport: transport,
		StreamID:  requestStream,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	replies, err := ListenSubscription(SubscriptionConfig{
		Transport: transport,
		StreamID:  responseStream,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer replies.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	responseCh := make(chan Message, 1)
	go func() {
		_ = replies.Serve(ctx, func(_ context.Context, msg Message) error {
			responseCh <- msg
			return nil
		})
	}()
	go func() {
		_ = server.Serve(ctx, func(ctx context.Context, msg Message) error {
			if !msg.HasResponseChannel() {
				return errors.New("missing response channel")
			}
			if msg.ResponseChannel.Transport != transport || msg.ResponseChannel.RemoteAddr != replies.LocalAddr().String() ||
				msg.ResponseChannel.StreamID != responseStream {
				return errors.New("unexpected response channel")
			}
			return msg.Respond(ctx, []byte("response: "+string(msg.Payload)), PublicationConfig{})
		})
	}()

	clientConfig := PublicationConfig{
		Transport:  transport,
		StreamID:   requestStream,
		SessionID:  221,
		RemoteAddr: server.LocalAddr().String(),
		ResponseChannel: ResponseChannel{
			Transport:  transport,
			RemoteAddr: replies.LocalAddr().String(),
			StreamID:   responseStream,
		},
	}
	if configureClient != nil {
		configureClient(&clientConfig)
	}
	client, err := DialPublication(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Send(ctx, payload); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-responseCh:
		if response.StreamID != responseStream || string(response.Payload) != "response: "+string(payload) {
			t.Fatalf("unexpected response: %#v", response)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func testLocalSpySubscriptionObservesPublication(t *testing.T, transport TransportMode, streamID uint32) {
	t.Helper()
	server, err := ListenSubscription(SubscriptionConfig{
		Transport: transport,
		StreamID:  streamID,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	serverCh := make(chan Message, 1)
	go func() {
		_ = server.Serve(ctx, func(_ context.Context, msg Message) error {
			serverCh <- msg
			return nil
		})
	}()

	spyMetrics := &Metrics{}
	spy, err := ListenSubscription(SubscriptionConfig{
		Transport: transport,
		StreamID:  streamID,
		LocalAddr: server.LocalAddr().String(),
		LocalSpy:  true,
		Metrics:   spyMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer spy.Close()
	spyCh := make(chan Message, 1)
	go func() {
		_ = spy.Serve(ctx, func(_ context.Context, msg Message) error {
			spyCh <- msg
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		Transport:  transport,
		StreamID:   streamID,
		SessionID:  226,
		RemoteAddr: server.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Send(ctx, []byte("spy-payload")); err != nil {
		t.Fatal(err)
	}
	expectMessagePayload(t, ctx, serverCh, "spy-payload")
	spyMsg := expectMessagePayload(t, ctx, spyCh, "spy-payload")
	if spyMsg.StreamID != streamID || spyMsg.SessionID != 226 || spyMsg.Remote.String() != server.LocalAddr().String() {
		t.Fatalf("unexpected spy message: %#v", spyMsg)
	}
	if snapshot := spyMetrics.Snapshot(); snapshot.MessagesReceived != 1 || snapshot.BytesReceived != uint64(len("spy-payload")) ||
		snapshot.FramesReceived != 1 || snapshot.FramesDropped != 0 {
		t.Fatalf("unexpected spy metrics: %#v", snapshot)
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

func TestUDPPublicationDynamicDestinations(t *testing.T) {
	subA, err := ListenSubscription(SubscriptionConfig{
		Transport: TransportUDP,
		StreamID:  117,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subA.Close()
	subB, err := ListenSubscription(SubscriptionConfig{
		Transport: TransportUDP,
		StreamID:  117,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	receivedA := make(chan string, 2)
	receivedB := make(chan string, 2)
	go func() {
		_ = subA.Serve(ctx, func(_ context.Context, msg Message) error {
			receivedA <- string(msg.Payload)
			return nil
		})
	}()
	go func() {
		_ = subB.Serve(ctx, func(_ context.Context, msg Message) error {
			receivedB <- string(msg.Payload)
			return nil
		})
	}()

	pubMetrics := &Metrics{}
	pub, err := DialPublication(PublicationConfig{
		Transport:  TransportUDP,
		StreamID:   117,
		SessionID:  219,
		RemoteAddr: subA.LocalAddr().String(),
		Metrics:    pubMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.AddDestination(subB.LocalAddr().String()); err != nil {
		t.Fatal(err)
	}
	destinations := pub.Destinations()
	if len(destinations) != 2 {
		t.Fatalf("destinations = %#v, want two", destinations)
	}
	if err := pub.Send(ctx, []byte("both")); err != nil {
		t.Fatal(err)
	}
	expectPayload(t, ctx, receivedA, "both")
	expectPayload(t, ctx, receivedB, "both")
	statuses := pub.DestinationStatuses()
	if len(statuses) != 2 {
		t.Fatalf("destination statuses = %#v, want two", statuses)
	}
	for _, status := range statuses {
		if !status.Active || status.Remote == "" || status.LastSequence != 1 ||
			status.LastSetupAt.IsZero() || status.LastStatusAt.IsZero() || status.LastAckAt.IsZero() || status.LastRTT <= 0 ||
			status.SetupFramesReceived != 1 || status.StatusFramesReceived != 1 || status.AckFramesReceived != 1 || status.NAKFramesReceived != 0 {
			t.Fatalf("unexpected active destination status: %#v", status)
		}
	}

	if err := pub.RemoveDestination(subA.LocalAddr().String()); err != nil {
		t.Fatal(err)
	}
	destinations = pub.Destinations()
	if len(destinations) != 1 || destinations[0] != subB.LocalAddr().String() {
		t.Fatalf("destinations after remove = %#v, want %s", destinations, subB.LocalAddr().String())
	}
	statuses = pub.DestinationStatuses()
	if len(statuses) != 1 || statuses[0].Remote != subB.LocalAddr().String() {
		t.Fatalf("destination statuses after remove = %#v, want %s", statuses, subB.LocalAddr().String())
	}
	if err := pub.Send(ctx, []byte("only-b")); err != nil {
		t.Fatal(err)
	}
	expectPayload(t, ctx, receivedB, "only-b")
	select {
	case got := <-receivedA:
		t.Fatalf("subscription A received %q after destination was removed", got)
	case <-time.After(25 * time.Millisecond):
	}

	snapshot := pubMetrics.Snapshot()
	if snapshot.MessagesSent != 2 || snapshot.AcksReceived != 3 {
		t.Fatalf("unexpected publication metrics: %#v", snapshot)
	}
}

func TestUDPDestinationStatusUsesReceiverTimeout(t *testing.T) {
	base := time.Unix(10, 0)
	destination := &udpDestination{
		endpoint:       "127.0.0.1:40456",
		lastFeedbackAt: base,
		lastSequence:   7,
	}
	active := destination.status(base.Add(time.Second), 2*time.Second)
	if !active.Active || active.LastSequence != 7 {
		t.Fatalf("destination should be active: %#v", active)
	}
	inactive := destination.status(base.Add(3*time.Second), 2*time.Second)
	if inactive.Active {
		t.Fatalf("destination should be inactive after timeout: %#v", inactive)
	}
}

func TestUDPDestinationSetupLifecycle(t *testing.T) {
	base := time.Unix(10, 0)
	destination := &udpDestination{}
	if !destination.needsSetup(base, 2*time.Second) {
		t.Fatal("destination should require initial setup")
	}
	destination.lastSetupAt = base
	destination.lastFeedbackAt = base
	if destination.needsSetup(base.Add(time.Second), 2*time.Second) {
		t.Fatal("destination should not require setup while feedback is fresh")
	}
	if !destination.needsSetup(base.Add(3*time.Second), 2*time.Second) {
		t.Fatal("destination should require setup after receiver timeout")
	}
}

func TestUDPPublicationRefreshesSetupAfterReceiverTimeout(t *testing.T) {
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: TransportUDP,
		StreamID:  140,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	received := make(chan string, 3)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- string(msg.Payload)
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		Transport:          TransportUDP,
		StreamID:           140,
		SessionID:          240,
		RemoteAddr:         sub.LocalAddr().String(),
		UDPReceiverTimeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Send(ctx, []byte("one")); err != nil {
		t.Fatal(err)
	}
	expectPayload(t, ctx, received, "one")
	statuses := pub.DestinationStatuses()
	if len(statuses) != 1 || statuses[0].LastSetupAt.IsZero() {
		t.Fatalf("destination statuses after first send = %#v, want setup time", statuses)
	}
	firstSetup := statuses[0].LastSetupAt

	if err := pub.Send(ctx, []byte("two")); err != nil {
		t.Fatal(err)
	}
	expectPayload(t, ctx, received, "two")
	statuses = pub.DestinationStatuses()
	if len(statuses) != 1 || !statuses[0].LastSetupAt.Equal(firstSetup) {
		t.Fatalf("destination setup refreshed early: first=%s statuses=%#v", firstSetup, statuses)
	}

	time.Sleep(30 * time.Millisecond)
	if err := pub.Send(ctx, []byte("three")); err != nil {
		t.Fatal(err)
	}
	expectPayload(t, ctx, received, "three")
	statuses = pub.DestinationStatuses()
	if len(statuses) != 1 || !statuses[0].LastSetupAt.After(firstSetup) {
		t.Fatalf("destination setup did not refresh after timeout: first=%s statuses=%#v", firstSetup, statuses)
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

	acks, err := pub.readUDPResponses(ctx, secondAppend, 2, []net.Addr{pub.udpRemote}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if acks != 1 {
		t.Fatalf("udp acks = %d, want 1", acks)
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
	statuses := pub.DestinationStatuses()
	if len(statuses) != 1 ||
		statuses[0].NAKFramesReceived != 1 ||
		statuses[0].NAKMessagesReceived != 1 ||
		statuses[0].LastNAKFromSequence != 1 ||
		statuses[0].LastNAKToSequence != 1 ||
		statuses[0].RetransmittedFrames != 1 ||
		statuses[0].RetransmittedMessages != 1 ||
		statuses[0].LastNAKAt.IsZero() ||
		statuses[0].LastRetransmitAt.IsZero() {
		t.Fatalf("unexpected destination status after NAK repair: %#v", statuses)
	}
	subSnapshot := subMetrics.Snapshot()
	if subSnapshot.LossGapEvents != 1 || subSnapshot.LossGapMessages != 1 {
		t.Fatalf("unexpected subscription loss metrics: %#v", subSnapshot)
	}
}

func TestUDPDestinationStatusReportsNAKCacheMiss(t *testing.T) {
	pub, err := DialPublication(PublicationConfig{
		Transport:  TransportUDP,
		StreamID:   116,
		SessionID:  218,
		RemoteAddr: "127.0.0.1:1",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	err = pub.applyUDPNak(pub.udpRemote, frame{
		typ:       frameNak,
		streamID:  pub.streamID,
		sessionID: pub.sessionID,
		seq:       2,
		payload: encodeNakPayload(nakPayload{
			fromSequence: 1,
			toSequence:   2,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	statuses := pub.DestinationStatuses()
	if len(statuses) != 1 ||
		statuses[0].NAKFramesReceived != 1 ||
		statuses[0].NAKMessagesReceived != 2 ||
		statuses[0].NAKCacheMisses != 2 ||
		statuses[0].LastNAKFromSequence != 1 ||
		statuses[0].LastNAKToSequence != 2 ||
		statuses[0].LastNAKMissSequence != 2 ||
		statuses[0].RetransmittedFrames != 0 ||
		statuses[0].RetransmittedMessages != 0 ||
		statuses[0].LastNAKAt.IsZero() ||
		statuses[0].LastNAKCacheMissAt.IsZero() {
		t.Fatalf("unexpected destination status after NAK cache miss: %#v", statuses)
	}
}

func TestUDPDestinationStatusReportsResponseTimeout(t *testing.T) {
	pub, err := DialPublication(PublicationConfig{
		Transport:  TransportUDP,
		StreamID:   118,
		SessionID:  220,
		RemoteAddr: "127.0.0.1:1",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = pub.readUDPResponses(ctx, termAppend{TermID: 0, Position: 64}, 7, []net.Addr{pub.udpRemote}, time.Now())
	if err == nil {
		t.Fatal("readUDPResponses() err = nil, want timeout")
	}
	statuses := pub.DestinationStatuses()
	if len(statuses) != 1 ||
		statuses[0].ResponseTimeouts != 1 ||
		statuses[0].LastTimeoutSequence != 7 ||
		statuses[0].LastSequence != 7 ||
		statuses[0].LastResponseTimeoutAt.IsZero() {
		t.Fatalf("unexpected destination status after response timeout: %#v", statuses)
	}
}

func TestUDPReceiverImageTracksOutOfOrderRebuild(t *testing.T) {
	lossObserved := make(chan LossObservation, 1)
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport: TransportUDP,
		StreamID:  141,
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
		Transport:  TransportUDP,
		StreamID:   141,
		SessionID:  241,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	_, firstDatagrams := udpDatagramsForTest(t, pub, 1, []byte("one"))
	_, secondDatagrams := udpDatagramsForTest(t, pub, 2, []byte("two"))
	if err := pub.writeUDPDatagrams(pub.udpRemote, secondDatagrams); err != nil {
		t.Fatal(err)
	}

	select {
	case observation := <-lossObserved:
		if observation.FromSequence != 1 || observation.ToSequence != 1 {
			t.Fatalf("unexpected loss observation: %#v", observation)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	waitForCondition(t, time.Second, func() bool {
		reports := sub.LagReports()
		return len(reports) == 1 &&
			reports[0].LastObservedSequence == 2 &&
			reports[0].LastSequence == 0 &&
			reports[0].ObservedPosition > reports[0].CurrentPosition &&
			reports[0].LagBytes == reports[0].ObservedPosition-reports[0].CurrentPosition &&
			reports[0].RebuildMessages == 1 &&
			reports[0].RebuildFrames == 1 &&
			reports[0].RebuildBytes > 0
	})
	select {
	case got := <-delivered:
		t.Fatalf("delivered sequence %d before missing sequence was repaired", got)
	case <-time.After(25 * time.Millisecond):
	}

	if err := pub.writeUDPDatagrams(pub.udpRemote, firstDatagrams); err != nil {
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

	waitForCondition(t, time.Second, func() bool {
		reports := sub.LagReports()
		return len(reports) == 1 &&
			reports[0].LastObservedSequence == 2 &&
			reports[0].LastSequence == 2 &&
			reports[0].ObservedPosition == reports[0].CurrentPosition &&
			reports[0].LagBytes == 0 &&
			reports[0].RebuildMessages == 0 &&
			reports[0].RebuildFrames == 0 &&
			reports[0].RebuildBytes == 0
	})
}

func TestUDPNakRetryRepeatsPendingGap(t *testing.T) {
	lossObserved := make(chan LossObservation, 2)
	sub, err := ListenSubscription(SubscriptionConfig{
		Transport:           TransportUDP,
		StreamID:            143,
		LocalAddr:           "127.0.0.1:0",
		UDPNakRetryInterval: 5 * time.Millisecond,
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

	go func() {
		_ = sub.Serve(ctx, func(context.Context, Message) error {
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		Transport:  TransportUDP,
		StreamID:   143,
		SessionID:  243,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	_, secondDatagrams := udpDatagramsForTest(t, pub, 2, []byte("two"))
	if err := pub.writeUDPDatagrams(pub.udpRemote, secondDatagrams); err != nil {
		t.Fatal(err)
	}
	select {
	case observation := <-lossObserved:
		if observation.Retry || observation.FromSequence != 1 || observation.ToSequence != 1 {
			t.Fatalf("unexpected first loss observation: %#v", observation)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	select {
	case observation := <-lossObserved:
		if !observation.Retry || observation.FromSequence != 1 || observation.ToSequence != 1 {
			t.Fatalf("unexpected retry loss observation: %#v", observation)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	reports := sub.LossReports()
	if len(reports) != 1 || reports[0].ObservationCount != 1 || reports[0].RetryCount != 1 ||
		reports[0].MissingMessages != 1 || reports[0].LastRetry.IsZero() {
		t.Fatalf("unexpected loss reports after retry: %#v", reports)
	}
	waitForCondition(t, time.Second, func() bool {
		statuses := sub.UDPPeerStatuses()
		return len(statuses) == 1 &&
			statuses[0].Active &&
			statuses[0].FramesReceived == 1 &&
			statuses[0].DataFramesReceived == 1 &&
			statuses[0].NAKFramesSent >= 2 &&
			statuses[0].NAKMessagesSent >= 2 &&
			statuses[0].NAKRetriesSent >= 1 &&
			statuses[0].LastSequence == 2 &&
			statuses[0].LastNAKFromSequence == 1 &&
			statuses[0].LastNAKToSequence == 1 &&
			!statuses[0].LastDataAt.IsZero() &&
			!statuses[0].LastNAKAt.IsZero() &&
			!statuses[0].LastNAKRetryAt.IsZero()
	})
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
	firstPayloadOverhead, err := responseChannelPayloadOverhead(pub.responseChannel)
	if err != nil {
		t.Fatal(err)
	}
	fragmentCount, err := countFragmentsWithFirstOverhead(len(payload), pub.mtuPayload, firstPayloadOverhead)
	if err != nil {
		t.Fatal(err)
	}
	packet, appendResult, err := pub.encodeDataPacket(seq, 0, payload, fragmentCount, pub.responseChannel, firstPayloadOverhead)
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

func expectPayload(t *testing.T, ctx context.Context, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("payload = %q, want %q", got, want)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func expectMessagePayload(t *testing.T, ctx context.Context, ch <-chan Message, want string) Message {
	t.Helper()
	select {
	case msg := <-ch:
		if string(msg.Payload) != want {
			t.Fatalf("payload = %q, want %q", msg.Payload, want)
		}
		return msg
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return Message{}
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("unexpected udp addr: %T", conn.LocalAddr())
	}
	return addr.Port
}

func multicastTestInterface(t *testing.T) net.Interface {
	t.Helper()
	interfaces, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, ifi := range interfaces {
		if ifi.Flags&net.FlagUp == 0 || ifi.Flags&net.FlagMulticast == 0 {
			continue
		}
		if ifi.Flags&net.FlagLoopback == 0 && interfaceHasIPv4(ifi) {
			return ifi
		}
	}
	for _, ifi := range interfaces {
		if ifi.Flags&net.FlagUp != 0 && ifi.Flags&net.FlagMulticast != 0 && interfaceHasIPv4(ifi) {
			return ifi
		}
	}
	t.Skip("no multicast-capable interface")
	return net.Interface{}
}

func interfaceHasIPv4(ifi net.Interface) bool {
	addrs, err := ifi.Addrs()
	if err != nil {
		return false
	}
	for _, addr := range addrs {
		switch value := addr.(type) {
		case *net.IPNet:
			if value.IP.To4() != nil {
				return true
			}
		case *net.IPAddr:
			if value.IP.To4() != nil {
				return true
			}
		}
	}
	return false
}
