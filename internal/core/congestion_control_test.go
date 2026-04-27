package core

import (
	"testing"
	"time"
)

func TestAIMDUDPCongestionControlAdjustsWindow(t *testing.T) {
	control := &AIMDUDPCongestionControl{
		InitialWindowBytes:  256,
		MinWindowBytes:      128,
		MaxWindowBytes:      512,
		AdditiveIncrease:    64,
		DecreaseNumerator:   1,
		DecreaseDenominator: 2,
	}
	if got := control.InitialWindow(1024); got != 256 {
		t.Fatalf("InitialWindow() = %d, want 256", got)
	}
	afterAck := control.OnFeedback(TransportFeedback{Transport: TransportUDP, RTT: time.Millisecond}, 256, 1024)
	if afterAck != 320 {
		t.Fatalf("window after ack = %d, want 320", afterAck)
	}
	afterLoss := control.OnFeedback(TransportFeedback{Transport: TransportUDP, RetransmittedFrames: 1}, afterAck, 1024)
	if afterLoss != 160 {
		t.Fatalf("window after loss = %d, want 160", afterLoss)
	}
	minClamped := control.OnFeedback(TransportFeedback{Transport: TransportUDP, RetransmittedFrames: 1}, afterLoss, 1024)
	if minClamped != 128 {
		t.Fatalf("window after second loss = %d, want 128", minClamped)
	}
}

func TestUDPCongestionControlAppliesPublicationWindow(t *testing.T) {
	pub, err := DialPublication(PublicationConfig{
		Transport:  TransportUDP,
		StreamID:   142,
		RemoteAddr: "127.0.0.1:1",
		UDPCongestionControl: &AIMDUDPCongestionControl{
			InitialWindowBytes:  256,
			MinWindowBytes:      128,
			MaxWindowBytes:      512,
			AdditiveIncrease:    64,
			DecreaseNumerator:   1,
			DecreaseDenominator: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if got := publicationWindowLimitForTest(pub.window); got != 256 {
		t.Fatalf("initial publication window = %d, want 256", got)
	}
	pub.observeTransportFeedback(TransportFeedback{Transport: TransportUDP, RTT: time.Millisecond})
	if got := publicationWindowLimitForTest(pub.window); got != 320 {
		t.Fatalf("publication window after ack = %d, want 320", got)
	}
	pub.observeTransportFeedback(TransportFeedback{Transport: TransportUDP, RetransmittedFrames: 1})
	if got := publicationWindowLimitForTest(pub.window); got != 160 {
		t.Fatalf("publication window after loss = %d, want 160", got)
	}
}

func publicationWindowLimitForTest(window *publicationWindow) int {
	window.mu.Lock()
	defer window.mu.Unlock()
	return window.limit
}
