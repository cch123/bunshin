package bunshin

import (
	"testing"
	"time"
)

func TestUnicastFlowControlUsesMaxRightEdge(t *testing.T) {
	flow := UnicastFlowControl{}
	limit := flow.InitialLimit(64)
	if limit != 64 {
		t.Fatalf("initial limit = %d, want 64", limit)
	}

	limit = flow.OnStatus(FlowControlStatus{Position: 100, WindowLength: 20}, 64)
	if limit != 120 {
		t.Fatalf("limit = %d, want 120", limit)
	}
	limit = flow.OnStatus(FlowControlStatus{Position: 50, WindowLength: 10}, limit)
	if limit != 120 {
		t.Fatalf("limit after lower right edge = %d, want 120", limit)
	}
}

func TestMaxMulticastFlowControlUsesMaxRightEdge(t *testing.T) {
	flow := MaxMulticastFlowControl{}
	limit := flow.OnStatus(FlowControlStatus{ReceiverID: "a", Position: 100, WindowLength: 20}, 64)
	limit = flow.OnStatus(FlowControlStatus{ReceiverID: "b", Position: 90, WindowLength: 10}, limit)
	if limit != 120 {
		t.Fatalf("limit = %d, want 120", limit)
	}
}

func TestMinMulticastFlowControlUsesSlowestTrackedReceiver(t *testing.T) {
	now := time.Unix(1, 0)
	flow := NewMinMulticastFlowControl(time.Second)

	limit := flow.OnStatus(FlowControlStatus{
		ReceiverID:   "fast",
		Position:     100,
		WindowLength: 50,
		ObservedAt:   now,
	}, 64)
	if limit != 150 {
		t.Fatalf("limit after first receiver = %d, want 150", limit)
	}

	limit = flow.OnStatus(FlowControlStatus{
		ReceiverID:   "slow",
		Position:     80,
		WindowLength: 20,
		ObservedAt:   now,
	}, limit)
	if limit != 100 {
		t.Fatalf("limit after slow receiver = %d, want 100", limit)
	}

	limit = flow.OnIdle(now.Add(2*time.Second), limit, 150)
	if limit != 100 {
		t.Fatalf("limit after receiver timeout = %d, want 100", limit)
	}
}

func TestPreferredMulticastFlowControlUsesPreferredReceivers(t *testing.T) {
	now := time.Unix(1, 0)
	flow := NewPreferredMulticastFlowControl(time.Second, "preferred")

	limit := flow.OnStatus(FlowControlStatus{
		ReceiverID:   "ordinary",
		Position:     100,
		WindowLength: 50,
		ObservedAt:   now,
	}, 64)
	if limit != 150 {
		t.Fatalf("limit after ordinary receiver = %d, want 150", limit)
	}
	limit = flow.OnStatus(FlowControlStatus{
		ReceiverID:   "preferred",
		Position:     80,
		WindowLength: 20,
		ObservedAt:   now,
	}, limit)
	if limit != 100 {
		t.Fatalf("limit after preferred receiver = %d, want 100", limit)
	}
	limit = flow.OnStatus(FlowControlStatus{
		ReceiverID:   "ordinary",
		Position:     40,
		WindowLength: 10,
		ObservedAt:   now,
	}, limit)
	if limit != 100 {
		t.Fatalf("preferred receiver did not retain priority: limit=%d want 100", limit)
	}
	limit = flow.OnIdle(now.Add(2*time.Second), limit, 150)
	if limit != 100 {
		t.Fatalf("limit after preferred timeout = %d, want 100", limit)
	}
	limit = flow.OnStatus(FlowControlStatus{
		ReceiverID:   "ordinary",
		Position:     60,
		WindowLength: 10,
		ObservedAt:   now.Add(3 * time.Second),
	}, limit)
	if limit != 70 {
		t.Fatalf("limit after preferred receiver timed out = %d, want 70", limit)
	}
}
