package bunshin

import (
	"context"
	"errors"
	"testing"
)

func TestSingleNodeClusterSequencesIngressIntoLog(t *testing.T) {
	var delivered []ClusterMessage
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		Service: ClusterHandler(func(_ context.Context, msg ClusterMessage) ([]byte, error) {
			delivered = append(delivered, msg)
			return append([]byte("ack:"), msg.Payload...), nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())

	client, err := node.NewClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("one")
	egress, err := client.Send(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'x'

	if egress.LogPosition != 1 || egress.SessionID != client.SessionID() || egress.CorrelationID != 1 ||
		string(egress.Payload) != "ack:one" {
		t.Fatalf("unexpected egress: %#v", egress)
	}
	if len(delivered) != 1 || delivered[0].LogPosition != 1 || delivered[0].Replay || string(delivered[0].Payload) != "one" {
		t.Fatalf("unexpected delivered messages: %#v", delivered)
	}
	snapshot, err := node.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Role != ClusterRoleLeader || snapshot.LastPosition != 1 {
		t.Fatalf("unexpected node snapshot: %#v", snapshot)
	}
}

func TestAppointedLeaderFollowerRejectsIngress(t *testing.T) {
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			t.Fatal("follower service should not receive ingress")
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())

	if node.Role() != ClusterRoleFollower {
		t.Fatalf("role = %q, want %q", node.Role(), ClusterRoleFollower)
	}
	if _, err := node.Submit(context.Background(), ClusterIngress{Payload: []byte("x")}); !errors.Is(err, ErrClusterNotLeader) {
		t.Fatalf("Submit() err = %v, want %v", err, ErrClusterNotLeader)
	}
}

func TestClusterReplaysExistingLogOnStart(t *testing.T) {
	log := NewInMemoryClusterLog()
	first, err := StartClusterNode(context.Background(), ClusterConfig{
		Log: log,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := first.NewClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	var replayed []ClusterMessage
	second, err := StartClusterNode(context.Background(), ClusterConfig{
		Log: log,
		Service: ClusterHandler(func(_ context.Context, msg ClusterMessage) ([]byte, error) {
			replayed = append(replayed, msg)
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(context.Background())

	if len(replayed) != 2 || !replayed[0].Replay || !replayed[1].Replay ||
		replayed[0].LogPosition != 1 || string(replayed[0].Payload) != "one" ||
		replayed[1].LogPosition != 2 || string(replayed[1].Payload) != "two" {
		t.Fatalf("unexpected replayed messages: %#v", replayed)
	}
}

func TestClusterLifecycleCallbacks(t *testing.T) {
	service := &recordingClusterService{}
	node, err := StartClusterNode(context.Background(), ClusterConfig{Service: service})
	if err != nil {
		t.Fatal(err)
	}
	client, err := node.NewClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if service.starts != 1 || service.stops != 1 || service.messages != 1 ||
		service.startContext.Role != ClusterRoleLeader || service.stopContext.LastPosition != 1 {
		t.Fatalf("unexpected lifecycle service state: %#v", service)
	}
}

func TestClusterRejectsInvalidConfig(t *testing.T) {
	if _, err := StartClusterNode(context.Background(), ClusterConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("StartClusterNode() err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := StartClusterNode(context.Background(), ClusterConfig{
		Mode:    ClusterModeAppointedLeader,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) { return nil, nil }),
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("StartClusterNode() appointed leader err = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestInMemoryClusterLogSnapshotsClonePayloads(t *testing.T) {
	log := NewInMemoryClusterLog()
	payload := []byte("one")
	entry, err := log.Append(context.Background(), ClusterLogEntry{Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'x'
	entry.Payload[1] = 'x'
	entries, err := log.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || string(entries[0].Payload) != "one" {
		t.Fatalf("snapshot did not clone payloads: %#v", entries)
	}
}

type recordingClusterService struct {
	starts       int
	stops        int
	messages     int
	startContext ClusterServiceContext
	stopContext  ClusterServiceContext
}

func (s *recordingClusterService) OnClusterStart(_ context.Context, ctx ClusterServiceContext) error {
	s.starts++
	s.startContext = ctx
	return nil
}

func (s *recordingClusterService) OnClusterStop(_ context.Context, ctx ClusterServiceContext) error {
	s.stops++
	s.stopContext = ctx
	return nil
}

func (s *recordingClusterService) OnClusterMessage(context.Context, ClusterMessage) ([]byte, error) {
	s.messages++
	return nil, nil
}
