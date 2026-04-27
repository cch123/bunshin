package bunshin

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"
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

func TestClusterClientSendBatchSequencesIngressIntoLog(t *testing.T) {
	var delivered []ClusterMessage
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		DisableTimerLoop: true,
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
	egresses, err := client.SendBatch(context.Background(), [][]byte{
		[]byte("one"),
		[]byte("two"),
		[]byte("three"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(egresses) != 3 || egresses[0].LogPosition != 1 || egresses[1].LogPosition != 2 ||
		egresses[2].LogPosition != 3 || string(egresses[0].Payload) != "ack:one" ||
		string(egresses[1].Payload) != "ack:two" || string(egresses[2].Payload) != "ack:three" {
		t.Fatalf("unexpected batch egresses: %#v", egresses)
	}
	if len(delivered) != 3 || delivered[0].LogPosition != 1 || delivered[1].LogPosition != 2 ||
		delivered[2].LogPosition != 3 || delivered[0].CorrelationID != 1 ||
		delivered[1].CorrelationID != 2 || delivered[2].CorrelationID != 3 {
		t.Fatalf("unexpected delivered batch: %#v", delivered)
	}
}

func TestClusterClientAutoBatchQuorumIngress(t *testing.T) {
	memberLog := &recordingClusterQuorumLog{ClusterLog: NewInMemoryClusterLog()}
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return nil, nil
		}),
		Quorum: &ClusterQuorumConfig{
			MemberLogs:  []ClusterLog{memberLog},
			RequiredAck: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())

	client, err := node.NewClientWithConfig(context.Background(), ClusterClientConfig{
		MaxBatchSize:  3,
		MaxBatchDelay: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errs := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func(i int) {
			<-start
			egress, err := client.Send(context.Background(), []byte{byte('a' + i)})
			if err != nil {
				errs <- err
				return
			}
			if egress.LogPosition < 1 || egress.LogPosition > 3 {
				errs <- errors.New("unexpected auto-batch log position")
				return
			}
			errs <- nil
		}(i)
	}
	close(start)
	for i := 0; i < 3; i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for auto-batched sends")
		}
	}

	memberLog.mu.Lock()
	batchSizes := append([]int(nil), memberLog.batchSizes...)
	memberLog.mu.Unlock()
	if len(batchSizes) != 1 || batchSizes[0] != 3 {
		t.Fatalf("unexpected auto-batch quorum sizes: %#v", batchSizes)
	}
}

func TestClusterClientAutoBatchMaxDelayFlushesPartialBatch(t *testing.T) {
	memberLog := &recordingClusterQuorumLog{ClusterLog: NewInMemoryClusterLog()}
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return nil, nil
		}),
		Quorum: &ClusterQuorumConfig{
			MemberLogs:  []ClusterLog{memberLog},
			RequiredAck: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())

	client, err := node.NewClientWithConfig(context.Background(), ClusterClientConfig{
		MaxBatchSize:  3,
		MaxBatchDelay: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("one")); err != nil {
		t.Fatal(err)
	}

	memberLog.mu.Lock()
	batchSizes := append([]int(nil), memberLog.batchSizes...)
	memberLog.mu.Unlock()
	if len(batchSizes) != 1 || batchSizes[0] != 1 {
		t.Fatalf("unexpected max-delay batch sizes: %#v", batchSizes)
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

func TestClusterSnapshotRecoverySkipsReplayedEntries(t *testing.T) {
	log := NewInMemoryClusterLog()
	store := NewInMemoryClusterSnapshotStore()
	firstService := &snapshotCounterService{}
	first, err := StartClusterNode(context.Background(), ClusterConfig{
		Log:           log,
		SnapshotStore: store,
		Service:       firstService,
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := first.NewClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := first.TakeSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Position != 2 || binary.BigEndian.Uint64(snapshot.Payload) != 2 {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	secondService := &snapshotCounterService{}
	second, err := StartClusterNode(context.Background(), ClusterConfig{
		Log:           log,
		SnapshotStore: store,
		Service:       secondService,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(context.Background())

	if secondService.value != 3 || secondService.loadedSnapshotPosition != 2 || secondService.replayed != 1 ||
		secondService.startContext.SnapshotPosition != 2 || secondService.startContext.LastPosition != 3 {
		t.Fatalf("unexpected recovered service: %#v", secondService)
	}
	status, err := second.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.SnapshotPosition != 2 || status.LastPosition != 3 {
		t.Fatalf("unexpected cluster snapshot status: %#v", status)
	}
}

func TestClusterSnapshotRequiresStoreAndServiceSupport(t *testing.T) {
	service := &snapshotCounterService{}
	node, err := StartClusterNode(context.Background(), ClusterConfig{Service: service})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())
	if _, err := node.TakeSnapshot(context.Background()); !errors.Is(err, ErrClusterSnapshotStoreUnavailable) {
		t.Fatalf("TakeSnapshot() err = %v, want %v", err, ErrClusterSnapshotStoreUnavailable)
	}

	store := NewInMemoryClusterSnapshotStore()
	if err := store.Save(context.Background(), ClusterStateSnapshot{Position: 1, Payload: []byte("state")}); err != nil {
		t.Fatal(err)
	}
	_, err = StartClusterNode(context.Background(), ClusterConfig{
		Log:           NewInMemoryClusterLog(),
		SnapshotStore: store,
		Service:       ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) { return nil, nil }),
	})
	if !errors.Is(err, ErrClusterSnapshotUnsupported) {
		t.Fatalf("StartClusterNode() err = %v, want %v", err, ErrClusterSnapshotUnsupported)
	}
}

func TestInMemoryClusterSnapshotStoreClonesPayloads(t *testing.T) {
	store := NewInMemoryClusterSnapshotStore()
	payload := []byte("state")
	if err := store.Save(context.Background(), ClusterStateSnapshot{Position: 3, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'x'
	snapshot, ok, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || snapshot.Position != 3 || string(snapshot.Payload) != "state" {
		t.Fatalf("unexpected loaded snapshot: %#v ok=%v", snapshot, ok)
	}
	snapshot.Payload[1] = 'x'
	again, ok, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(again.Payload) != "state" {
		t.Fatalf("snapshot store did not clone payloads: %#v", again)
	}
}

type recordingClusterService struct {
	starts       int
	stops        int
	messages     int
	startContext ClusterServiceContext
	stopContext  ClusterServiceContext
}

type snapshotCounterService struct {
	mu                     sync.Mutex
	value                  uint64
	replayed               int
	loadedSnapshotPosition int64
	startContext           ClusterServiceContext
}

type recordingClusterQuorumLog struct {
	ClusterLog
	mu         sync.Mutex
	batchSizes []int
}

func (l *recordingClusterQuorumLog) AppendQuorumBatch(ctx context.Context, entries []ClusterLogEntry) error {
	l.mu.Lock()
	l.batchSizes = append(l.batchSizes, len(entries))
	l.mu.Unlock()
	for _, entry := range entries {
		if _, err := l.ClusterLog.Append(ctx, entry); err != nil {
			return err
		}
	}
	return nil
}

func (s *snapshotCounterService) OnClusterStart(_ context.Context, ctx ClusterServiceContext) error {
	s.startContext = ctx
	return nil
}

func (s *snapshotCounterService) OnClusterStop(context.Context, ClusterServiceContext) error {
	return nil
}

func (s *snapshotCounterService) OnClusterMessage(_ context.Context, msg ClusterMessage) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if msg.Replay {
		s.replayed++
	}
	if string(msg.Payload) == "increment" {
		s.value++
	}
	response := make([]byte, 8)
	binary.BigEndian.PutUint64(response, s.value)
	return response, nil
}

func (s *snapshotCounterService) SnapshotClusterState(context.Context, ClusterServiceContext) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, s.value)
	return payload, nil
}

func (s *snapshotCounterService) LoadClusterSnapshot(_ context.Context, snapshot ClusterStateSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadedSnapshotPosition = snapshot.Position
	s.value = binary.BigEndian.Uint64(snapshot.Payload)
	return nil
}

func (s *snapshotCounterService) currentValue() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value
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
