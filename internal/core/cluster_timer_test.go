package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClusterTimerFiresFromLogAndReplays(t *testing.T) {
	log := NewInMemoryClusterLog()
	service := &recordingClusterMessageService{}
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		Log:              log,
		DisableTimerLoop: true,
		Service:          service,
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().UTC().Add(time.Hour)
	timer, err := node.ScheduleTimer(context.Background(), ClusterTimer{
		Deadline: deadline,
		Payload:  []byte("timer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(service.messages) != 0 {
		t.Fatalf("timer schedule delivered service messages: %#v", service.messages)
	}
	early, err := node.FireDueTimers(context.Background(), deadline.Add(-time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}
	if len(early) != 0 {
		t.Fatalf("early timer fire result: %#v", early)
	}
	fired, err := node.FireDueTimers(context.Background(), deadline)
	if err != nil {
		t.Fatal(err)
	}
	if len(fired) != 1 || fired[0].Type != ClusterLogEntryTimerFire ||
		fired[0].TimerID != timer.TimerID || string(fired[0].Payload) != "ack:timer" {
		t.Fatalf("unexpected fired timers: %#v", fired)
	}
	if len(service.messages) != 1 || service.messages[0].Type != ClusterLogEntryTimerFire ||
		service.messages[0].TimerID != timer.TimerID || service.messages[0].Deadline != deadline ||
		string(service.messages[0].Payload) != "timer" {
		t.Fatalf("unexpected timer service messages: %#v", service.messages)
	}
	if err := node.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	replayedService := &recordingClusterMessageService{}
	replayed, err := StartClusterNode(context.Background(), ClusterConfig{
		Log:              log,
		DisableTimerLoop: true,
		Service:          replayedService,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer replayed.Close(context.Background())
	if len(replayedService.messages) != 1 || !replayedService.messages[0].Replay ||
		replayedService.messages[0].Type != ClusterLogEntryTimerFire ||
		replayedService.messages[0].TimerID != timer.TimerID {
		t.Fatalf("unexpected replayed timer messages: %#v", replayedService.messages)
	}
}

func TestClusterTimerCancelPreventsFire(t *testing.T) {
	service := &recordingClusterMessageService{}
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		DisableTimerLoop: true,
		Service:          service,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())
	timer, err := node.ScheduleTimer(context.Background(), ClusterTimer{
		Deadline: time.Now().UTC().Add(time.Hour),
		Payload:  []byte("timer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.CancelTimer(context.Background(), timer.TimerID); err != nil {
		t.Fatal(err)
	}
	fired, err := node.FireDueTimers(context.Background(), timer.Deadline.Add(time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}
	if len(fired) != 0 || len(service.messages) != 0 {
		t.Fatalf("canceled timer fired result=%#v messages=%#v", fired, service.messages)
	}
	if err := node.CancelTimer(context.Background(), timer.TimerID); !errors.Is(err, ErrClusterTimerNotFound) {
		t.Fatalf("CancelTimer() err = %v, want %v", err, ErrClusterTimerNotFound)
	}
}

func TestClusterTimerSnapshotRestoresPendingTimer(t *testing.T) {
	log := NewInMemoryClusterLog()
	store := NewInMemoryClusterSnapshotStore()
	service := &recordingSnapshotClusterMessageService{}
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		Log:              log,
		SnapshotStore:    store,
		DisableTimerLoop: true,
		Service:          service,
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().UTC().Add(time.Hour)
	timer, err := node.ScheduleTimer(context.Background(), ClusterTimer{
		Deadline: deadline,
		Payload:  []byte("timer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := node.TakeSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Position != 1 || len(snapshot.Timers) != 1 ||
		snapshot.Timers[0].TimerID != timer.TimerID || string(snapshot.Timers[0].Payload) != "timer" {
		t.Fatalf("unexpected timer snapshot: %#v", snapshot)
	}
	if err := node.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	recoveredService := &recordingSnapshotClusterMessageService{}
	recovered, err := StartClusterNode(context.Background(), ClusterConfig{
		Log:              log,
		SnapshotStore:    store,
		DisableTimerLoop: true,
		Service:          recoveredService,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close(context.Background())
	fired, err := recovered.FireDueTimers(context.Background(), deadline)
	if err != nil {
		t.Fatal(err)
	}
	if len(fired) != 1 || fired[0].TimerID != timer.TimerID {
		t.Fatalf("unexpected restored timer fire result: %#v", fired)
	}
	if recoveredService.loadedSnapshot.Position != 1 || len(recoveredService.messages) != 1 ||
		recoveredService.messages[0].Type != ClusterLogEntryTimerFire ||
		recoveredService.messages[0].Replay {
		t.Fatalf("unexpected recovered timer service: %#v loaded=%#v", recoveredService.messages, recoveredService.loadedSnapshot)
	}
}

func TestClusterServiceMessageAppendsAndReplays(t *testing.T) {
	log := NewInMemoryClusterLog()
	service := &recordingClusterMessageService{}
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		Log:              log,
		DisableTimerLoop: true,
		Service:          service,
	})
	if err != nil {
		t.Fatal(err)
	}
	egress, err := node.SendServiceMessage(context.Background(), ClusterServiceMessage{
		SourceService: "orders",
		TargetService: "risk",
		Payload:       []byte("rebalance"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if egress.Type != ClusterLogEntryServiceMessage || egress.CorrelationID == 0 ||
		egress.SourceService != "orders" || egress.TargetService != "risk" ||
		string(egress.Payload) != "ack:rebalance" {
		t.Fatalf("unexpected service message egress: %#v", egress)
	}
	if len(service.messages) != 1 || service.messages[0].Type != ClusterLogEntryServiceMessage ||
		service.messages[0].SourceService != "orders" || service.messages[0].TargetService != "risk" {
		t.Fatalf("unexpected service message delivery: %#v", service.messages)
	}
	if err := node.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	replayedService := &recordingClusterMessageService{}
	replayed, err := StartClusterNode(context.Background(), ClusterConfig{
		Log:              log,
		DisableTimerLoop: true,
		Service:          replayedService,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer replayed.Close(context.Background())
	if len(replayedService.messages) != 1 || !replayedService.messages[0].Replay ||
		replayedService.messages[0].Type != ClusterLogEntryServiceMessage ||
		replayedService.messages[0].CorrelationID != egress.CorrelationID {
		t.Fatalf("unexpected replayed service messages: %#v", replayedService.messages)
	}
}

func TestClusterTimerAndServiceMessageRequireLeader(t *testing.T) {
	follower, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Service:           &recordingClusterMessageService{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close(context.Background())
	if _, err := follower.ScheduleTimer(context.Background(), ClusterTimer{Deadline: time.Now().UTC()}); !errors.Is(err, ErrClusterNotLeader) {
		t.Fatalf("ScheduleTimer() err = %v, want %v", err, ErrClusterNotLeader)
	}
	if _, err := follower.FireDueTimers(context.Background(), time.Now().UTC()); !errors.Is(err, ErrClusterNotLeader) {
		t.Fatalf("FireDueTimers() err = %v, want %v", err, ErrClusterNotLeader)
	}
	if _, err := follower.SendServiceMessage(context.Background(), ClusterServiceMessage{}); !errors.Is(err, ErrClusterNotLeader) {
		t.Fatalf("SendServiceMessage() err = %v, want %v", err, ErrClusterNotLeader)
	}
}

type recordingClusterMessageService struct {
	messages []ClusterMessage
}

func (s *recordingClusterMessageService) OnClusterMessage(_ context.Context, msg ClusterMessage) ([]byte, error) {
	s.messages = append(s.messages, msg)
	return append([]byte("ack:"), msg.Payload...), nil
}

type recordingSnapshotClusterMessageService struct {
	recordingClusterMessageService
	loadedSnapshot ClusterStateSnapshot
}

func (s *recordingSnapshotClusterMessageService) SnapshotClusterState(context.Context, ClusterServiceContext) ([]byte, error) {
	return []byte("state"), nil
}

func (s *recordingSnapshotClusterMessageService) LoadClusterSnapshot(_ context.Context, snapshot ClusterStateSnapshot) error {
	s.loadedSnapshot = snapshot
	return nil
}
