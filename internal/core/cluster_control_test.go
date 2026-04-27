package core

import (
	"context"
	"errors"
	"testing"
)

func TestClusterSuspendResumeRejectsIngress(t *testing.T) {
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return []byte("ok"), nil
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

	if err := node.Suspend(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err := node.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !status.Suspended {
		t.Fatalf("node snapshot suspended = false, want true")
	}
	if _, err := client.Send(context.Background(), []byte("one")); !errors.Is(err, ErrClusterSuspended) {
		t.Fatalf("Send() err = %v, want %v", err, ErrClusterSuspended)
	}

	if err := node.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("one")); err != nil {
		t.Fatal(err)
	}
	status, err = node.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Suspended || status.LastPosition != 1 {
		t.Fatalf("unexpected node snapshot after resume: %#v", status)
	}
}

func TestClusterControlServerOperations(t *testing.T) {
	store := NewInMemoryClusterSnapshotStore()
	service := &snapshotCounterService{}
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		SnapshotStore: store,
		Service:       service,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())
	client, err := node.NewClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	server, err := StartClusterControlServer(node, ClusterControlConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	control := server.Client()

	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}
	description, err := control.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if description.Role != ClusterRoleLeader || description.LastPosition != 1 || description.Suspended {
		t.Fatalf("unexpected cluster description: %#v", description)
	}
	snapshot, err := control.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Position != 1 {
		t.Fatalf("snapshot position = %d, want 1", snapshot.Position)
	}

	if err := control.Suspend(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); !errors.Is(err, ErrClusterSuspended) {
		t.Fatalf("Send() while suspended err = %v, want %v", err, ErrClusterSuspended)
	}
	if err := control.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}
	report, err := control.Validate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !report.Healthy || report.Description.LastPosition != 2 || report.Description.SnapshotPosition != 1 {
		t.Fatalf("unexpected validation report: %#v", report)
	}

	if err := control.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	report, err = control.Validate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Healthy || !report.Description.Closed || len(report.Errors) != 1 {
		t.Fatalf("unexpected validation report after shutdown: %#v", report)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); !errors.Is(err, ErrClusterClosed) {
		t.Fatalf("Send() after shutdown err = %v, want %v", err, ErrClusterClosed)
	}
}

func TestClusterControlServerAuthorizer(t *testing.T) {
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())

	errUnauthorized := errors.New("unauthorized cluster operation")
	var actions []ClusterControlAction
	server, err := StartClusterControlServer(node, ClusterControlConfig{
		Authorizer: func(_ context.Context, action ClusterControlAction) error {
			actions = append(actions, action)
			if action == ClusterControlActionSuspend {
				return errUnauthorized
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	control := server.Client()

	if _, err := control.Describe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := control.Suspend(context.Background()); !errors.Is(err, errUnauthorized) {
		t.Fatalf("Suspend() err = %v, want %v", err, errUnauthorized)
	}
	status, err := node.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Suspended {
		t.Fatalf("node was suspended despite authorization failure: %#v", status)
	}
	if len(actions) != 2 || actions[0] != ClusterControlActionDescribe || actions[1] != ClusterControlActionSuspend {
		t.Fatalf("unexpected authorized actions: %#v", actions)
	}
}

func TestClusterControlServerRejectsAfterClose(t *testing.T) {
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())

	server, err := StartClusterControlServer(node, ClusterControlConfig{})
	if err != nil {
		t.Fatal(err)
	}
	control := server.Client()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := control.Describe(context.Background()); !errors.Is(err, ErrClusterControlClosed) {
		t.Fatalf("Describe() err = %v, want %v", err, ErrClusterControlClosed)
	}
}
