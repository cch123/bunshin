package bunshin

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClusterAuthenticatorAndAuthorizerGateIngress(t *testing.T) {
	log := NewInMemoryClusterLog()
	errUnauthorized := errors.New("unauthorized cluster ingress")
	var authn []ClusterAuthenticationRequest
	var actions []ClusterAuthorizationAction
	var delivered []ClusterMessage
	var sawAuthenticatedIngress bool

	node, err := StartClusterNode(context.Background(), ClusterConfig{
		Log: log,
		Authenticator: func(_ context.Context, request ClusterAuthenticationRequest) (ClusterPrincipal, error) {
			authn = append(authn, request)
			return ClusterPrincipal{
				Identity: "alice",
				Roles:    []string{"writer"},
				Metadata: map[string]string{"tenant": "blue"},
			}, nil
		},
		Authorizer: func(_ context.Context, request ClusterAuthorizationRequest) error {
			actions = append(actions, request.Action)
			if request.Action == ClusterAuthorizationActionIngress {
				deny := string(request.Payload) == "deny"
				if request.Principal.Identity == "alice" &&
					len(request.Principal.Roles) == 1 &&
					request.Principal.Roles[0] == "writer" &&
					request.SessionID != 0 &&
					request.CorrelationID != 0 {
					sawAuthenticatedIngress = true
				}
				if len(request.Payload) > 0 {
					request.Payload[0] = 'x'
				}
				if len(request.Principal.Roles) > 0 {
					request.Principal.Roles[0] = "mutated"
				}
				if deny {
					return errUnauthorized
				}
			}
			return nil
		},
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
	egress, err := client.Send(context.Background(), []byte("allow"))
	if err != nil {
		t.Fatal(err)
	}
	if string(egress.Payload) != "ack:allow" {
		t.Fatalf("authorizer mutated ingress payload before service delivery: %#v", egress)
	}
	if _, err := client.Send(context.Background(), []byte("deny")); !errors.Is(err, errUnauthorized) {
		t.Fatalf("Send() err = %v, want %v", err, errUnauthorized)
	}

	entries, err := log.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || string(entries[0].Payload) != "allow" {
		t.Fatalf("unexpected log after authorization failure: %#v", entries)
	}
	if len(delivered) != 1 || string(delivered[0].Payload) != "allow" {
		t.Fatalf("unexpected delivered messages: %#v", delivered)
	}
	if len(authn) != 1 || authn[0].NodeID != 1 || authn[0].Role != ClusterRoleLeader {
		t.Fatalf("unexpected authentication requests: %#v", authn)
	}
	if len(actions) != 3 ||
		actions[0] != ClusterAuthorizationActionOpenSession ||
		actions[1] != ClusterAuthorizationActionIngress ||
		actions[2] != ClusterAuthorizationActionIngress ||
		!sawAuthenticatedIngress {
		t.Fatalf("unexpected authorization calls actions=%#v sawAuthenticatedIngress=%v", actions, sawAuthenticatedIngress)
	}
}

func TestClusterNewClientRejectsAuthenticationAndOpenSessionAuthorization(t *testing.T) {
	errRejected := errors.New("rejected cluster client")
	authnNode, err := StartClusterNode(context.Background(), ClusterConfig{
		Authenticator: func(context.Context, ClusterAuthenticationRequest) (ClusterPrincipal, error) {
			return ClusterPrincipal{}, errRejected
		},
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer authnNode.Close(context.Background())
	if _, err := authnNode.NewClient(context.Background()); !errors.Is(err, errRejected) {
		t.Fatalf("NewClient() authentication err = %v, want %v", err, errRejected)
	}

	node, err := StartClusterNode(context.Background(), ClusterConfig{
		Authorizer: func(_ context.Context, request ClusterAuthorizationRequest) error {
			if request.Action == ClusterAuthorizationActionOpenSession {
				return errRejected
			}
			return nil
		},
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())
	if _, err := node.NewClient(context.Background()); !errors.Is(err, errRejected) {
		t.Fatalf("NewClient() err = %v, want %v", err, errRejected)
	}
}

func TestClusterAuthorizerGatesOperationsWithContextPrincipal(t *testing.T) {
	log := NewInMemoryClusterLog()
	store := NewInMemoryClusterSnapshotStore()
	service := &recordingSnapshotClusterMessageService{}
	errUnauthorized := errors.New("unauthorized cluster operation")
	var actions []ClusterAuthorizationAction
	var sawScheduler bool

	node, err := StartClusterNode(context.Background(), ClusterConfig{
		Log:              log,
		SnapshotStore:    store,
		DisableTimerLoop: true,
		Authorizer: func(_ context.Context, request ClusterAuthorizationRequest) error {
			actions = append(actions, request.Action)
			if request.Principal.Identity == "scheduler" {
				sawScheduler = true
			}
			switch request.Action {
			case ClusterAuthorizationActionCancelTimer,
				ClusterAuthorizationActionServiceMessage,
				ClusterAuthorizationActionSnapshot:
				return errUnauthorized
			default:
				return nil
			}
		},
		Service: service,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())

	ctx := ContextWithClusterPrincipal(context.Background(), ClusterPrincipal{Identity: "scheduler"})
	deadline := time.Now().UTC().Add(time.Hour)
	timer, err := node.ScheduleTimer(ctx, ClusterTimer{
		Deadline: deadline,
		Payload:  []byte("timer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.CancelTimer(ctx, timer.TimerID); !errors.Is(err, errUnauthorized) {
		t.Fatalf("CancelTimer() err = %v, want %v", err, errUnauthorized)
	}
	if _, err := node.SendServiceMessage(ctx, ClusterServiceMessage{Payload: []byte("service")}); !errors.Is(err, errUnauthorized) {
		t.Fatalf("SendServiceMessage() err = %v, want %v", err, errUnauthorized)
	}
	if _, err := node.TakeSnapshot(ctx); !errors.Is(err, errUnauthorized) {
		t.Fatalf("TakeSnapshot() err = %v, want %v", err, errUnauthorized)
	}

	fired, err := node.FireDueTimers(context.Background(), deadline)
	if err != nil {
		t.Fatal(err)
	}
	if len(fired) != 1 || fired[0].TimerID != timer.TimerID {
		t.Fatalf("cancelled timer should still be pending after authorization failure: %#v", fired)
	}
	entries, err := log.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Type != ClusterLogEntryTimerSchedule || entries[1].Type != ClusterLogEntryTimerFire {
		t.Fatalf("unexpected log entries after authorization failures: %#v", entries)
	}
	if _, ok, err := store.Load(context.Background()); err != nil || ok {
		t.Fatalf("snapshot store changed despite authorization failure ok=%v err=%v", ok, err)
	}
	if !sawScheduler || len(actions) != 4 ||
		actions[0] != ClusterAuthorizationActionScheduleTimer ||
		actions[1] != ClusterAuthorizationActionCancelTimer ||
		actions[2] != ClusterAuthorizationActionServiceMessage ||
		actions[3] != ClusterAuthorizationActionSnapshot {
		t.Fatalf("unexpected authorization actions=%#v sawScheduler=%v", actions, sawScheduler)
	}
}
