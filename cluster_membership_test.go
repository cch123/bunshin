package bunshin

import (
	"context"
	"errors"
	"testing"
)

func TestPlanClusterMembershipChangeAddsVoterThroughStandby(t *testing.T) {
	current := testMembership()
	plan, err := PlanClusterMembershipChange(current, ClusterMembershipChange{
		Add: []ClusterMember{{
			NodeID:         4,
			Version:        "1.0.0",
			Voting:         true,
			Active:         true,
			SyncedPosition: 5,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Safe || len(plan.Result.Members) != 4 {
		t.Fatalf("unexpected membership plan: %#v", plan)
	}
	member := clusterMemberMap(plan.Result.Members)[4]
	if !member.Voting || member.Role != ClusterRoleFollower || member.SyncedPosition != current.LogPosition {
		t.Fatalf("unexpected added member: %#v", member)
	}
	if len(plan.Steps) != 4 ||
		plan.Steps[0].Action != ClusterMembershipActionAddStandby ||
		plan.Steps[1].Action != ClusterMembershipActionCatchUp ||
		plan.Steps[2].Action != ClusterMembershipActionPromoteVoter ||
		plan.Steps[3].Action != ClusterMembershipActionValidate {
		t.Fatalf("unexpected membership steps: %#v", plan.Steps)
	}
}

func TestPlanClusterMembershipChangeTransfersLeaderBeforeRemoval(t *testing.T) {
	current := testMembership()
	plan, err := PlanClusterMembershipChange(current, ClusterMembershipChange{
		TransferLeaderTo: 2,
		Remove:           []ClusterNodeID{1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Result.LeaderID != 2 {
		t.Fatalf("leader id = %d, want 2", plan.Result.LeaderID)
	}
	if _, ok := clusterMemberMap(plan.Result.Members)[1]; ok {
		t.Fatalf("old leader still present in membership: %#v", plan.Result.Members)
	}
	if len(plan.Steps) < 3 ||
		plan.Steps[0].Action != ClusterMembershipActionTransferLeader ||
		plan.Steps[1].Action != ClusterMembershipActionDemoteVoter ||
		plan.Steps[2].Action != ClusterMembershipActionRemoveMember {
		t.Fatalf("unexpected transfer/remove steps: %#v", plan.Steps)
	}
}

func TestPlanClusterMembershipChangeRejectsUnsafeChanges(t *testing.T) {
	current := testMembership()
	if _, err := PlanClusterMembershipChange(current, ClusterMembershipChange{
		Remove: []ClusterNodeID{1},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("remove leader err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := PlanClusterMembershipChange(current, ClusterMembershipChange{
		Remove: []ClusterNodeID{4},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("remove unknown member err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := PlanClusterMembershipChange(current, ClusterMembershipChange{
		TransferLeaderTo: 4,
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unknown transfer target err = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestPlanClusterRollingUpgradeOrdersFollowersBeforeLeader(t *testing.T) {
	current := testMembership()
	plan, err := PlanClusterRollingUpgrade(ClusterRollingUpgradeConfig{
		Membership:    current,
		TargetVersion: "2.0.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Safe || plan.TargetVersion != "2.0.0" {
		t.Fatalf("unexpected rolling upgrade plan: %#v", plan)
	}
	var upgradeOrder []ClusterNodeID
	for _, step := range plan.Steps {
		if step.Action == ClusterMembershipActionUpgradeMember {
			upgradeOrder = append(upgradeOrder, step.NodeID)
		}
	}
	if len(upgradeOrder) != 3 || upgradeOrder[0] == current.LeaderID || upgradeOrder[2] != current.LeaderID {
		t.Fatalf("unexpected upgrade order: %#v", upgradeOrder)
	}
	foundLeaderTransfer := false
	for _, step := range plan.Steps {
		if step.Action == ClusterMembershipActionTransferLeader {
			foundLeaderTransfer = true
			break
		}
	}
	if !foundLeaderTransfer {
		t.Fatalf("expected leader transfer before upgrading leader: %#v", plan.Steps)
	}
}

func TestPlanClusterRollingUpgradeRejectsQuorumLoss(t *testing.T) {
	current := ClusterMembership{
		LeaderID:    1,
		LogPosition: 10,
		Members: []ClusterMember{
			{NodeID: 1, Version: "1.0.0", Voting: true, Active: true, SyncedPosition: 10},
			{NodeID: 2, Version: "1.0.0", Voting: true, Active: false, SyncedPosition: 10},
			{NodeID: 3, Version: "1.0.0", Voting: true, Active: true, SyncedPosition: 10},
		},
	}
	if _, err := PlanClusterRollingUpgrade(ClusterRollingUpgradeConfig{
		Membership:    current,
		TargetVersion: "2.0.0",
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("rolling quorum loss err = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestClusterMembershipRuntimeAppliesChangeWithCatchUp(t *testing.T) {
	var caughtUp []ClusterNodeID
	var validated []ClusterMembership
	runtime, err := NewClusterMembershipRuntime(ClusterMembershipRuntimeConfig{
		Membership: testMembership(),
		Hooks: ClusterMembershipRuntimeHooks{
			CatchUp: func(_ context.Context, membership ClusterMembership, member ClusterMember) (ClusterMember, error) {
				caughtUp = append(caughtUp, member.NodeID)
				member.SyncedPosition = membership.LogPosition
				return member, nil
			},
			Validate: func(_ context.Context, membership ClusterMembership) error {
				validated = append(validated, membership)
				return nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := runtime.ApplyChange(context.Background(), ClusterMembershipChange{
		Add: []ClusterMember{{
			NodeID:         4,
			Version:        "1.0.0",
			Voting:         true,
			Active:         true,
			SyncedPosition: 5,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Safe || len(result.Applied) != len(result.Steps) || len(caughtUp) != 1 || caughtUp[0] != 4 ||
		len(validated) != 1 {
		t.Fatalf("unexpected apply result=%#v caughtUp=%#v validated=%#v", result, caughtUp, validated)
	}
	member := clusterMemberMap(result.Membership.Members)[4]
	if !member.Active || !member.Voting || member.Role != ClusterRoleFollower || member.SyncedPosition != 10 {
		t.Fatalf("unexpected applied member: %#v", member)
	}
	snapshot := runtime.Snapshot()
	if snapshot.LeaderID != 1 || len(snapshot.Members) != 4 {
		t.Fatalf("unexpected runtime snapshot: %#v", snapshot)
	}
}

func TestClusterMembershipRuntimeRequiresCatchUpBeforePromotion(t *testing.T) {
	runtime, err := NewClusterMembershipRuntime(ClusterMembershipRuntimeConfig{
		Membership: testMembership(),
	})
	if err != nil {
		t.Fatal(err)
	}
	previous := runtime.Snapshot()
	_, err = runtime.ApplyChange(context.Background(), ClusterMembershipChange{
		Add: []ClusterMember{{
			NodeID:         4,
			Version:        "1.0.0",
			Voting:         true,
			Active:         true,
			SyncedPosition: 5,
		}},
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ApplyChange() err = %v, want %v", err, ErrInvalidConfig)
	}
	if snapshot := runtime.Snapshot(); len(snapshot.Members) != len(previous.Members) {
		t.Fatalf("runtime changed after failed catch-up: before=%#v after=%#v", previous, snapshot)
	}
}

func TestClusterMembershipRuntimeAppliesRollingUpgrade(t *testing.T) {
	var transfers []ClusterNodeID
	var upgrades []ClusterNodeID
	runtime, err := NewClusterMembershipRuntime(ClusterMembershipRuntimeConfig{
		Membership: testMembership(),
		Hooks: ClusterMembershipRuntimeHooks{
			TransferLeader: func(_ context.Context, _ ClusterMembership, nodeID ClusterNodeID) error {
				transfers = append(transfers, nodeID)
				return nil
			},
			UpgradeMember: func(_ context.Context, _ ClusterMembership, member ClusterMember, targetVersion string) (ClusterMember, error) {
				upgrades = append(upgrades, member.NodeID)
				member.Version = targetVersion
				member.Active = true
				return member, nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := runtime.ApplyRollingUpgrade(context.Background(), "2.0.0", false)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Safe || len(upgrades) != 3 || len(transfers) != 1 || transfers[0] == 1 {
		t.Fatalf("unexpected rolling result=%#v upgrades=%#v transfers=%#v", result, upgrades, transfers)
	}
	if result.Membership.LeaderID != transfers[0] {
		t.Fatalf("leader id = %d, want transferred target %d", result.Membership.LeaderID, transfers[0])
	}
	for _, member := range result.Membership.Members {
		if member.Version != "2.0.0" {
			t.Fatalf("member was not upgraded: %#v", result.Membership.Members)
		}
	}
}

func testMembership() ClusterMembership {
	return ClusterMembership{
		LeaderID:    1,
		LogPosition: 10,
		Members: []ClusterMember{
			{NodeID: 1, Version: "1.0.0", Voting: true, Active: true, SyncedPosition: 10},
			{NodeID: 2, Version: "1.0.0", Voting: true, Active: true, SyncedPosition: 10},
			{NodeID: 3, Version: "1.0.0", Voting: true, Active: true, SyncedPosition: 10},
		},
	}
}
