package core

import (
	"context"
	"sort"
	"sync"
)

type ClusterMember struct {
	NodeID         ClusterNodeID `json:"node_id"`
	Role           ClusterRole   `json:"role"`
	Version        string        `json:"version,omitempty"`
	Voting         bool          `json:"voting"`
	Active         bool          `json:"active"`
	SyncedPosition int64         `json:"synced_position,omitempty"`
}

type ClusterMembership struct {
	LeaderID    ClusterNodeID   `json:"leader_id"`
	LogPosition int64           `json:"log_position"`
	Members     []ClusterMember `json:"members"`
}

type ClusterMembershipChange struct {
	Add              []ClusterMember `json:"add,omitempty"`
	Remove           []ClusterNodeID `json:"remove,omitempty"`
	Promote          []ClusterNodeID `json:"promote,omitempty"`
	Demote           []ClusterNodeID `json:"demote,omitempty"`
	TransferLeaderTo ClusterNodeID   `json:"transfer_leader_to,omitempty"`
}

type ClusterMembershipAction string

const (
	ClusterMembershipActionAddStandby     ClusterMembershipAction = "add_standby"
	ClusterMembershipActionCatchUp        ClusterMembershipAction = "catch_up"
	ClusterMembershipActionPromoteVoter   ClusterMembershipAction = "promote_voter"
	ClusterMembershipActionDemoteVoter    ClusterMembershipAction = "demote_voter"
	ClusterMembershipActionTransferLeader ClusterMembershipAction = "transfer_leader"
	ClusterMembershipActionRemoveMember   ClusterMembershipAction = "remove_member"
	ClusterMembershipActionUpgradeMember  ClusterMembershipAction = "upgrade_member"
	ClusterMembershipActionValidate       ClusterMembershipAction = "validate"
)

type ClusterMembershipStep struct {
	Action      ClusterMembershipAction `json:"action"`
	NodeID      ClusterNodeID           `json:"node_id,omitempty"`
	FromVersion string                  `json:"from_version,omitempty"`
	ToVersion   string                  `json:"to_version,omitempty"`
	Detail      string                  `json:"detail,omitempty"`
}

type ClusterMembershipPlan struct {
	Current  ClusterMembership       `json:"current"`
	Result   ClusterMembership       `json:"result"`
	Steps    []ClusterMembershipStep `json:"steps"`
	Warnings []string                `json:"warnings,omitempty"`
	Safe     bool                    `json:"safe"`
}

type ClusterRollingUpgradeConfig struct {
	Membership       ClusterMembership
	TargetVersion    string
	AllowLeaderFirst bool
}

type ClusterRollingUpgradePlan struct {
	TargetVersion string                  `json:"target_version"`
	Steps         []ClusterMembershipStep `json:"steps"`
	Warnings      []string                `json:"warnings,omitempty"`
	Safe          bool                    `json:"safe"`
}

type ClusterMembershipRuntimeConfig struct {
	Membership ClusterMembership
	Hooks      ClusterMembershipRuntimeHooks
}

type ClusterMembershipRuntimeHooks struct {
	CatchUp        func(context.Context, ClusterMembership, ClusterMember) (ClusterMember, error)
	TransferLeader func(context.Context, ClusterMembership, ClusterNodeID) error
	UpgradeMember  func(context.Context, ClusterMembership, ClusterMember, string) (ClusterMember, error)
	Validate       func(context.Context, ClusterMembership) error
}

type ClusterMembershipRuntime struct {
	mu         sync.Mutex
	membership ClusterMembership
	hooks      ClusterMembershipRuntimeHooks
}

type ClusterMembershipApplyResult struct {
	Previous   ClusterMembership       `json:"previous"`
	Membership ClusterMembership       `json:"membership"`
	Steps      []ClusterMembershipStep `json:"steps"`
	Applied    []ClusterMembershipStep `json:"applied"`
	Safe       bool                    `json:"safe"`
}

func PlanClusterMembershipChange(current ClusterMembership, change ClusterMembershipChange) (ClusterMembershipPlan, error) {
	normalized, err := normalizeClusterMembership(current)
	if err != nil {
		return ClusterMembershipPlan{}, err
	}
	working := cloneClusterMembership(normalized)
	members := clusterMemberMap(working.Members)
	var steps []ClusterMembershipStep

	if change.TransferLeaderTo != 0 && change.TransferLeaderTo != working.LeaderID {
		member, ok := members[change.TransferLeaderTo]
		if !ok {
			return ClusterMembershipPlan{}, invalidConfigf("transfer leader target is not a member: %d", change.TransferLeaderTo)
		}
		if !member.Active || !member.Voting {
			return ClusterMembershipPlan{}, invalidConfigf("transfer leader target must be an active voter: %d", change.TransferLeaderTo)
		}
		steps = append(steps, ClusterMembershipStep{
			Action: ClusterMembershipActionTransferLeader,
			NodeID: change.TransferLeaderTo,
			Detail: "transfer leadership before membership change",
		})
		working.LeaderID = change.TransferLeaderTo
	}

	for _, member := range change.Add {
		member = normalizeClusterMember(member, working.LeaderID)
		if member.NodeID == 0 {
			return ClusterMembershipPlan{}, invalidConfigf("added member id is required")
		}
		if _, ok := members[member.NodeID]; ok {
			return ClusterMembershipPlan{}, invalidConfigf("member already exists: %d", member.NodeID)
		}
		added := member
		added.Voting = false
		if added.Role == "" || added.Role == ClusterRoleLeader || added.Role == ClusterRoleFollower {
			added.Role = ClusterRoleStandby
		}
		members[added.NodeID] = added
		steps = append(steps, ClusterMembershipStep{
			Action: ClusterMembershipActionAddStandby,
			NodeID: added.NodeID,
			Detail: "add member as non-voting standby",
		})
		if member.SyncedPosition < working.LogPosition {
			steps = append(steps, ClusterMembershipStep{
				Action: ClusterMembershipActionCatchUp,
				NodeID: member.NodeID,
				Detail: "catch member up before promotion",
			})
			member.SyncedPosition = working.LogPosition
		}
		if member.Voting {
			steps = append(steps, ClusterMembershipStep{
				Action: ClusterMembershipActionPromoteVoter,
				NodeID: member.NodeID,
				Detail: "promote caught-up member to voting set",
			})
		}
		members[member.NodeID] = normalizeClusterMember(member, working.LeaderID)
	}

	for _, nodeID := range change.Promote {
		member, ok := members[nodeID]
		if !ok {
			return ClusterMembershipPlan{}, invalidConfigf("promoted member is not a member: %d", nodeID)
		}
		if member.SyncedPosition < working.LogPosition {
			steps = append(steps, ClusterMembershipStep{
				Action: ClusterMembershipActionCatchUp,
				NodeID: nodeID,
				Detail: "catch member up before promotion",
			})
			member.SyncedPosition = working.LogPosition
		}
		member.Active = true
		member.Voting = true
		member.Role = roleForMembershipMember(nodeID, working.LeaderID, member.Voting)
		members[nodeID] = member
		steps = append(steps, ClusterMembershipStep{
			Action: ClusterMembershipActionPromoteVoter,
			NodeID: nodeID,
			Detail: "promote existing member to voting set",
		})
	}

	for _, nodeID := range change.Demote {
		if nodeID == working.LeaderID {
			return ClusterMembershipPlan{}, invalidConfigf("cannot demote current leader without transfer: %d", nodeID)
		}
		member, ok := members[nodeID]
		if !ok {
			return ClusterMembershipPlan{}, invalidConfigf("demoted member is not a member: %d", nodeID)
		}
		member.Voting = false
		member.Role = ClusterRoleStandby
		members[nodeID] = member
		steps = append(steps, ClusterMembershipStep{
			Action: ClusterMembershipActionDemoteVoter,
			NodeID: nodeID,
			Detail: "demote member from voting set",
		})
	}

	for _, nodeID := range change.Remove {
		if nodeID == working.LeaderID {
			return ClusterMembershipPlan{}, invalidConfigf("cannot remove current leader without transfer: %d", nodeID)
		}
		member, ok := members[nodeID]
		if !ok {
			return ClusterMembershipPlan{}, invalidConfigf("removed member is not a member: %d", nodeID)
		}
		if member.Voting {
			steps = append(steps, ClusterMembershipStep{
				Action: ClusterMembershipActionDemoteVoter,
				NodeID: nodeID,
				Detail: "demote member before removal",
			})
		}
		delete(members, nodeID)
		steps = append(steps, ClusterMembershipStep{
			Action: ClusterMembershipActionRemoveMember,
			NodeID: nodeID,
			Detail: "remove member from cluster membership",
		})
	}

	working.Members = sortedClusterMembers(members)
	if err := validateMembershipTransition(normalized, working); err != nil {
		return ClusterMembershipPlan{}, err
	}
	steps = append(steps, ClusterMembershipStep{
		Action: ClusterMembershipActionValidate,
		Detail: "validate quorum and leader after membership change",
	})
	return ClusterMembershipPlan{
		Current: normalized,
		Result:  working,
		Steps:   steps,
		Safe:    true,
	}, nil
}

func PlanClusterRollingUpgrade(cfg ClusterRollingUpgradeConfig) (ClusterRollingUpgradePlan, error) {
	if cfg.TargetVersion == "" {
		return ClusterRollingUpgradePlan{}, invalidConfigf("rolling upgrade target version is required")
	}
	membership, err := normalizeClusterMembership(cfg.Membership)
	if err != nil {
		return ClusterRollingUpgradePlan{}, err
	}
	members := make([]ClusterMember, 0, len(membership.Members))
	for _, member := range membership.Members {
		if member.Version != cfg.TargetVersion {
			members = append(members, member)
		}
	}
	sort.Slice(members, func(i, j int) bool {
		leftLeader := members[i].NodeID == membership.LeaderID
		rightLeader := members[j].NodeID == membership.LeaderID
		if leftLeader != rightLeader && !cfg.AllowLeaderFirst {
			return !leftLeader
		}
		return members[i].NodeID < members[j].NodeID
	})

	activeVoters := activeVotingClusterMembers(membership.Members)
	quorum := clusterQuorum(votingClusterMembers(membership.Members))
	var steps []ClusterMembershipStep
	leaderUpgradePlanned := false
	for _, member := range members {
		if member.Voting && member.Active && activeVoters-1 < quorum {
			return ClusterRollingUpgradePlan{}, invalidConfigf("upgrading member would break quorum: %d", member.NodeID)
		}
		if member.NodeID == membership.LeaderID {
			leaderUpgradePlanned = true
			if !cfg.AllowLeaderFirst && len(membership.Members) > 1 {
				target := firstUpgradeTransferTarget(membership, cfg.TargetVersion)
				if target == 0 {
					return ClusterRollingUpgradePlan{}, invalidConfigf("rolling upgrade leader transfer target is unavailable")
				}
				steps = append(steps, ClusterMembershipStep{
					Action: ClusterMembershipActionTransferLeader,
					NodeID: target,
					Detail: "transfer leadership before upgrading current leader",
				})
			}
		}
		steps = append(steps, ClusterMembershipStep{
			Action:      ClusterMembershipActionUpgradeMember,
			NodeID:      member.NodeID,
			FromVersion: member.Version,
			ToVersion:   cfg.TargetVersion,
			Detail:      "upgrade one member and restart it before continuing",
		})
		if member.SyncedPosition < membership.LogPosition {
			steps = append(steps, ClusterMembershipStep{
				Action: ClusterMembershipActionCatchUp,
				NodeID: member.NodeID,
				Detail: "catch upgraded member up before the next upgrade",
			})
		}
		steps = append(steps, ClusterMembershipStep{
			Action: ClusterMembershipActionValidate,
			NodeID: member.NodeID,
			Detail: "validate member health and quorum before continuing",
		})
	}
	if leaderUpgradePlanned {
		steps = append(steps, ClusterMembershipStep{
			Action: ClusterMembershipActionValidate,
			Detail: "validate upgraded leader can rejoin or be re-elected",
		})
	}
	if len(steps) == 0 {
		steps = append(steps, ClusterMembershipStep{
			Action: ClusterMembershipActionValidate,
			Detail: "all members already run the target version",
		})
	}
	return ClusterRollingUpgradePlan{
		TargetVersion: cfg.TargetVersion,
		Steps:         steps,
		Safe:          true,
	}, nil
}

func NewClusterMembershipRuntime(cfg ClusterMembershipRuntimeConfig) (*ClusterMembershipRuntime, error) {
	membership, err := normalizeClusterMembership(cfg.Membership)
	if err != nil {
		return nil, err
	}
	return &ClusterMembershipRuntime{
		membership: membership,
		hooks:      cfg.Hooks,
	}, nil
}

func (r *ClusterMembershipRuntime) Snapshot() ClusterMembership {
	if r == nil {
		return ClusterMembership{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneClusterMembership(r.membership)
}

func (r *ClusterMembershipRuntime) ApplyChange(ctx context.Context, change ClusterMembershipChange) (ClusterMembershipApplyResult, error) {
	if r == nil {
		return ClusterMembershipApplyResult{}, invalidConfigf("cluster membership runtime is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	previous := cloneClusterMembership(r.membership)
	plan, err := PlanClusterMembershipChange(previous, change)
	if err != nil {
		return ClusterMembershipApplyResult{}, err
	}
	addedMembers := clusterMembershipAddedStandbyMembers(change, previous.LeaderID)
	membership, applied, err := r.applyMembershipSteps(ctx, previous, plan.Result, plan.Steps, addedMembers)
	if err != nil {
		return ClusterMembershipApplyResult{}, err
	}
	r.membership = membership
	return ClusterMembershipApplyResult{
		Previous:   previous,
		Membership: cloneClusterMembership(membership),
		Steps:      append([]ClusterMembershipStep(nil), plan.Steps...),
		Applied:    applied,
		Safe:       true,
	}, nil
}

func (r *ClusterMembershipRuntime) ApplyRollingUpgrade(ctx context.Context, targetVersion string, allowLeaderFirst bool) (ClusterMembershipApplyResult, error) {
	if r == nil {
		return ClusterMembershipApplyResult{}, invalidConfigf("cluster membership runtime is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	previous := cloneClusterMembership(r.membership)
	plan, err := PlanClusterRollingUpgrade(ClusterRollingUpgradeConfig{
		Membership:       previous,
		TargetVersion:    targetVersion,
		AllowLeaderFirst: allowLeaderFirst,
	})
	if err != nil {
		return ClusterMembershipApplyResult{}, err
	}
	membership, applied, err := r.applyMembershipSteps(ctx, previous, ClusterMembership{}, plan.Steps, nil)
	if err != nil {
		return ClusterMembershipApplyResult{}, err
	}
	r.membership = membership
	return ClusterMembershipApplyResult{
		Previous:   previous,
		Membership: cloneClusterMembership(membership),
		Steps:      append([]ClusterMembershipStep(nil), plan.Steps...),
		Applied:    applied,
		Safe:       true,
	}, nil
}

func normalizeClusterMembership(membership ClusterMembership) (ClusterMembership, error) {
	if len(membership.Members) == 0 {
		return ClusterMembership{}, invalidConfigf("cluster membership members are required")
	}
	members := make(map[ClusterNodeID]ClusterMember, len(membership.Members))
	for _, member := range membership.Members {
		member = normalizeClusterMember(member, membership.LeaderID)
		if member.NodeID == 0 {
			return ClusterMembership{}, invalidConfigf("cluster membership member id is required")
		}
		if _, ok := members[member.NodeID]; ok {
			return ClusterMembership{}, invalidConfigf("duplicate cluster membership member: %d", member.NodeID)
		}
		members[member.NodeID] = member
	}
	if membership.LeaderID == 0 {
		membership.LeaderID = firstActiveVotingClusterMember(sortedClusterMembers(members))
	}
	leader, ok := members[membership.LeaderID]
	if !ok {
		return ClusterMembership{}, invalidConfigf("cluster membership leader is not a member: %d", membership.LeaderID)
	}
	if !leader.Active || !leader.Voting {
		return ClusterMembership{}, invalidConfigf("cluster membership leader must be an active voter: %d", membership.LeaderID)
	}
	for id, member := range members {
		member.Role = roleForMembershipMember(id, membership.LeaderID, member.Voting)
		members[id] = member
	}
	membership.Members = sortedClusterMembers(members)
	if activeVotingClusterMembers(membership.Members) < clusterQuorum(votingClusterMembers(membership.Members)) {
		return ClusterMembership{}, invalidConfigf("cluster membership does not have active quorum")
	}
	return membership, nil
}

func (r *ClusterMembershipRuntime) applyMembershipSteps(ctx context.Context, current, planned ClusterMembership, steps []ClusterMembershipStep, addedMembers map[ClusterNodeID]ClusterMember) (ClusterMembership, []ClusterMembershipStep, error) {
	working := cloneClusterMembership(current)
	members := clusterMemberMap(working.Members)
	plannedMembers := clusterMemberMap(planned.Members)
	applied := make([]ClusterMembershipStep, 0, len(steps))

	for _, step := range steps {
		select {
		case <-ctx.Done():
			return current, applied, ctx.Err()
		default:
		}
		switch step.Action {
		case ClusterMembershipActionAddStandby:
			member, ok := addedMembers[step.NodeID]
			if !ok {
				member, ok = plannedMembers[step.NodeID]
			}
			if !ok {
				return current, applied, invalidConfigf("planned member is missing: %d", step.NodeID)
			}
			member.Voting = false
			member.Role = ClusterRoleStandby
			members[step.NodeID] = member
		case ClusterMembershipActionCatchUp:
			member, ok := members[step.NodeID]
			if !ok {
				return current, applied, invalidConfigf("catch-up member is not present: %d", step.NodeID)
			}
			caughtUp, err := r.catchUpMember(ctx, workingWithMembers(working, members), member)
			if err != nil {
				return current, applied, err
			}
			if caughtUp.SyncedPosition < working.LogPosition {
				return current, applied, invalidConfigf("member did not catch up: %d synced=%d required=%d", step.NodeID, caughtUp.SyncedPosition, working.LogPosition)
			}
			members[step.NodeID] = normalizeClusterMember(caughtUp, working.LeaderID)
		case ClusterMembershipActionPromoteVoter:
			member, ok := members[step.NodeID]
			if !ok {
				return current, applied, invalidConfigf("promoted member is not present: %d", step.NodeID)
			}
			if member.SyncedPosition < working.LogPosition {
				return current, applied, invalidConfigf("promoted member is behind log: %d synced=%d required=%d", step.NodeID, member.SyncedPosition, working.LogPosition)
			}
			member.Active = true
			member.Voting = true
			member.Role = roleForMembershipMember(step.NodeID, working.LeaderID, true)
			members[step.NodeID] = member
		case ClusterMembershipActionDemoteVoter:
			member, ok := members[step.NodeID]
			if !ok {
				return current, applied, invalidConfigf("demoted member is not present: %d", step.NodeID)
			}
			member.Voting = false
			member.Role = ClusterRoleStandby
			members[step.NodeID] = member
		case ClusterMembershipActionTransferLeader:
			member, ok := members[step.NodeID]
			if !ok {
				return current, applied, invalidConfigf("leader transfer target is not present: %d", step.NodeID)
			}
			if !member.Active || !member.Voting {
				return current, applied, invalidConfigf("leader transfer target is not an active voter: %d", step.NodeID)
			}
			if r.hooks.TransferLeader != nil {
				if err := r.hooks.TransferLeader(ctx, workingWithMembers(working, members), step.NodeID); err != nil {
					return current, applied, err
				}
			}
			working.LeaderID = step.NodeID
			for id, member := range members {
				member.Role = roleForMembershipMember(id, working.LeaderID, member.Voting)
				if id == working.LeaderID {
					member.Active = true
					member.Voting = true
				}
				members[id] = member
			}
		case ClusterMembershipActionRemoveMember:
			if _, ok := members[step.NodeID]; !ok {
				return current, applied, invalidConfigf("removed member is not present: %d", step.NodeID)
			}
			delete(members, step.NodeID)
		case ClusterMembershipActionUpgradeMember:
			member, ok := members[step.NodeID]
			if !ok {
				return current, applied, invalidConfigf("upgraded member is not present: %d", step.NodeID)
			}
			upgraded := member
			var err error
			if r.hooks.UpgradeMember != nil {
				upgraded, err = r.hooks.UpgradeMember(ctx, workingWithMembers(working, members), cloneClusterMember(member), step.ToVersion)
				if err != nil {
					return current, applied, err
				}
			} else {
				upgraded.Version = step.ToVersion
			}
			upgraded.NodeID = member.NodeID
			if upgraded.Version != step.ToVersion {
				return current, applied, invalidConfigf("member did not upgrade: %d version=%s required=%s", step.NodeID, upgraded.Version, step.ToVersion)
			}
			upgraded.Role = roleForMembershipMember(upgraded.NodeID, working.LeaderID, upgraded.Voting)
			members[step.NodeID] = upgraded
		case ClusterMembershipActionValidate:
			candidate := workingWithMembers(working, members)
			normalized, err := normalizeClusterMembership(candidate)
			if err != nil {
				return current, applied, err
			}
			if r.hooks.Validate != nil {
				if err := r.hooks.Validate(ctx, normalized); err != nil {
					return current, applied, err
				}
			}
			working = normalized
			members = clusterMemberMap(working.Members)
		default:
			return current, applied, invalidConfigf("unsupported cluster membership step: %s", step.Action)
		}
		working.Members = sortedClusterMembers(members)
		applied = append(applied, step)
	}
	normalized, err := normalizeClusterMembership(workingWithMembers(working, members))
	if err != nil {
		return current, applied, err
	}
	return normalized, applied, nil
}

func clusterMembershipAddedStandbyMembers(change ClusterMembershipChange, leaderID ClusterNodeID) map[ClusterNodeID]ClusterMember {
	if len(change.Add) == 0 {
		return nil
	}
	members := make(map[ClusterNodeID]ClusterMember, len(change.Add))
	for _, member := range change.Add {
		member = normalizeClusterMember(member, leaderID)
		member.Voting = false
		if member.Role == "" || member.Role == ClusterRoleLeader || member.Role == ClusterRoleFollower {
			member.Role = ClusterRoleStandby
		}
		members[member.NodeID] = member
	}
	return members
}

func (r *ClusterMembershipRuntime) catchUpMember(ctx context.Context, membership ClusterMembership, member ClusterMember) (ClusterMember, error) {
	if r.hooks.CatchUp == nil {
		if member.SyncedPosition < membership.LogPosition {
			return ClusterMember{}, invalidConfigf("cluster membership catch-up hook is required for member: %d", member.NodeID)
		}
		return member, nil
	}
	caughtUp, err := r.hooks.CatchUp(ctx, membership, cloneClusterMember(member))
	if err != nil {
		return ClusterMember{}, err
	}
	caughtUp.NodeID = member.NodeID
	if caughtUp.Version == "" {
		caughtUp.Version = member.Version
	}
	if caughtUp.Role == "" {
		caughtUp.Role = member.Role
	}
	return caughtUp, nil
}

func workingWithMembers(membership ClusterMembership, members map[ClusterNodeID]ClusterMember) ClusterMembership {
	membership.Members = sortedClusterMembers(members)
	return membership
}

func cloneClusterMember(member ClusterMember) ClusterMember {
	return member
}

func normalizeClusterMember(member ClusterMember, leaderID ClusterNodeID) ClusterMember {
	if member.Role == "" {
		member.Role = roleForMembershipMember(member.NodeID, leaderID, member.Voting)
	}
	if member.NodeID == leaderID {
		member.Role = ClusterRoleLeader
		member.Voting = true
		member.Active = true
	}
	if !member.Voting && (member.Role == ClusterRoleLeader || member.Role == ClusterRoleFollower) {
		member.Role = ClusterRoleStandby
	}
	return member
}

func validateMembershipTransition(current, next ClusterMembership) error {
	if _, err := normalizeClusterMembership(next); err != nil {
		return err
	}
	currentVoters := clusterVotingSet(current.Members)
	nextVoters := clusterVotingSet(next.Members)
	overlap := 0
	for voter := range currentVoters {
		if _, ok := nextVoters[voter]; ok {
			overlap++
		}
	}
	if len(currentVoters) > 0 && len(nextVoters) > 0 && overlap == 0 {
		return invalidConfigf("cluster membership change has no voting-set overlap")
	}
	return nil
}

func roleForMembershipMember(nodeID, leaderID ClusterNodeID, voting bool) ClusterRole {
	if nodeID == leaderID {
		return ClusterRoleLeader
	}
	if voting {
		return ClusterRoleFollower
	}
	return ClusterRoleStandby
}

func clusterMemberMap(members []ClusterMember) map[ClusterNodeID]ClusterMember {
	result := make(map[ClusterNodeID]ClusterMember, len(members))
	for _, member := range members {
		result[member.NodeID] = member
	}
	return result
}

func sortedClusterMembers(members map[ClusterNodeID]ClusterMember) []ClusterMember {
	result := make([]ClusterMember, 0, len(members))
	for _, member := range members {
		result = append(result, member)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].NodeID < result[j].NodeID
	})
	return result
}

func cloneClusterMembership(membership ClusterMembership) ClusterMembership {
	membership.Members = append([]ClusterMember(nil), membership.Members...)
	return membership
}

func votingClusterMembers(members []ClusterMember) int {
	count := 0
	for _, member := range members {
		if member.Voting {
			count++
		}
	}
	return count
}

func activeVotingClusterMembers(members []ClusterMember) int {
	count := 0
	for _, member := range members {
		if member.Voting && member.Active {
			count++
		}
	}
	return count
}

func clusterVotingSet(members []ClusterMember) map[ClusterNodeID]struct{} {
	voters := make(map[ClusterNodeID]struct{})
	for _, member := range members {
		if member.Voting {
			voters[member.NodeID] = struct{}{}
		}
	}
	return voters
}

func clusterQuorum(voters int) int {
	return voters/2 + 1
}

func firstActiveVotingClusterMember(members []ClusterMember) ClusterNodeID {
	for _, member := range members {
		if member.Active && member.Voting {
			return member.NodeID
		}
	}
	return 0
}

func firstUpgradeTransferTarget(membership ClusterMembership, targetVersion string) ClusterNodeID {
	for _, member := range membership.Members {
		if member.NodeID != membership.LeaderID && member.Active && member.Voting && member.Version == targetVersion {
			return member.NodeID
		}
	}
	for _, member := range membership.Members {
		if member.NodeID != membership.LeaderID && member.Active && member.Voting {
			return member.NodeID
		}
	}
	return 0
}
