# Cluster

Bunshin cluster support starts with a local replicated-log service container. It is Aeron-inspired in shape, but it is Bunshin-native and does not implement Aeron Cluster wire compatibility.

The first supported modes are:

- `single-node`: the node is always leader and sequences ingress directly into its log.
- `appointed-leader`: a configured node ID is leader; non-leader nodes reject local ingress.
- `learner`: the node does not participate in consensus or accept ingress; it follows a configured master log and snapshots local service state.

Cluster member transport and runtime membership transitions are Bunshin-native layers above this core. Quorum commit gating can be enabled over `ClusterLog` implementations, including local, archive-backed, or remote member clients.

## Node

```go
node, err := bunshin.StartClusterNode(ctx, bunshin.ClusterConfig{
    NodeID: 1,
    Mode:   bunshin.ClusterModeSingleNode,
    Service: bunshin.ClusterHandler(func(ctx context.Context, msg bunshin.ClusterMessage) ([]byte, error) {
        return append([]byte("ack:"), msg.Payload...), nil
    }),
})
defer node.Close(ctx)
```

`ClusterConfig.Log` can provide a durable implementation. If unset, Bunshin uses `NewInMemoryClusterLog`, which is useful for tests and local development. `ClusterConfig.SnapshotStore` enables snapshot-based recovery.

For archive-backed durability, create a normal Bunshin `Archive` and wrap it with `NewArchiveClusterLog` and `NewArchiveClusterSnapshotStore`. Both wrappers use Bunshin-native archive records, so cluster log entries and snapshots share the same segment/catalog tooling as stream recordings.

## Ingress And Egress

Clients use a session and correlation ID. `ClusterClient.Send` creates local ingress, the leader appends it to the cluster log, then invokes the deterministic service callback.

```go
client, err := node.NewClient(ctx)
egress, err := client.Send(ctx, []byte("increment"))
fmt.Println(egress.LogPosition, egress.CorrelationID, string(egress.Payload))
```

Applications can also submit an explicit `ClusterIngress` when they own session and correlation allocation.

## Authentication And Authorization

`ClusterConfig.Authenticator` runs when `NewClient` opens a session. It can validate credentials carried by the request context and return a `ClusterPrincipal` that is attached to the client session.

```go
node, err := bunshin.StartClusterNode(ctx, bunshin.ClusterConfig{
    Authenticator: func(ctx context.Context, request bunshin.ClusterAuthenticationRequest) (bunshin.ClusterPrincipal, error) {
        return bunshin.ClusterPrincipal{Identity: "alice", Roles: []string{"writer"}}, nil
    },
    Authorizer: func(ctx context.Context, request bunshin.ClusterAuthorizationRequest) error {
        if request.Action == bunshin.ClusterAuthorizationActionIngress && request.Principal.Identity == "" {
            return errors.New("unauthorized")
        }
        return nil
    },
    Service: service,
})
```

`ClusterConfig.Authorizer` is called before session open, ingress append, timer schedule/cancel, service message append, and explicit snapshots. The request includes node role, principal, session/correlation IDs, timer metadata, service routing metadata, and a cloned payload. Direct node operations can attach a principal to the context with `ContextWithClusterPrincipal`.

## Replicated Log

`ClusterLog` is the persistence boundary:

```go
type ClusterLog interface {
    Append(context.Context, bunshin.ClusterLogEntry) (bunshin.ClusterLogEntry, error)
    Snapshot(context.Context) ([]bunshin.ClusterLogEntry, error)
    LastPosition(context.Context) (int64, error)
    Close() error
}
```

The log assigns monotonically increasing positions. On node start, existing entries are replayed into the service with `ClusterMessage.Replay = true`, so service state can be rebuilt deterministically from the log.

```go
archive, err := bunshin.OpenArchive(bunshin.ArchiveConfig{Path: "/var/lib/bunshin/cluster-archive"})
log, err := bunshin.NewArchiveClusterLog(archive)
store, err := bunshin.NewArchiveClusterSnapshotStore(archive)

node, err := bunshin.StartClusterNode(ctx, bunshin.ClusterConfig{
    Log:           log,
    SnapshotStore: store,
    Service:       service,
})
```

## Quorum Commit

Appointed leaders can optionally gate service delivery on durable majority recording. Set `ClusterConfig.Quorum` with the other member logs. The leader preassigns the next log position, appends the entry to member logs, and only appends locally and invokes the service after the configured majority has acknowledged the exact same entry.

```go
leader, err := bunshin.StartClusterNode(ctx, bunshin.ClusterConfig{
    NodeID:            1,
    Mode:              bunshin.ClusterModeAppointedLeader,
    AppointedLeaderID: 1,
    Log:               leaderLog,
    Service:           service,
    Quorum: &bunshin.ClusterQuorumConfig{
        MemberLogs: []bunshin.ClusterLog{followerOneLog, followerTwoLog},
    },
})
```

`RequiredAck` defaults to a majority of the local leader plus configured member logs. It can be raised but not lowered below majority. Quorum is valid only on the appointed leader; single-node, follower, and learner modes reject it.

If the leader cannot reach quorum, append returns `ErrClusterQuorumUnavailable`, the local leader log is not advanced, and the service callback is not invoked. `ClusterSnapshot.Quorum`, `ClusterDescription.Quorum`, and validation reports expose the latest commit position, ack count, and failure string.

Existing matching entries in a member log count as acknowledgements. This makes repeated commit attempts idempotent after a member accepted the entry but the leader did not complete the previous commit. Quorum-aware failover/catch-up orchestration is still a future layer above this `ClusterLog` boundary.

## Remote Member Transport

`ListenClusterMemberTransport` exposes a running node over a Bunshin-native JSON/TCP request-response protocol. `DialClusterMember` returns a `ClusterMemberClient` that implements `ClusterLog` and `ClusterSnapshotStore`, and also supports remote ingress/egress through `Submit`.

```go
server, err := bunshin.ListenClusterMemberTransport(ctx, leader, bunshin.ClusterMemberTransportConfig{
    Addr: "10.0.0.10:7901",
})
defer server.Close()

remoteLeader, err := bunshin.DialClusterMember(ctx, bunshin.ClusterMemberClientConfig{
    Addr: "10.0.0.10:7901",
})
defer remoteLeader.Close()
```

Because the remote client implements `ClusterLog`, it can be used by existing replication, learner, and quorum paths:

```go
follower, err := bunshin.StartClusterNode(ctx, bunshin.ClusterConfig{
    NodeID:            2,
    Mode:              bunshin.ClusterModeAppointedLeader,
    AppointedLeaderID: 1,
    Log:               followerLog,
    Service:           followerService,
    Replication: &bunshin.ClusterReplicationConfig{
        SourceLog: remoteLeader,
    },
})

leader, err := bunshin.StartClusterNode(ctx, bunshin.ClusterConfig{
    NodeID:            1,
    Mode:              bunshin.ClusterModeAppointedLeader,
    AppointedLeaderID: 1,
    Log:               leaderLog,
    Service:           service,
    Quorum: &bunshin.ClusterQuorumConfig{
        MemberLogs: []bunshin.ClusterLog{remoteFollower},
    },
})
```

The protocol currently supports log append/snapshot/last-position, snapshot save/load/take, remote ingress submit, and describe. Common cluster sentinel errors are mapped back on the client side so callers can still use `errors.Is` for values such as `ErrClusterNotLeader`, `ErrClusterLogPosition`, and `ErrClusterSnapshotStoreUnavailable`.

## Heartbeats And Election

Appointed-leader nodes can optionally enable heartbeat tracking and local leader election. This is a Bunshin-native state machine, not an Aeron or Raft wire protocol.

```go
node, err := bunshin.StartClusterNode(ctx, bunshin.ClusterConfig{
    NodeID:            2,
    Mode:              bunshin.ClusterModeAppointedLeader,
    AppointedLeaderID: 1,
    Service:           service,
    Election: &bunshin.ClusterElectionConfig{
        Members:          []bunshin.ClusterNodeID{1, 2, 3},
        HeartbeatTimeout: time.Second,
    },
})
```

`RecordHeartbeat` records member liveness and accepts leader heartbeats with term metadata. `TickElection` evaluates heartbeat timeouts and deterministically selects the first live member when the current leader expires. Role changes immediately affect ingress and timer ownership: only the current local leader can append ingress, fire timers, or send service messages.

```go
status, err := node.RecordHeartbeat(ctx, bunshin.ClusterHeartbeat{
    NodeID:   1,
    LeaderID: 1,
    Term:     4,
    At:       time.Now(),
})

status, err = node.TickElection(ctx, time.Now())
```

By default Bunshin starts a local election tick loop using `ClusterElectionConfig.TickInterval`. Set `DisableAutoRun` when an application or future member transport wants to drive election ticks explicitly.

## Timers And Service Messages

Cluster timers are log-backed. Scheduling, cancellation, and firing are represented as cluster log entries so recovery, follower replication, and learner catch-up observe the same deterministic sequence.

```go
timer, err := node.ScheduleTimer(ctx, bunshin.ClusterTimer{
    Deadline: time.Now().Add(time.Second),
    Payload:  []byte("settle"),
})

err = node.CancelTimer(ctx, timer.TimerID)
```

Leaders own timer firing. `ClusterNode.FireDueTimers` can be called directly by applications that want explicit scheduling control. Unless `ClusterConfig.DisableTimerLoop` is set, Bunshin also starts a leader-only timer loop using `ClusterConfig.TimerCheckInterval`.

Timer fire entries are delivered to the service as `ClusterMessage.Type = ClusterLogEntryTimerFire` with `TimerID`, `Deadline`, and `Payload` set. Pending timers are included in `ClusterStateSnapshot`, so a timer scheduled before a snapshot and due after restart is restored without replaying the old schedule entry.

Inter-service messages use the same replicated log boundary:

```go
egress, err := node.SendServiceMessage(ctx, bunshin.ClusterServiceMessage{
    SourceService: "orders",
    TargetService: "risk",
    Payload:       []byte("rebalance"),
})
```

Service messages are delivered as `ClusterLogEntryServiceMessage` and replay like ingress. Bunshin currently runs a single service callback per node; `SourceService` and `TargetService` are metadata for applications that multiplex deterministic services behind that callback.

## Follower Replication

Appointed-leader followers can optionally replicate and catch up from a leader/source log. This keeps follower service state warm without accepting local ingress.

```go
follower, err := bunshin.StartClusterNode(ctx, bunshin.ClusterConfig{
    NodeID:            2,
    Mode:              bunshin.ClusterModeAppointedLeader,
    AppointedLeaderID: 1,
    Log:               followerLog,
    Service:           followerService,
    Replication: &bunshin.ClusterReplicationConfig{
        SourceLog:    leaderLog,
        SyncInterval: 100 * time.Millisecond,
    },
})
```

`ClusterReplicationConfig.SourceLog` is the source of committed log entries. The follower copies entries after its local position, appends them at the same positions, and invokes its service callback with `ClusterRoleFollower`. `ClusterNode.SyncReplication` can be called manually; by default Bunshin also starts a periodic sync loop. `DisableAutoRun` leaves scheduling to the application or tests.

This is a Bunshin-native replication boundary over `ClusterLog`, not a network wire protocol. A future transport can expose a remote member log behind the same source-log shape.

## Backup And Standby

`StartClusterBackup` starts a separate backup agent. It copies a source cluster log into a local backup log. In cold backup mode it only persists the log and timer metadata; in standby mode it also applies deliverable entries into a local service.

```go
backup, err := bunshin.StartClusterBackup(ctx, bunshin.ClusterBackupConfig{
    NodeID:        4,
    SourceLog:     leaderLog,
    Log:           backupLog,
    SnapshotStore: backupSnapshots,
    Service:       standbyService,
    Standby:       true,
})

result, err := backup.Sync(ctx)
```

When `SnapshotStore` is configured, the service must implement `ClusterSnapshotService`. Snapshots include service state and pending timers. `SnapshotEvery` controls how many applied entries can accumulate before a backup snapshot is taken; the zero value snapshots after each successful sync batch. `DisableAutoRun` leaves sync scheduling to the application.

Cold backup is useful when the process should only keep a durable log copy. Standby backup is useful when the process should keep deterministic service state warm. To promote a backup, start a normal `ClusterNode` with the backup log and snapshot store.

## Membership And Rolling Upgrade

Bunshin exposes membership-change and rolling-upgrade planning helpers for control planes. These helpers validate quorum, leader, and catch-up constraints and return ordered steps. `ClusterMembershipRuntime` can then apply those steps to a live membership view through application-provided hooks.

```go
plan, err := bunshin.PlanClusterMembershipChange(current, bunshin.ClusterMembershipChange{
    Add: []bunshin.ClusterMember{{
        NodeID:         4,
        Version:        "1.0.0",
        Voting:         true,
        Active:         true,
        SyncedPosition: current.LogPosition,
    }},
})
```

New voting members are planned as standby first, caught up to the current log position, then promoted into the voting set. Removing a leader requires an explicit leader transfer. Membership transitions are rejected when the resulting membership has no active quorum, no valid leader, or no voting-set overlap with the current membership.

Rolling upgrades are planned one member at a time. Followers are upgraded before the current leader unless `AllowLeaderFirst` is set. The planner rejects a member upgrade if taking that active voter down would break quorum.

```go
upgrade, err := bunshin.PlanClusterRollingUpgrade(bunshin.ClusterRollingUpgradeConfig{
    Membership:    current,
    TargetVersion: "2.0.0",
})
```

Runtime application keeps the same safety checks but lets the embedding control plane perform the environment-specific work:

```go
runtime, err := bunshin.NewClusterMembershipRuntime(bunshin.ClusterMembershipRuntimeConfig{
    Membership: current,
    Hooks: bunshin.ClusterMembershipRuntimeHooks{
        CatchUp: func(ctx context.Context, membership bunshin.ClusterMembership, member bunshin.ClusterMember) (bunshin.ClusterMember, error) {
            // Replicate from the current leader, then return the member's observed position.
            member.SyncedPosition = membership.LogPosition
            return member, nil
        },
        TransferLeader: func(ctx context.Context, membership bunshin.ClusterMembership, target bunshin.ClusterNodeID) error {
            // Drive the local election/control path for the deployment.
            return nil
        },
        UpgradeMember: func(ctx context.Context, membership bunshin.ClusterMembership, member bunshin.ClusterMember, targetVersion string) (bunshin.ClusterMember, error) {
            // Restart or replace the process, then return the observed member state.
            member.Version = targetVersion
            member.Active = true
            return member, nil
        },
        Validate: func(ctx context.Context, membership bunshin.ClusterMembership) error {
            return nil
        },
    },
})

result, err := runtime.ApplyChange(ctx, change)
upgradeResult, err := runtime.ApplyRollingUpgrade(ctx, "2.0.0", false)
```

`ApplyChange` and `ApplyRollingUpgrade` mutate the runtime membership only after every required step succeeds. If a planned promotion requires catch-up and no `CatchUp` hook is configured, the transition is rejected instead of silently marking the member current.

## Snapshots

Services that support snapshots implement `ClusterSnapshotService`:

```go
type ClusterSnapshotService interface {
    SnapshotClusterState(context.Context, bunshin.ClusterServiceContext) ([]byte, error)
    LoadClusterSnapshot(context.Context, bunshin.ClusterStateSnapshot) error
}
```

`ClusterNode.TakeSnapshot` asks the service to serialize deterministic state at the current log position and stores it in `ClusterConfig.SnapshotStore`. Bunshin also records pending cluster timers in the snapshot metadata. `NewArchiveClusterSnapshotStore` persists these snapshots as archive records and loads the latest snapshot by cluster log position.

```go
store := bunshin.NewInMemoryClusterSnapshotStore()
node, err := bunshin.StartClusterNode(ctx, bunshin.ClusterConfig{
    Log:           log,
    SnapshotStore: store,
    Service:       service,
})

snapshot, err := node.TakeSnapshot(ctx)
fmt.Println(snapshot.Position)
```

On restart, the node loads the latest snapshot into the service first, then replays only log entries with positions greater than the snapshot position.

## Learner Nodes

Learner nodes are optional. A normal cluster node does not start learner behavior unless `ClusterConfig.Learner` is set. If `Mode` is omitted while `Learner` is set, Bunshin automatically selects `ClusterModeLearner`; explicitly setting `ClusterModeLearner` still requires a learner config.

```go
learner, err := bunshin.StartClusterNode(ctx, bunshin.ClusterConfig{
    NodeID:        2,
    Mode:          bunshin.ClusterModeLearner,
    Log:           learnerLog,
    SnapshotStore: learnerSnapshots,
    Service:       snapshotService,
    Learner: &bunshin.ClusterLearnerConfig{
        MasterLog:    masterLog,
        SyncInterval: 100 * time.Millisecond,
    },
})
```

A learner has `ClusterRoleLearner`. It does not vote, does not become leader, and local ingress is rejected with `ErrClusterNotLeader`. Its job is to keep reading the configured master log, apply committed entries to its local service, and write snapshots through `SnapshotStore`.

`ClusterLearnerConfig.MasterLog` is the source log. `SnapshotStore` and a `ClusterSnapshotService` are required because learner recovery is snapshot-first. `SnapshotEvery` controls how many applied entries can accumulate before a snapshot is taken; the zero value snapshots after each successful sync batch. `DisableAutoRun` is available for tests and externally scheduled replication loops. Applications can also call `ClusterNode.SyncLearner` to force a sync cycle.

## Control

`StartClusterControlServer` provides an in-process typed control channel for local cluster operations. It is Bunshin-native and can be fronted by an external process or CLI later.

```go
control, err := bunshin.StartClusterControlServer(node, bunshin.ClusterControlConfig{})
client := control.Client()

description, err := client.Describe(ctx)
snapshot, err := client.Snapshot(ctx)
report, err := client.Validate(ctx)

err = client.Suspend(ctx)
err = client.Resume(ctx)
err = client.Shutdown(ctx)
```

`ClusterControlConfig.Authorizer` can reject control operations before they touch the node. Cluster node authorization is configured separately with `ClusterConfig.Authorizer`, so applications can keep data-plane/session policy and local control-plane policy distinct. Suspend makes local ingress return `ErrClusterSuspended`; resume re-enables ingress without changing the log. Shutdown closes the node through the same lifecycle path as `ClusterNode.Close`.

## Service Callbacks

Services implement `ClusterService`:

```go
type ClusterService interface {
    OnClusterMessage(context.Context, bunshin.ClusterMessage) ([]byte, error)
}
```

`ClusterLifecycleService` is optional for start/stop hooks. Service code should treat `ClusterMessage` as the deterministic input boundary: state changes should depend on log position, session/correlation metadata, and payload, not wall-clock time or process-local randomness.

## Scope

The current cluster layer provides the local service container, in-process and remote Bunshin-native ingress/egress protocol, authentication and authorization hooks, appointed-leader gating, quorum commit gating over `ClusterLog`, heartbeat-based local election, replayable replicated-log abstraction, remote member log/snapshot transport, archive-backed log and snapshot storage, log-backed timers and service messages, follower log catch-up, backup and standby replication, membership-change and rolling-upgrade planning plus runtime application hooks, snapshot recovery hooks, local control operations, and optional learner nodes. It does not yet provide automated backup promotion.
