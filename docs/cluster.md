# Cluster

Bunshin cluster support starts with a local replicated-log service container. It is Aeron-inspired in shape, but it is Bunshin-native and does not implement Aeron Cluster wire compatibility.

The first supported modes are:

- `single-node`: the node is always leader and sequences ingress directly into its log.
- `appointed-leader`: a configured node ID is leader; non-leader nodes reject local ingress.
- `learner`: the node does not participate in consensus or accept ingress; it follows a configured master log and snapshots local service state.

Leader election, network log replication, backup catch-up, and rolling upgrades are future layers above this core.

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

## Ingress And Egress

Clients use a session and correlation ID. `ClusterClient.Send` creates local ingress, the leader appends it to the cluster log, then invokes the deterministic service callback.

```go
client, err := node.NewClient(ctx)
egress, err := client.Send(ctx, []byte("increment"))
fmt.Println(egress.LogPosition, egress.CorrelationID, string(egress.Payload))
```

Applications can also submit an explicit `ClusterIngress` when they own session and correlation allocation.

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

## Snapshots

Services that support snapshots implement `ClusterSnapshotService`:

```go
type ClusterSnapshotService interface {
    SnapshotClusterState(context.Context, bunshin.ClusterServiceContext) ([]byte, error)
    LoadClusterSnapshot(context.Context, bunshin.ClusterStateSnapshot) error
}
```

`ClusterNode.TakeSnapshot` asks the service to serialize deterministic state at the current log position and stores it in `ClusterConfig.SnapshotStore`.

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

`ClusterControlConfig.Authorizer` can reject control operations before they touch the node. Suspend makes local ingress return `ErrClusterSuspended`; resume re-enables ingress without changing the log. Shutdown closes the node through the same lifecycle path as `ClusterNode.Close`.

## Service Callbacks

Services implement `ClusterService`:

```go
type ClusterService interface {
    OnClusterMessage(context.Context, bunshin.ClusterMessage) ([]byte, error)
}
```

`ClusterLifecycleService` is optional for start/stop hooks. Service code should treat `ClusterMessage` as the deterministic input boundary: state changes should depend on log position, session/correlation metadata, and payload, not wall-clock time or process-local randomness.

## Scope

The current cluster layer provides the local service container, in-process ingress/egress protocol, appointed-leader gating, replayable replicated-log abstraction, snapshot recovery hooks, local control operations, and optional learner nodes. It does not yet provide leader election, remote member communication, quorum replication, or full backup promotion.
