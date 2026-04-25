# Cluster

Bunshin cluster support starts with a local replicated-log service container. It is Aeron-inspired in shape, but it is Bunshin-native and does not implement Aeron Cluster wire compatibility.

The first supported modes are:

- `single-node`: the node is always leader and sequences ingress directly into its log.
- `appointed-leader`: a configured node ID is leader; non-leader nodes reject local ingress.

Leader election, network log replication, backup catch-up, snapshots, and rolling upgrades are future layers above this core.

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

`ClusterConfig.Log` can provide a durable implementation. If unset, Bunshin uses `NewInMemoryClusterLog`, which is useful for tests and local development.

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

## Service Callbacks

Services implement `ClusterService`:

```go
type ClusterService interface {
    OnClusterMessage(context.Context, bunshin.ClusterMessage) ([]byte, error)
}
```

`ClusterLifecycleService` is optional for start/stop hooks. Service code should treat `ClusterMessage` as the deterministic input boundary: state changes should depend on log position, session/correlation metadata, and payload, not wall-clock time or process-local randomness.

## Scope

The current cluster layer provides the local service container, in-process ingress/egress protocol, appointed-leader gating, and replayable replicated-log abstraction. It does not yet provide leader election, remote member communication, log catch-up, snapshots, backups, or cluster control tooling.
