# Media Driver

Bunshin now has an embeddable media driver. It runs as an in-process goroutine with a command/control channel, owns client/resource lifecycle, and delegates actual I/O to the existing QUIC publication/subscription primitives.

This is not yet an external media-driver process. The API boundary is intentionally shaped so that backend can be added without replacing the client-facing driver API, and Bunshin now includes a memory-mapped local IPC ring as the low-level building block for that path.

## Starting A Driver

```go
driver, err := bunshin.StartMediaDriver(bunshin.DriverConfig{})
if err != nil {
    return err
}
defer driver.Close()

client, err := driver.NewClient(ctx, "example")
```

`DriverConfig.Metrics` and `DriverConfig.Logger` are inherited by driver-managed publications and subscriptions when their own config does not provide values.

## Managed Resources

Clients register resources through command/control messages to the driver:

```go
sub, err := client.AddSubscription(ctx, bunshin.SubscriptionConfig{
    StreamID:  1,
    LocalAddr: "127.0.0.1:40456",
})
go sub.Serve(ctx, handler)

pub, err := client.AddPublication(ctx, bunshin.PublicationConfig{
    StreamID:   1,
    RemoteAddr: sub.LocalAddr().String(),
})
err = pub.Send(ctx, []byte("hello"))
```

`DriverPublication` exposes `Send`, `LocalAddr`, `ID`, and `Close`. `DriverSubscription` exposes `Serve`, `LocalAddr`, `LossReports`, `ID`, and `Close`. Closing a `DriverClient` closes all of its managed publications and subscriptions.

## Counters And Lifecycle

`MediaDriver.Snapshot` returns process-local driver counters plus client/publication/subscription lifecycle state:

```go
snapshot, err := driver.Snapshot(ctx)
fmt.Println(snapshot.Counters.ClientsRegistered, len(snapshot.Publications))
```

Counters include processed/failed commands, registered/closed clients, registered/closed publications, registered/closed subscriptions, cleanup runs, and stale client cleanup count.

## Cleanup

Clients should call `Heartbeat` when they are long-lived but idle:

```go
err := client.Heartbeat(ctx)
```

The driver closes clients that have not heartbeat within `DriverConfig.ClientTimeout`, and it also closes their managed publications and subscriptions. `CleanupInterval` controls how often stale clients are checked.

## Current IPC Scope

The current driver command/control path is a buffered Go channel inside the process. It gives Bunshin a driver ownership model and testable lifecycle semantics before adding an external process command protocol.

## Memory-Mapped Local IPC

`OpenIPCRing` creates or opens a fixed-capacity memory-mapped ring file. It exposes non-blocking `Offer` and `Poll` operations over length-prefixed records:

```go
ring, err := bunshin.OpenIPCRing(bunshin.IPCRingConfig{
    Path:     "/tmp/bunshin/control.ring",
    Capacity: 64 * 1024,
    Reset:    true,
})
if err != nil {
    return err
}
defer ring.Close()

if err := ring.Offer([]byte("command")); err != nil {
    return err
}

_, err = ring.Poll(func(payload []byte) error {
    fmt.Printf("%s\n", payload)
    return nil
})
```

For blocking loops, `OfferContext`, `PollContext`, and `PollNContext` retry the non-blocking operations until work succeeds, the ring closes, or the context expires. The caller can provide an `IdleStrategy`; a default backoff strategy is used when nil is passed.

The ring can be opened by multiple handles that map the same file, which is enough for local command/control transport experiments and driver tests. Cross-process synchronization and a typed external-driver command protocol are intentionally left as the next layer above this primitive.
