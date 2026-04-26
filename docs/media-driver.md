# Media Driver

Bunshin now has an embeddable media driver and an out-of-process driver binary. The embeddable driver runs in-process with a command/control channel, owns client/resource lifecycle, and delegates actual I/O to the publication/subscription primitives. The external driver uses the same resource model over typed local IPC.

Bunshin's driver boundary is still intentionally small: external publications can send through the driver, external subscriptions are owned by the driver process, and local `Serve` callbacks remain an embeddable-driver capability.

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

`DriverConfig.ThreadingMode` defaults to `DriverThreadingShared`, where the conductor handles commands, cleanup, and lifecycle duties on one event-driven loop. Set `DriverThreadingDedicated` to start separate sender and receiver agent loops. Dedicated loops schedule their work back through conductor-owned state and can use `SenderIdleStrategy` and `ReceiverIdleStrategy` to tune polling behavior without introducing concurrent writes to driver state.

Set `DriverConfig.Directory` when the driver should expose Aeron-style local files for tooling:

```go
driver, err := bunshin.StartMediaDriver(bunshin.DriverConfig{
    Directory: "/var/run/bunshin/default",
})
```

The directory is used by both embeddable and external drivers. It is the filesystem contract shared by tools and external clients.

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

`DriverPublication` exposes `Send`, `LocalAddr`, `ID`, and `Close`. `DriverSubscription` exposes `Serve`, `Poll`, `PollN`, `ControlledPoll`, `ControlledPollN`, `LocalAddr`, `Images`, `LossReports`, `ID`, and `Close`. Closing a `DriverClient` closes all of its managed publications and subscriptions.

In embedded mode, the returned handles wrap in-process resources. Against an external driver, `ConnectMediaDriver` returns the same client-facing handle shape, but publication and subscription ownership stays in the driver process:

```go
client, err := bunshin.ConnectMediaDriver(ctx, bunshin.DriverConnectionConfig{
    Directory:  "/var/run/bunshin/default",
    ClientName: "worker-a",
})

pub, err := client.AddPublication(ctx, bunshin.PublicationConfig{
    StreamID:   1,
    RemoteAddr: "127.0.0.1:40456",
})
err = pub.Send(ctx, []byte("hello"))
```

External publication sends, closes, client heartbeats, snapshots, and resource registration use typed IPC commands and correlated events. External subscription handles expose the driver-owned local address and can be closed through IPC; local `Serve` callbacks remain an embedded-driver capability.

## Counters And Lifecycle

`MediaDriver.Snapshot` returns process-local driver counters plus client/publication/subscription lifecycle state:

```go
snapshot, err := driver.Snapshot(ctx)
fmt.Println(snapshot.Counters.ClientsRegistered, len(snapshot.Publications))
```

Counters include processed/failed commands, registered/closed clients, registered/closed publications, registered/closed subscriptions, cleanup runs, stale client cleanup count, conductor/sender/receiver duty cycles, aggregate duty-cycle time, and stall time. `DriverConfig.StallThreshold` controls when a duty cycle is counted as a stall and defaults to 50 ms. `snapshot.StatusCounters` reports the current active client, channel endpoint, publication, subscription, and image counts. `snapshot.CounterSnapshots` exposes driver counters, status counters, and configured `Metrics` counters as stable type IDs, labels, names, scopes, and numeric values for tools.

## Driver Directory

`ResolveDriverDirectoryLayout` returns the stable local layout:

- `driver.mark`: JSON mark file with PID, status, start time, update time, and timeout configuration.
- `command.ring` and `events.ring`: typed IPC ring paths for driver commands and async events.
- `reports/counters.json`: driver counters, driver status counters, inherited transport metrics, CnC-style counter snapshots, and active resource counts.
- `reports/loss-report.json`: aggregated loss reports from driver-managed subscriptions.
- `reports/error-report.json`: driver command and lifecycle errors.
- `clients/`, `publications/`, `subscriptions/`, and `buffers/`: reserved ownership and shared-buffer directories.

When a driver directory is configured, driver-managed publications use mmap-backed term-buffer partitions under `buffers/publications/publication-<id>/`. Publication snapshots expose the mapped term-buffer file paths so tools can inspect ownership and file lifecycle without parsing transport state.

`MediaDriver.FlushReports` writes the latest counters, loss reports, and error reports atomically:

```go
report, err := driver.FlushReports(ctx)
fmt.Println(report.Layout.MarkFile, report.Counters.Counters.ClientsRegistered)
```

`MediaDriver.Directory` returns the resolved layout for tools that need to locate these files. On `Close`, the mark file is updated to `closed`.

The `bunshin-driver` CLI can inspect and control a driver directory:

```sh
bunshin-driver status -dir /tmp/bunshin-driver
bunshin-driver counters -dir /tmp/bunshin-driver
bunshin-driver errors -dir /tmp/bunshin-driver
bunshin-driver loss -dir /tmp/bunshin-driver
bunshin-driver streams -dir /tmp/bunshin-driver
bunshin-driver flush -dir /tmp/bunshin-driver
bunshin-driver terminate -dir /tmp/bunshin-driver
```

## Cleanup

Clients should call `Heartbeat` when they are long-lived but idle:

```go
err := client.Heartbeat(ctx)
```

The driver closes clients that have not heartbeat within `DriverConfig.ClientTimeout`, and it also closes their managed publications and subscriptions. `CleanupInterval` controls how often stale clients are checked.

## Driver IPC Protocol

`OpenDriverIPC` opens the command and event rings from a driver directory or explicit ring paths. Commands are JSON records with a protocol version, command type, and correlation ID. Events are written asynchronously to the event ring with the same correlation ID.

```go
ipc, err := bunshin.OpenDriverIPC(bunshin.DriverIPCConfig{
    Directory: "/var/run/bunshin/default",
    Reset:     true,
})

correlationID, err := ipc.SendCommand(ctx, bunshin.DriverIPCCommand{
    Type:       bunshin.DriverIPCCommandOpenClient,
    ClientName: "worker-a",
}, nil)

_, err = ipc.PollEvents(1, func(event bunshin.DriverIPCEvent) error {
    fmt.Println(event.CorrelationID == correlationID, event.Type, event.ClientID)
    return nil
})
```

`NewDriverIPCServer` adapts the typed IPC protocol to the embeddable `MediaDriver`. It handles client open/heartbeat/close, publication add/send/close, subscription add/close, snapshot, report flush, and terminate commands. Failed commands emit `DriverIPCEventCommandError` instead of mutating the ring protocol shape.

## Out-Of-Process Driver

`bunshin-driver` runs the media driver as a separate process over the driver directory and IPC rings:

```sh
bunshin-driver run -dir /var/run/bunshin/default
```

The process updates `driver.mark` on each heartbeat/report flush. Tooling can inspect the mark file with:

```sh
bunshin-driver status -dir /var/run/bunshin/default
```

`CheckDriverProcess` reads the same mark file and reports a driver as stale when it is still marked `active` but its heartbeat age exceeds the configured stale timeout. A normal shutdown updates the mark file to `closed`.

Driver startup refuses to reuse a directory that still has a fresh `active` mark file, returning `ErrDriverDirectoryActive`. If the mark file is `active` but stale, startup first recovers it by writing a closed mark with `closed_at`, then creates the new active mark. `DriverConfig.DirectoryStaleTimeout` and `DriverProcessConfig.StaleTimeout` control this recovery window; the default is 5 seconds.

Terminate a running process through the typed IPC command ring:

```sh
bunshin-driver terminate -dir /var/run/bunshin/default
```

The terminate command emits a correlated `terminated` event before the process exits.

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

The ring can be opened by multiple handles that map the same file, which is enough for the typed driver IPC protocol and local driver tests. Driver-managed publications use the same mmap primitive for term-buffer files when a driver directory is configured.
