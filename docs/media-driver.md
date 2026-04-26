# Media Driver

Bunshin now has an embeddable media driver and an out-of-process driver binary. The embeddable driver runs in-process with a command/control channel, owns client/resource lifecycle, and delegates actual I/O to the publication/subscription primitives. The external driver uses the same resource model over typed local IPC.

Bunshin's driver boundary is still intentionally small: external publications can send through the driver, external subscriptions can be polled through driver IPC, and local `Serve` callbacks remain an embeddable-driver capability. External clients use per-client response rings by default so concurrent clients do not consume each other's correlated driver responses, and external subscription payloads are delivered through per-subscription mmap data rings instead of command response events.

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

`DriverPublication` exposes `Send`, `LocalAddr`, `ID`, and `Close`. `DriverSubscription` exposes `Serve`, `Poll`, `PollN`, `ControlledPoll`, `ControlledPollN`, `LocalAddr`, `Images`, `LossReports`, `DataRingSnapshot`, `DataRingStatus`, `PendingMessages`, `ID`, and `Close`. Closing a `DriverClient` closes all of its managed publications and subscriptions.

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

External publication sends, closes, client heartbeats, snapshots, and resource registration use typed IPC commands and correlated events. `ConnectMediaDriver` creates a client-owned response ring under the driver `clients/` directory unless an explicit `EventRingPath` is supplied. External subscription handles expose the driver-owned local address and can `Poll`, `PollN`, `ControlledPoll`, or `ControlledPollN`. The driver process pumps subscription payloads into the subscription's mmap data ring from its main duty loop, and client poll calls drain local fallback messages and the mmap data ring before issuing a poll IPC command. If that data ring cannot accept more payload records, the driver returns a back-pressured `subscription_polled` event with the current ring occupancy instead of a command failure. The driver preflights data-ring capacity before polling transport work, so a back-pressured poll does not consume a new transport message. To avoid consuming multiple transport messages after a data ring fills, command-triggered external polls write at most one new payload per poll command; external `PollN` preserves batching by issuing additional safe poll commands until its limit is reached or no more work is immediately available. External controlled poll runs the handler while the data-ring record is still pending, so `ControlledPollAbort` leaves that record available for a later poll. When a single payload is larger than the configured data ring can hold, the driver falls back to the correlated response event for that payload and marks the poll back-pressured. Background-pumped oversized payloads are held in a server-side fallback queue until the owning client polls; pump work for that subscription pauses while the fallback queue is non-empty to preserve delivery order. The client keeps fallback messages in a local pending queue if a handler error or `ControlledPollAbort` requires retry, and `PendingMessages` reports that local retry queue depth. External subscriptions can inspect their local data ring with `DataRingSnapshot`; `DataRingStatus` combines that local ring occupancy with local and server-side fallback pending counts. External subscriptions also read `Images`, `LagReports`, and `LossReports` from driver snapshots, including UDP image rebuild pending message/frame/byte counts. Local `Serve` callbacks remain an embedded-driver capability.

`DriverConnectionConfig.Timeout` bounds each external IPC request and defaults to the driver client timeout when unset. External clients start an automatic heartbeat loop by default so long-lived driver-owned publications and subscriptions are not cleaned up while the client process is healthy. `DriverConnectionConfig.HeartbeatInterval` controls that interval and defaults to one third of the driver client timeout. Set `DisableAutoHeartbeat` only when an application owns heartbeat scheduling explicitly.

`SubscriptionConfig.DriverDataRingCapacity` controls the per-subscription mmap data ring used by external-driver polling. When unset, the driver uses a 1 MiB ring. Custom capacities must be large enough to hold at least one encoded driver IPC message. `bunshin-driver run -subscription-pump-limit` controls how many external subscription messages the driver process may pump into data rings per duty loop; `-disable-subscription-pump` leaves data-ring filling entirely command-triggered for deployments that want the older request-driven behavior.

## Counters And Lifecycle

`MediaDriver.Snapshot` returns process-local driver counters plus client/publication/subscription lifecycle state:

```go
snapshot, err := driver.Snapshot(ctx)
fmt.Println(snapshot.Counters.ClientsRegistered, len(snapshot.Publications))
```

Counters include processed/failed commands, registered/closed clients, registered/closed publications, registered/closed subscriptions, cleanup runs, stale client cleanup count, conductor/sender/receiver duty cycles, aggregate duty-cycle time, and stall time. `DriverConfig.StallThreshold` controls when a duty cycle is counted as a stall and defaults to 50 ms. `snapshot.StatusCounters` reports the current active client, channel endpoint, publication, subscription, and image counts. `snapshot.CounterSnapshots` exposes driver counters, status counters, and configured `Metrics` counters as stable type IDs, labels, names, scopes, and numeric values for tools. External-driver subscription data-ring preflight failures are counted as `BackPressureEvents`. External-driver subscription snapshots include the mmap data ring path, capacity, used bytes, free bytes, read/write positions, and server-side fallback pending-message count.

## Driver Directory

`ResolveDriverDirectoryLayout` returns the stable local layout:

- `driver.mark`: JSON mark file with PID, status, start time, update time, and timeout configuration.
- `command.ring` and `events.ring`: typed IPC ring paths for driver commands and shared async events. External clients created through `ConnectMediaDriver` normally use a dedicated response ring under `clients/` for correlated command responses.
- `reports/counters.json`: driver counters, driver status counters, inherited transport metrics, CnC-style counter snapshots, and active resource counts.
- `reports/loss-report.json`: aggregated loss reports from driver-managed subscriptions.
- `reports/error-report.json`: driver command and lifecycle errors.
- `reports/rings.json`: external subscription data ring paths, capacities, occupancy, read/write positions, and server-side fallback pending-message counts.
- `clients/`, `publications/`, `subscriptions/`, and `buffers/`: reserved ownership and shared-buffer directories. External subscription data rings live under `buffers/subscriptions/subscription-<id>/data.ring`.

When a driver directory is configured, driver-managed publications use mmap-backed term-buffer partitions under `buffers/publications/publication-<id>/`. Publication snapshots expose the mapped term-buffer file paths so tools can inspect ownership and file lifecycle without parsing transport state.

`MediaDriver.FlushReports` writes the latest counters, loss reports, error reports, and ring reports atomically:

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
bunshin-driver rings -dir /tmp/bunshin-driver
bunshin-driver rings -dir /tmp/bunshin-driver -report
bunshin-driver flush -dir /tmp/bunshin-driver
bunshin-driver terminate -dir /tmp/bunshin-driver
```

`bunshin-driver rings` queries the live driver by default. Add `-report` to read the last persisted `reports/rings.json` file without requiring a live IPC round trip.

## Aeron Tooling Boundary

Bunshin's driver directory is intentionally Bunshin-native. It mirrors the operational intent of Aeron's CnC, counters, error log, and loss report, but it does not use Aeron's binary file layouts:

- CnC/mark state: `driver.mark` is JSON with process identity, status, timestamps, and timeout settings. It is not an Aeron CnC file and cannot be consumed by AeronStat.
- Counters: `reports/counters.json` contains Bunshin driver counters, status counters, inherited transport metrics, and stable `counter_snapshots` with Bunshin type IDs. These IDs are stable within Bunshin, not Aeron counter IDs.
- Error log: `reports/error-report.json` stores Bunshin command and lifecycle errors as structured JSON records. It is not Aeron's distinct error log format.
- Loss report: `reports/loss-report.json` stores Bunshin stream/session gap observations. It is diagnostic-equivalent to LossStat's purpose, but not LossStat-compatible.
- Ring diagnostics: `reports/rings.json` is specific to Bunshin external subscription data rings and fallback queues.

An Aeron tooling adapter should treat these files as source data and explicitly map fields into Aeron-shaped output. The core driver should not silently write Aeron-compatible binary files unless that compatibility mode becomes an explicit project.

## Cleanup

Clients should call `Heartbeat` when they are long-lived but idle:

```go
err := client.Heartbeat(ctx)
```

The driver closes clients that have not heartbeat within `DriverConfig.ClientTimeout`, and it also closes their managed publications and subscriptions. `CleanupInterval` controls how often stale clients are checked.

External clients created by `ConnectMediaDriver` send heartbeats automatically unless `DisableAutoHeartbeat` is set. Embedded clients can call `Heartbeat` directly when they need explicit liveness updates. The external driver reconciles IPC-owned subscription data rings after stale-client cleanup so data ring files do not remain active after the owning driver resource has been removed.

## Driver IPC Protocol

`OpenDriverIPC` opens the command and event rings from a driver directory or explicit ring paths. Commands are JSON records with a protocol version, command type, correlation ID, and response ring path. Events are written asynchronously to the command's response ring with the same correlation ID; if no response ring is supplied, the driver-wide event ring is used. Subscription registration events include a data ring path for out-of-process polling. Subscription poll events include data ring capacity, used/free bytes, read/write positions, and a `back_pressured` flag when the ring could not accept another payload record.

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
