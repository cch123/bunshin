# Observability

Bunshin exposes observability at two layers:

- Bunshin counters for stable application-level publication/subscription behavior.
- `quic-go` qlog traces for QUIC packet, congestion, and recovery details.

## Aeron Reference Note

Aeron exposes counters through its media driver and tooling. Bunshin's embeddable media driver exposes lifecycle counters through `MediaDriver.Snapshot`; the `Metrics` type remains a process-local counter set for stable application-level publication/subscription behavior.

QUIC-specific details are exposed through qlog rather than being normalized into Bunshin counters. UDP exposes Bunshin frame-level counters, retransmit counts, RTT summaries, and transport feedback callbacks.

## Metrics

Attach a shared `Metrics` value to publications and subscriptions:

```go
metrics := &bunshin.Metrics{}

sub, err := bunshin.ListenSubscription(bunshin.SubscriptionConfig{
    StreamID:  1,
    LocalAddr: "0.0.0.0:40456",
    Metrics:   metrics,
})

pub, err := bunshin.DialPublication(bunshin.PublicationConfig{
    StreamID:   1,
    RemoteAddr: "127.0.0.1:40456",
    Metrics:    metrics,
})
```

Read counters with `Snapshot`:

```go
snapshot := metrics.Snapshot()
fmt.Println(snapshot.MessagesSent, snapshot.MessagesReceived)
```

For tooling, read the same metrics as a stable counter list:

```go
for _, counter := range metrics.CounterSnapshots() {
    fmt.Println(counter.TypeID, counter.Name, counter.Label, counter.Value)
}
```

Counter type IDs are stable and new counters are appended with new IDs. RTT duration counters are exposed in nanoseconds in this list.

Counters currently include connections opened/accepted, messages and bytes sent/received, application-level frames sent/received/dropped, application-level retransmits, ACKs, publication back-pressure events, sequence-gap observations, missing sequence counts, send/receive errors, protocol errors, and UDP RTT summaries.

`FramesSent` and `FramesReceived` count Bunshin protocol frames such as HELLO, DATA, ACK, ERROR, STATUS, and NAK across QUIC and UDP. `FramesDropped` counts valid or attempted Bunshin frames that are suppressed locally, such as duplicates or malformed/unsupported input. QUIC packet drops and QUIC retransmissions remain transport-level details exposed through qlog; Bunshin's `Retransmits` counter records Bunshin-managed retransmission paths such as UDP NAK repair.

`PublicationConfig.TransportFeedback` receives UDP RTT observations after ACK and retransmit observations after NAK repair. This hook is intentionally low-level so applications can experiment with congestion-control policy without replacing the default QUIC transport.

Subscriptions also keep process-local loss reports:

```go
for _, report := range sub.LossReports() {
    fmt.Println(report.StreamID, report.SessionID, report.ObservationCount, report.MissingMessages)
}
```

These reports mirror the diagnostic intent of Aeron's LossStat (https://aeron.io/docs/aeron/aeron-tooling/#loss-stat), but they are based on Bunshin publisher sequence gaps rather than media-driver packet loss.

Subscriptions expose image snapshots for stream/session/source status:

```go
for _, image := range sub.Images() {
    fmt.Println(image.StreamID, image.SessionID, image.Source, image.CurrentPosition)
}
```

`Subscription.LagReports` returns the same source/session view focused on receiver lag. `ObservedPosition` advances when a message is being handled. For UDP, it also advances when a complete out-of-order DATA message has been written to the receiver image rebuild buffer. `CurrentPosition` advances after successful handler completion. `LagBytes` is the positive difference between those positions, and `RebuildMessages`, `RebuildFrames`, and `RebuildBytes` report how much UDP rebuild state is waiting for ordered delivery:

```go
for _, lag := range sub.LagReports() {
    fmt.Println(lag.StreamID, lag.SessionID, lag.Source, lag.LagBytes, lag.RebuildMessages)
}
```

The embeddable media driver includes the same image state in `MediaDriver.Snapshot().Images`, along with the owning client and subscription resource IDs.

`MediaDriver.Snapshot().StatusCounters` reports current active clients, channel endpoints, publication endpoints, subscription endpoints, publications, subscriptions, images, unavailable images, and lagging images. `MediaDriver.Snapshot().CounterSnapshots` combines those status counters, driver lifecycle counters, and the configured transport/application `Metrics` counters into one client-readable list. The driver directory writes the same list to `reports/counters.json` under `counter_snapshots`.

Driver counters also include conductor/sender/receiver duty-cycle counts, aggregate duty-cycle count, cumulative duty-cycle nanoseconds, max duty-cycle nanoseconds, stall count, cumulative stall nanoseconds, and max stall nanoseconds. `DriverConfig.StallThreshold` controls stall classification.

## Structured Logging

`PublicationConfig.Logger` and `SubscriptionConfig.Logger` accept a dependency-free structured logging hook:

```go
logger := bunshin.LoggerFunc(func(ctx context.Context, event bunshin.LogEvent) {
    slog.InfoContext(ctx, event.Message,
        "level", event.Level,
        "component", event.Component,
        "operation", event.Operation,
        "fields", event.Fields,
        "error", event.Err,
    )
})
```

Log events cover connection/listen lifecycle, send/delivery success, back pressure, close, handler failures, and protocol errors. Bunshin does not depend on `log/slog`; the example only shows one adapter shape.

## qlog

Enable qlog on a QUIC config:

```go
quicConfig := bunshin.QUICConfigWithQLog(nil)

pub, err := bunshin.DialPublication(bunshin.PublicationConfig{
    StreamID:   1,
    RemoteAddr: "127.0.0.1:40456",
    QUICConfig: quicConfig,
})
```

`quic-go` writes qlog files when `QLOGDIR` is set:

```sh
QLOGDIR=/tmp/bunshin-qlog go test ./...
```

The helper clones the supplied `quic.Config` before setting `Tracer`, so callers can reuse their base config safely.

## pprof And expvar

The optional example in `examples/observability_pprof_expvar` exposes Bunshin metrics through `expvar` and enables the standard library pprof handlers:

```sh
go run ./examples/observability_pprof_expvar
```

Open `http://127.0.0.1:6060/debug/vars` for counters or `http://127.0.0.1:6060/debug/pprof/` for runtime profiles.
