# Observability

Bunshin exposes observability at two layers:

- Bunshin counters for stable application-level publication/subscription behavior.
- `quic-go` qlog traces for transport-level packet, congestion, and recovery details.

## Aeron Reference Note

Aeron exposes counters through its media driver and tooling. Bunshin's embeddable media driver exposes lifecycle counters through `MediaDriver.Snapshot`; the `Metrics` type remains a process-local counter set for stable application-level publication/subscription behavior.

QUIC-specific details are exposed through qlog rather than being normalized into Bunshin counters. This keeps Bunshin counters stable while still allowing transport-level debugging.

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

Counters currently include connections opened/accepted, messages and bytes sent/received, application-level frames sent/received/dropped, application-level retransmits, ACKs, publication back-pressure events, sequence-gap observations, missing sequence counts, send/receive errors, and protocol errors.

`FramesSent` and `FramesReceived` count Bunshin protocol frames such as HELLO, DATA, ACK, and ERROR. `FramesDropped` counts valid or attempted Bunshin frames that are suppressed locally, such as duplicates or malformed/unsupported input. QUIC packet drops and QUIC retransmissions remain transport-level details exposed through qlog; Bunshin's `Retransmits` counter is reserved for future Bunshin-managed retransmission paths.

Subscriptions also keep process-local loss reports:

```go
for _, report := range sub.LossReports() {
    fmt.Println(report.StreamID, report.SessionID, report.ObservationCount, report.MissingMessages)
}
```

These reports mirror the diagnostic intent of Aeron's LossStat (https://aeron.io/docs/aeron/aeron-tooling/#loss-stat), but they are based on Bunshin publisher sequence gaps rather than media-driver packet loss.

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
