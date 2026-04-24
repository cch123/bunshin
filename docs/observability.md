# Observability

Bunshin exposes observability at two layers:

- Bunshin counters for stable application-level publication/subscription behavior.
- `quic-go` qlog traces for transport-level packet, congestion, and recovery details.

## Aeron Reference Note

Aeron exposes counters through its media driver and tooling. Bunshin does not have a media driver yet, so the current `Metrics` type is a process-local counter set that mirrors the same intent: keep operational counters stable and separate from transport internals.

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

Counters currently include connections opened/accepted, messages and bytes sent/received, application-level ACKs, send/receive errors, and protocol errors.

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
