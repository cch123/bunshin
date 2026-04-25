# Bunshin

Bunshin is a small Go transport prototype inspired by the public Apache-2.0 Aeron projects, not a 1:1 source-code translation and not an official Aeron distribution.

Implementation decisions should follow the Aeron-first principle documented in [MEMORY.md](MEMORY.md).

The first cut focuses on a narrow, testable slice:

- QUIC-backed publication/subscription with an optional Bunshin-native UDP backend.
- Binary frame header with stream, session, term, sequence, and payload fields.
- Reliable send over `quic-go` streams with application-level ACK frames; UDP sends Bunshin frames directly as datagrams and uses receiver STATUS, NAK repair, and application-level ACK/ERROR responses.
- Publication back pressure through a bounded in-flight send window.
- Configurable unicast/max-multicast/min-multicast flow control strategies.
- Idle strategy primitives for low-latency polling loops.
- Receiver sequence-gap detection with process-local loss reports.
- Configurable MTU with fragmentation and reassembly for larger payloads.
- Ordered delivery per stream/session/source.
- Subscriber-side duplicate suppression.
- Typed publication/subscription configuration with validation and defaulting.
- Bunshin channel URI parser/builder for `bunshin:quic`, `bunshin:udp`, and `bunshin:ipc`.
- Metrics, qlog, transport feedback, and dependency-free structured logging hooks.
- Embeddable media driver with lifecycle counters plus memory-mapped local IPC `Offer`/`Poll` primitives.

Protocol details are documented in [docs/protocol.md](docs/protocol.md).

Transport direction is documented in [docs/transport-decision.md](docs/transport-decision.md). The current default transport is `quic-go`; `TransportUDP` is available explicitly for Bunshin-native UDP experiments. The built-in self-signed TLS configuration is intended for development and tests on the QUIC path.

Benchmark commands are documented in [docs/benchmarks.md](docs/benchmarks.md).

Production TLS setup is documented in [docs/tls.md](docs/tls.md).

Observability hooks are documented in [docs/observability.md](docs/observability.md).

Archive recording and replay are documented in [docs/archive.md](docs/archive.md).

The embeddable media driver is documented in [docs/media-driver.md](docs/media-driver.md).

Runnable examples are available under [examples](examples).

## Example

```go
sub, _ := bunshin.ListenSubscription(bunshin.SubscriptionConfig{
    StreamID:  1,
    LocalAddr: "127.0.0.1:40456",
})
go sub.Serve(context.Background(), func(ctx context.Context, msg bunshin.Message) error {
    fmt.Printf("%s\n", msg.Payload)
    return nil
})

pub, _ := bunshin.DialPublication(bunshin.PublicationConfig{
    StreamID:   1,
    RemoteAddr: "127.0.0.1:40456",
})
_ = pub.Send(context.Background(), []byte("hello"))
```

## Scope

A full Aeron-inspired Go implementation still needs multicast, deeper driver agent loops, tooling, and protocol evolution work. This repository currently establishes the Go API, QUIC default transport, explicit UDP transport, media-driver boundary, archive, cluster, and local IPC primitives to extend from.
