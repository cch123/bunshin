# Bunshin

Bunshin is a small Go transport prototype inspired by the public Apache-2.0 Aeron projects. It is not Apache Aeron, not a 1:1 source-code translation, not an official Aeron distribution, and not an Aeron wire/API compatible implementation.

Implementation decisions should follow the Aeron-first principle documented in [MEMORY.md](MEMORY.md).

The first cut focuses on a narrow, testable slice:

- QUIC-backed publication/subscription with an optional Bunshin-native UDP backend.
- Binary frame header with stream, session, term, sequence, and payload fields.
- Reliable send over `quic-go` streams with application-level ACK frames; UDP sends Bunshin frames directly to unicast, multi-destination, or multicast destinations and uses receiver STATUS, NAK repair, and application-level ACK/ERROR responses.
- Publication back pressure through a bounded in-flight send window.
- Publication `Offer` and vectored offer APIs returning stable accepted/back-pressured/closed status values plus stream position on success.
- Publication claim/commit API for writing one message into a claimed buffer before offering it.
- Exclusive publication facade for single-writer send, offer, vectored offer, and claim/commit loops.
- Subscription `Poll`/`PollN` APIs for pull-based receive loops with fragment budgets.
- Controlled subscription polling actions for continue, break, abort, and commit-style receive loops.
- Subscription images for source/session identity, position tracking, and availability callbacks.
- Subscription lag reports based on image observed/current position.
- Configurable unicast/max-multicast/min-multicast flow control strategies.
- Protocol-level response channels for request/response flows without embedding reply addresses in application payloads.
- Local spy subscriptions for observing outbound publications inside the same process without network loopback.
- Idle strategy primitives for low-latency polling loops.
- Receiver sequence-gap detection with process-local loss reports.
- Configurable MTU with fragmentation and reassembly for larger payloads.
- Ordered delivery per stream/session/source.
- Subscriber-side duplicate suppression.
- Typed publication/subscription configuration with validation and defaulting.
- Bunshin channel URI parser/builder for `bunshin:quic`, `bunshin:udp`, and `bunshin:ipc`, including UDP destination lists, name re-resolution, wildcard ports, and local spies.
- Metrics, qlog, transport feedback, and dependency-free structured logging hooks.
- Optional pprof/expvar observability example.
- Embeddable media driver with lifecycle counters plus memory-mapped local IPC `Offer`/`Poll` primitives.

Protocol details are documented in [docs/protocol.md](docs/protocol.md).

Architecture diagrams are documented in [docs/architecture.md](docs/architecture.md).

API stability notes are documented in [docs/api.md](docs/api.md).

Compatibility, licensing, trademark, and feature-gap notes are documented in [docs/compatibility.md](docs/compatibility.md).

Aeron semantic parity gaps are tracked in [docs/aeron-parity.md](docs/aeron-parity.md).

Transport direction is documented in [docs/transport-decision.md](docs/transport-decision.md). The current default transport is `quic-go`; `TransportUDP` is available explicitly for Bunshin-native UDP experiments. The built-in self-signed TLS configuration is intended for development and tests on the QUIC path.

Benchmark commands are documented in [docs/benchmarks.md](docs/benchmarks.md).

Performance tuning notes are documented in [docs/performance.md](docs/performance.md).

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

Bunshin uses Aeron-inspired concepts where they are useful, but all protocols and file formats in this repository are Bunshin-native unless explicitly documented otherwise. Remaining work is tracked in [TASKS.md](TASKS.md).
