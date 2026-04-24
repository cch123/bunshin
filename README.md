# Bunshin

Bunshin is a small Go transport prototype inspired by the public Apache-2.0 Aeron projects, not a 1:1 source-code translation and not an official Aeron distribution.

Implementation decisions should follow the Aeron-first principle documented in [MEMORY.md](MEMORY.md).

The first cut focuses on a narrow, testable slice:

- QUIC-backed publication/subscription.
- Binary frame header with stream, session, sequence, and payload fields.
- Reliable send over `quic-go` streams with application-level ACK frames.
- Subscriber-side duplicate suppression.

Protocol details are documented in [docs/protocol.md](docs/protocol.md).

Transport direction is documented in [docs/transport-decision.md](docs/transport-decision.md). The current default transport is `quic-go`; the built-in self-signed TLS configuration is intended for development and tests.

Benchmark commands are documented in [docs/benchmarks.md](docs/benchmarks.md).

Production TLS setup is documented in [docs/tls.md](docs/tls.md).

Observability hooks are documented in [docs/observability.md](docs/observability.md).

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

A full Aeron-compatible Go implementation would still need term buffers, loss reports, media-driver separation, archive, cluster, counters, tooling, and protocol compatibility work. This repository currently establishes the Go API and a QUIC-backed reliable transport baseline to extend from.
