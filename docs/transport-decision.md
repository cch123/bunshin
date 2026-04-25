# Transport Decision

The reliable UDP layer should be delegated to an existing Go transport library. Bunshin should focus on messaging semantics, framing, stream/session metadata, observability, archive, and cluster behavior.

## Decision

Use `github.com/quic-go/quic-go` as Bunshin's default reliable transport backend.

Keep the transport boundary narrow enough that lower-level Aeron-backed options can still be evaluated later for specialized low-latency deployments.

## Rationale

Maintaining a custom reliability protocol would require owning retransmission, congestion behavior, flow control, path MTU behavior, packet loss recovery, keepalive, security, tuning, and production edge cases. That is not the right initial scope for Bunshin.

`quic-go` gives Bunshin a maintained production-grade transport stack instead of a custom reliability protocol. It implements QUIC RFCs and provides reliable streams, flow control, congestion control, datagrams, path MTU discovery, connection migration, qlog, and Prometheus integration.

The tradeoff is that QUIC is a larger protocol stack than a purpose-built Aeron-like media driver. Bunshin should accept that tradeoff for the default implementation, while keeping enough abstraction to benchmark and swap specialized transports later.

For an Aeron-like goal, the evaluation must also consider:

- A Go API over the official Aeron C/C++ media driver via cgo or process boundary.
- A purpose-built Go transport that reuses Aeron's architecture carefully, while avoiding premature custom protocol work.
- A hybrid approach where production-grade reliability is delegated only after latency, allocation, and recovery behavior are measured.

## Consequences

- The hand-written UDP ACK/retransmit implementation has been replaced by QUIC streams.
- Reliability, retransmission, flow control, congestion control, and TLS are owned by `quic-go`.
- Bunshin's ACK frame now confirms application-level handling over a reliable QUIC stream rather than packet-level delivery.
- Bunshin does not perform a mandatory payload CRC32 on top of QUIC. Like Aeron, it exposes an application-defined reserved value that can carry a checksum or timestamp when needed.
- Bunshin frame fields use little-endian byte order to align with Aeron's data-header convention.
- Publications apply a bounded send window before appending to the term log. If the window is exhausted, `Send` waits for ACK capacity until its context is done and records a back-pressure event.
- Subscriptions report sequence gaps per stream/session/source for diagnostics. QUIC still owns transport-level retransmission.
- Publications can split a large application payload into MTU-sized DATA frames on one QUIC stream. Subscriptions reassemble those fragments before invoking the application handler.
- Packet-loss recovery is benchmarked by injecting drops below `quic-go`, because QUIC owns retransmission for the default backend.
- Benchmarks should still compare QUIC with any future Aeron-backed option under the same message workload.
- The built-in self-signed TLS configuration is for development and tests. Production users should provide explicit TLS configuration.

## Next Implementation Steps

1. Benchmark future Aeron-backed options against the QUIC default before adding another backend.
2. Add flow control strategies for unicast and multicast.
3. Add ordered delivery guarantees per stream/session.
