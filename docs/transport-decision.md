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
- Bunshin frame checksums may become optional because QUIC already provides transport integrity.
- Benchmarks should still compare QUIC with any future Aeron-backed option under the same message workload.
- The built-in self-signed TLS configuration is for development and tests. Production users should provide explicit TLS configuration.

## Next Implementation Steps

1. Add transport benchmarks for latency percentiles, throughput, allocation rate, loss recovery, and tail behavior.
2. Expose production TLS configuration examples.
3. Add qlog and metrics hooks for QUIC observability.
4. Evaluate whether Bunshin's own payload CRC32 is still useful on top of QUIC.
5. Benchmark future Aeron-backed options against the QUIC default before adding another backend.
