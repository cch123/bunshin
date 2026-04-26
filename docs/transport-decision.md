# Transport Decision

The reliable UDP layer should be delegated to an existing Go transport library. Bunshin should focus on messaging semantics, framing, stream/session metadata, observability, archive, and cluster behavior.

## Decision

Use `github.com/quic-go/quic-go` as Bunshin's default reliable transport backend. Keep a Bunshin-native UDP backend available as an explicit transport option for lower-level transport work and future Aeron-style repair experiments.

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
- `TransportUDP` performs a Bunshin HELLO setup handshake per destination, then sends Bunshin DATA frames as UDP datagrams to one or more unicast or multicast destinations and waits for receiver STATUS plus application-level ACK, NAK, or ERROR frames. It shares the publication/subscription API, term metadata, flow-control strategy hook, fragmentation, reassembly, ordered delivery, receiver image rebuild buffers, loss observation, optional NAK retry, NAK repair, destination liveness and repair diagnostics, RTT metrics, transport feedback hooks, optional AIMD congestion-window policy, and metrics path.
- UDP does not provide Aeron wire-level congestion-control compatibility or transport-level security.
- Bunshin does not perform a mandatory payload CRC32 on top of QUIC. Like Aeron, it exposes an application-defined reserved value that can carry a checksum or timestamp when needed.
- Bunshin frame fields use little-endian byte order to align with Aeron's data-header convention.
- Publications apply a bounded send window before appending to the term log. If the window is exhausted, `Send` waits for ACK capacity until its context is done and records a back-pressure event.
- Flow control is strategy-driven. The default unicast strategy uses the maximum receiver right edge; multicast-oriented max, min, and preferred-receiver strategies are available for multi-receiver transports.
- Idle strategies are available as reusable primitives for low-latency polling loops: no-op, busy-spin, yield, fixed sleep, and capped backoff.
- Subscriptions report sequence gaps per stream/session/source for diagnostics. QUIC still owns transport-level retransmission.
- Subscriptions buffer out-of-order messages and invoke handlers in sequence order per stream/session/source. Application-level ACKs are withheld until delivery.
- Publications can split a large application payload into MTU-sized DATA frames on one QUIC stream. Subscriptions reassemble those fragments before invoking the application handler.
- Publications expose `Offer` and vectored offer APIs for immediate window-capacity checks with stable status values and term positions on accepted sends.
- Publications can attach protocol-level response channels to DATA frames so request/response handlers can reply without application-encoded reply addresses.
- Local spy subscriptions observe successful outbound publications in-process by matching transport, stream ID, and endpoint. They do not ACK, affect flow control, or add publication back pressure.
- `ParseChannelURI` and `ChannelURI.String` provide a stable `bunshin:quic`, `bunshin:udp`, and `bunshin:ipc` channel representation for future IPC APIs. UDP channel URIs can carry repeated `destination=` values, `name-resolution-interval`, wildcard ports, and `spy=true`. UDP publications expose dynamic destination add/remove, destination re-resolution, and channel URI inspection APIs for unicast fanout and multicast groups.
- Packet-loss recovery is benchmarked by injecting drops below `quic-go`, because QUIC owns retransmission for the default backend.
- Benchmarks should still compare QUIC with any future Aeron-backed option under the same message workload.
- The built-in self-signed TLS configuration is for development and tests. Production users should provide explicit TLS configuration.
- The embeddable and external media driver paths own client/resource lifecycle through command/control channels while QUIC remains the default transport implementation.

## Next Implementation Steps

1. Benchmark and tune UDP loss recovery, fanout, receiver-lag, and congestion-window behavior under production-like workloads.
2. Benchmark QUIC, UDP, IPC, and future Aeron-backed options under the same message workloads.
3. Add richer driver agent-loop scheduling around the completed transport primitives.
