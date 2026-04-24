# Benchmarks

Bunshin uses benchmarks to keep the QUIC transport baseline measurable before adding more transport backends.

## Commands

Run the throughput and allocation benchmark:

```sh
go test -run '^$' -bench BenchmarkPublicationSubscriptionSend -benchmem ./...
```

Run the latency percentile smoke test:

```sh
go test -run TestTransportLatencyPercentiles -v ./...
```

## Current Coverage

- `BenchmarkPublicationSubscriptionSend` measures end-to-end publication/subscription throughput and allocations for 256-byte payloads over the default `quic-go` transport.
- `TestTransportLatencyPercentiles` records 256 sequential sends and logs p50, p95, p99, and max latency.

## Gaps

- Loss recovery is not benchmarked yet. That requires a packet-loss harness below QUIC, such as a custom `net.PacketConn` or UDP proxy that can inject loss, delay, duplication, and jitter.
- Benchmarks currently use loopback networking, so results are useful for regression detection but not for production capacity planning.
- The current send path opens one QUIC stream per message. Future benchmarks should compare this against long-lived streams and QUIC datagrams where appropriate.
