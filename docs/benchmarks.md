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

Run the packet-loss recovery benchmark:

```sh
go test -run '^$' -bench BenchmarkPublicationSubscriptionPacketLoss -benchmem ./...
```

## Current Coverage

- `BenchmarkPublicationSubscriptionSend` measures end-to-end publication/subscription throughput and allocations for 256-byte payloads over the default `quic-go` transport.
- `TestTransportLatencyPercentiles` records 256 sequential sends and logs p50, p95, p99, and max latency.
- `BenchmarkPublicationSubscriptionPacketLoss` measures the same request/ACK path with deterministic packet drops injected below QUIC.
- `TestTransportRecoversFromPacketLoss` verifies that sequential sends complete when the test packet connection drops packets.

## Gaps

- The current loss harness supports deterministic packet drops only. It does not yet inject delay, duplication, reordering, or jitter.
- Benchmarks currently use loopback networking, so results are useful for regression detection but not for production capacity planning.
- The current send path opens one QUIC stream per message. Future benchmarks should compare this against long-lived streams and QUIC datagrams where appropriate.

## Aeron Reference Note

Aeron handles loss recovery through its media-driver protocol, including receiver status, NAKs, and retransmit behavior. Bunshin currently delegates recovery to QUIC, so this harness injects loss below `quic-go` and measures the resulting application-level send/ACK behavior instead of implementing Aeron-style NAK repair directly.
