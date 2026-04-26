# Benchmarks

Bunshin uses benchmarks to keep the QUIC transport baseline measurable before adding more transport backends.

## Commands

Run the throughput and allocation benchmark:

```sh
go test -run '^$' -bench BenchmarkPublicationSubscriptionSend -benchmem ./...
```

The benchmark reports Go's standard `ns/op`, allocation, and `MB/s` output plus a Bunshin-specific `msg/s` metric.

Run the latency percentile smoke test:

```sh
go test -run TestTransportLatencyPercentiles -v ./...
```

The latency test logs a JSON record prefixed with `transport benchmark output:`:

```json
{"name":"transport_latency","transport":"quic","payload_bytes":256,"samples":256,"p50_nanos":0,"p95_nanos":0,"p99_nanos":0,"p999_nanos":0,"max_nanos":0}
```

Run the packet-loss recovery benchmark:

```sh
go test -run '^$' -bench BenchmarkPublicationSubscriptionPacketLoss -benchmem ./...
```

The packet-loss benchmark reports `msg/s` and `dropped_packets` in addition to the standard benchmark metrics.

Run focused microbenchmarks:

```sh
go test -run '^$' -bench 'Benchmark(FrameEncode|FrameDecode|PublicationSubscriptionSendUDP|SubscriptionHandlerDispatch)$' -benchmem ./...
```

Run p99/p999 latency benchmarks for the transport paths:

```sh
go test -run '^$' -bench 'Benchmark(TransportLatencyQUIC|TransportLatencyUDP|IPCRingLatency)$' -benchmem ./...
```

These report `p50-ns`, `p95-ns`, `p99-ns`, `p999-ns`, and `max-ns`. The QUIC and UDP benchmarks also report `msg/s`.

Run the Aeron semantic-parity transport baseline:

```sh
go test -run '^$' -bench BenchmarkAeronParityTransportBaseline -benchmem ./...
```

This runs the same 256-byte and 1024-byte payload workloads over Bunshin QUIC, Bunshin UDP, and the mmap-backed IPC ring path. It reports Go's standard benchmark metrics plus `msg/s`. Bunshin does not currently ship an Aeron-backed adapter; if one is added later, it should be added as another sub-benchmark under the same workload names.

Run mmap IPC ring and mapped term-buffer throughput:

```sh
go test -run '^$' -bench 'Benchmark(IPCRingThroughput|MappedTermBufferAppend)$' -benchmem ./...
```

Run fanout, multi-destination, archive replay, and replay-merge benchmarks:

```sh
go test -run '^$' -bench 'Benchmark(UDPFanout|UDPMultiDestinationChurn|ArchiveReplay|ArchiveReplayMerge)$' -benchmem ./...
```

Run memory and GC profile benchmarks for publication/subscription, archive, and driver loops:

```sh
go test -run '^$' -bench 'Benchmark(PublicationSubscriptionMemoryProfile|ArchiveMemoryProfile|MediaDriverMemoryProfile)$' -benchmem ./...
```

These report `gc-cycles`, `heap-delta-bytes`, and `total-alloc-bytes` in addition to Go's standard allocation metrics.

## Current Coverage

- `BenchmarkPublicationSubscriptionSend` measures end-to-end publication/subscription throughput and allocations for 256-byte payloads over the default `quic-go` transport.
- `TestTransportLatencyPercentiles` records 256 sequential sends and logs p50, p95, p99, and max latency in JSON.
- `BenchmarkPublicationSubscriptionPacketLoss` measures the same request/ACK path with deterministic packet drops injected below QUIC.
- `TestTransportRecoversFromPacketLoss` verifies that sequential sends complete when the test packet connection drops packets.
- `BenchmarkFrameEncode` and `BenchmarkFrameDecode` measure fixed-header frame serialization cost for 256-byte DATA frames.
- `BenchmarkPublicationSubscriptionSendUDP` measures the Bunshin-native UDP send/receive path over loopback.
- `BenchmarkSubscriptionHandlerDispatch` measures the subscription-side message dispatch path without transport I/O.
- `BenchmarkTransportLatencyQUIC`, `BenchmarkTransportLatencyUDP`, and `BenchmarkIPCRingLatency` report p99/p999 latency under benchmark load.
- `BenchmarkAeronParityTransportBaseline` compares QUIC, UDP, and IPC ring throughput/allocation behavior under the same payload-size workloads.
- `BenchmarkIPCRingThroughput` measures mmap-backed IPC ring offer/poll throughput and reports `msg/s`.
- `BenchmarkMappedTermBufferAppend` measures mmap-backed term-buffer append throughput and reports `appends/s`.
- `BenchmarkUDPFanout` measures loopback UDP fanout to multiple subscribers and reports delivered message rate.
- `BenchmarkUDPMultiDestinationChurn` measures add/send/remove behavior for dynamic UDP destinations.
- `BenchmarkArchiveReplay` and `BenchmarkArchiveReplayMerge` measure archive history replay and replay-merge delivery overhead.
- `BenchmarkPublicationSubscriptionMemoryProfile`, `BenchmarkArchiveMemoryProfile`, and `BenchmarkMediaDriverMemoryProfile` report allocation and GC counters for the main messaging, archive, and embeddable driver loops.

## Gaps

- The current loss harness supports deterministic packet drops and write-side jitter. It does not yet inject duplication or reordering.
- Benchmarks currently use loopback networking, so results are useful for regression detection but not for production capacity planning.
- The current send path opens one QUIC stream per message. Future benchmarks should compare this against long-lived streams and QUIC datagrams where appropriate.

## Aeron Reference Note

Aeron handles loss recovery through its media-driver protocol, including receiver status, NAKs, and retransmit behavior. Bunshin currently delegates recovery to QUIC, so this harness injects loss below `quic-go` and measures the resulting application-level send/ACK behavior instead of implementing Aeron-style NAK repair directly.
