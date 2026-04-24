# Bunshin Go Aeron-Inspired Roadmap

This checklist tracks the work needed to evolve Bunshin from the current UDP prototype into a fuller Go implementation inspired by Aeron's open-source architecture.

## Protocol And Compatibility

- [x] Document the current frame format, wire assumptions, byte order, and versioning policy.
- [x] Decide whether Bunshin targets Aeron wire compatibility or a Go-native Aeron-inspired protocol.
- [x] Add protocol negotiation and explicit error frames.
- [x] Add checksums or integrity validation for data frames.
- [x] Add fuzz tests for frame decoding and malformed packets.

## Transport Core

- [x] Use `quic-go` as the default reliable transport instead of custom UDP ACK/retransmit.
- [x] Add transport benchmarks for latency percentiles, throughput, and allocations.
- [x] Add packet-loss recovery benchmark harness for QUIC.
- [x] Add production TLS configuration examples.
- [x] Add qlog and metrics hooks for QUIC observability.
- [ ] Evaluate whether Bunshin's payload CRC32 should remain on top of QUIC transport integrity.
- [ ] Benchmark future Aeron-backed options against the QUIC default before adding another backend.
- [ ] Implement term buffers with append positions, term IDs, and rotation.
- [ ] Add publication back pressure instead of blocking only on ACK timeout.
- [ ] Add receiver gap detection and loss reporting.
- [ ] Add configurable MTU and fragmentation/reassembly for payloads larger than a single datagram.
- [ ] Add flow control strategies for unicast and multicast.
- [ ] Add ordered delivery guarantees per stream/session.
- [ ] Add idle strategies for low-latency polling loops.

## Media Driver

- [ ] Separate client API from a media-driver process or embeddable driver.
- [ ] Add driver-managed publications, subscriptions, counters, and lifecycle state.
- [ ] Add command/control channels between clients and driver.
- [ ] Add shared-memory or memory-mapped transport for local IPC.
- [ ] Add driver cleanup for stale clients and inactive sessions.

## API

- [ ] Stabilize public package names, constructors, and error types.
- [ ] Add non-blocking offer/poll APIs similar to high-performance messaging systems.
- [ ] Add context-aware blocking helpers on top of non-blocking primitives.
- [ ] Add typed configuration with validation and sane defaults.
- [ ] Add examples for publication, subscription, request/response, and embedded driver usage.

## Observability

- [ ] Add counters for sent frames, received frames, retransmits, drops, gaps, and back pressure.
- [ ] Add structured logging hooks without forcing a logging dependency.
- [ ] Add loss reports and subscription lag reporting.
- [ ] Add optional pprof/expvar integration examples.
- [ ] Add benchmark output for latency and throughput.

## Archive

- [ ] Design an append-only recording format for streams.
- [ ] Implement recording of live streams.
- [ ] Implement replay from a recorded position.
- [ ] Implement truncate, purge, and integrity scan operations.
- [ ] Add archive replication between nodes.

## Cluster

- [ ] Define cluster scope and whether to implement Raft-compatible semantics.
- [ ] Add replicated log abstraction.
- [ ] Add leader election and heartbeat handling.
- [ ] Add snapshot and recovery support.
- [ ] Add deterministic service execution examples.
- [ ] Add rolling upgrade and membership-change strategy.

## Performance

- [ ] Add microbenchmarks for frame encode/decode, send/receive, and handler dispatch.
- [ ] Reduce allocations in hot paths.
- [ ] Add buffer pooling where it improves throughput without complicating ownership.
- [ ] Measure latency percentiles under load.
- [ ] Add Linux-specific socket tuning documentation.
- [ ] Evaluate runtime pinning and busy-spin options for low-latency deployments.

## Testing

- [ ] Add unit tests for timeout, close, duplicate suppression, and invalid config behavior.
- [ ] Add integration tests for retransmission after packet loss.
- [ ] Add race tests to CI.
- [ ] Add soak tests for long-running publications/subscriptions.
- [ ] Add packet-loss and jitter simulation tests.
- [ ] Add cross-platform CI for macOS and Linux.

## Documentation And Licensing

- [ ] Keep README clear that Bunshin is not an official Aeron distribution.
- [ ] Add Apache-2.0 license compatibility notes for any code or design borrowed from upstream projects.
- [ ] Avoid using Aeron trademarks in product naming.
- [ ] Add architecture diagrams for client, driver, transport, archive, and cluster layers.
- [ ] Add migration notes if protocol compatibility becomes a goal.
