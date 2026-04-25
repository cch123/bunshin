# Bunshin Go Aeron-Inspired Roadmap

This checklist tracks the work needed to evolve Bunshin from the current QUIC-backed prototype into a fuller Go implementation inspired by Aeron's open-source architecture.

## Protocol And Compatibility

- [x] Document the current frame format, wire assumptions, byte order, and versioning policy.
- [x] Decide that Bunshin uses a Go-native protocol and does not target Aeron wire compatibility.
- [x] Add protocol negotiation and explicit error frames.
- [x] Add optional reserved value support for application-level integrity metadata.
- [x] Add fuzz tests for frame decoding and malformed packets.
- [ ] Document the intentional compatibility boundary: Aeron-inspired concepts, not Aeron wire/API compatibility.
- [ ] Add protocol conformance tests for version negotiation, unknown frame handling, and error frames across future versions.
- [ ] Add migration notes for evolving the Bunshin-native protocol without breaking existing recordings or peers.

## Transport Core

- [x] Use `quic-go` as the default reliable transport instead of custom UDP ACK/retransmit.
- [x] Add transport benchmarks for latency percentiles, throughput, and allocations.
- [x] Add packet-loss recovery benchmark harness for QUIC.
- [x] Add production TLS configuration examples.
- [x] Add qlog and metrics hooks for QUIC observability.
- [x] Evaluate whether Bunshin's payload CRC32 should remain on top of QUIC transport integrity.
- [ ] Benchmark future Aeron-backed options against the QUIC default before adding another backend.
- [x] Implement term buffers with append positions, term IDs, and rotation.
- [x] Add publication back pressure instead of blocking only on ACK timeout.
- [x] Add receiver gap detection and loss reporting.
- [x] Add configurable MTU and fragmentation/reassembly for payloads larger than a single datagram.
- [x] Add flow control strategies for unicast and multicast.
- [x] Add ordered delivery guarantees per stream/session.
- [x] Add idle strategies for low-latency polling loops.
- [ ] Add a Bunshin-native UDP transport backend behind the publication/subscription API.
- [ ] Add receiver status/position feedback frames for non-QUIC transports.
- [ ] Add NAK-style repair for UDP/backend transports while keeping QUIC reliability as the default path.
- [ ] Add RTT measurement and congestion-control hooks for non-QUIC transports.
- [ ] Add multicast transport support for UDP publications and subscriptions.
- [ ] Add multi-destination send and receive support with dynamic destination add/remove.
- [ ] Add response-channel support for request/response without application-encoded reply addresses.
- [ ] Add local spy subscriptions for observing outbound publications without network loopback.
- [ ] Add name re-resolution and wildcard port management for UDP channels.

## Media Driver

- [x] Separate client API from a media-driver process or embeddable driver.
- [x] Add driver-managed publications, subscriptions, counters, and lifecycle state.
- [x] Add command/control channels between clients and driver.
- [x] Add shared-memory or memory-mapped transport for local IPC.
- [x] Add driver cleanup for stale clients and inactive sessions.
- [ ] Define external driver directory layout, mark file, counters file, and loss/error report files.
- [ ] Implement a typed driver command protocol over local IPC with correlation IDs and async driver events.
- [ ] Add an out-of-process media driver binary with heartbeat, termination, and stale-driver detection.
- [ ] Move publication/subscription resource ownership behind the external driver boundary while preserving embeddable mode.
- [ ] Back driver-managed term buffers with mmap files shared between clients and driver.
- [ ] Add driver conductor/sender/receiver agent loops with configurable threading and idle strategies.
- [ ] Add active-directory detection and stale mark-file recovery.

## API

- [ ] Stabilize public package names, constructors, and error types.
- [x] Add non-blocking offer/poll APIs similar to high-performance messaging systems.
- [x] Add context-aware blocking helpers on top of non-blocking primitives.
- [x] Add typed configuration with validation and sane defaults.
- [x] Add examples for publication, subscription, request/response, and embedded driver usage.
- [ ] Add publication-level non-blocking `Offer` returning stream position or stable status values.
- [ ] Add subscription-level pull `Poll`/`PollN` APIs with fragment limits.
- [ ] Add an `Image` abstraction with source identity, join position, current position, and lifecycle callbacks.
- [ ] Add available/unavailable image handlers for subscriptions.
- [ ] Add controlled polling actions for continue, break, abort, and commit semantics.
- [ ] Add vectored offer APIs for gathering multiple buffers into one message.
- [ ] Add zero-copy claim/commit APIs for single-message writes.
- [ ] Add an exclusive publication API for single-writer hot paths.
- [ ] Add a Bunshin channel URI parser/builder for `bunshin:quic`, `bunshin:udp`, and `bunshin:ipc`.
- [ ] Add channel-level dynamic destination APIs.

## Observability

- [x] Add counters for sent frames, received frames, retransmits, and drops.
- [x] Add structured logging hooks without forcing a logging dependency.
- [ ] Add subscription lag reporting.
- [ ] Add optional pprof/expvar integration examples.
- [ ] Add benchmark output for latency and throughput.
- [ ] Add CnC-style counters with stable type IDs, labels, and client-readable snapshots.
- [ ] Add tooling-readable distinct error log files.
- [ ] Add tooling-readable loss report files.
- [ ] Add channel endpoint, publication, subscription, and image status counters.
- [ ] Add driver duty-cycle and stall counters.
- [ ] Add CLI tools for counters, errors, loss reports, stream status, and driver control.

## Archive

- [x] Design an append-only recording format for streams.
- [x] Implement recording of live streams.
- [x] Implement replay from a recorded position.
- [x] Implement truncate, purge, and integrity scan operations.
- [x] Add a recording catalog with recording IDs, descriptors, start positions, stop positions, and source metadata.
- [x] Add archive control API/server for start, stop, list, query, replay, truncate, and purge requests.
- [x] Add recording progress events and recording signal callbacks.
- [x] Add recording extension support for appending to existing recordings.
- [x] Add segmented recording files with configurable segment length.
- [x] Add replay merge from recorded history into a live stream.
- [x] Add archive replication between nodes.
- [x] Add archive segment attach, detach, delete, and migrate operations.
- [x] Add archive describe, verify, and migrate tooling.
- [x] Add archive authentication and authorization hooks.

## Cluster

- [x] Define Bunshin cluster scope and consensus semantics.
- [x] Add cluster client ingress and egress protocol.
- [x] Add replicated log abstraction.
- [x] Add a consensus module that sequences ingress into a single replicated log.
- [ ] Add leader election and heartbeat handling.
- [ ] Add log replication and catch-up between cluster members.
- [x] Add snapshot and recovery support.
- [x] Add service container and deterministic service callback APIs.
- [x] Add optional learner nodes that follow the master log and build snapshots without joining consensus.
- [ ] Add reliable cluster timers and inter-service messaging.
- [x] Add appointed-leader and single-node development modes.
- [ ] Add backup and standby replication support.
- [x] Add deterministic service execution examples.
- [ ] Add rolling upgrade and membership-change strategy.
- [x] Add cluster control tool for describe, snapshot, suspend, resume, shutdown, and validation.
- [ ] Add cluster authentication and authorization hooks.

## Performance

- [ ] Add microbenchmarks for frame encode/decode, send/receive, and handler dispatch.
- [ ] Reduce allocations in hot paths.
- [ ] Add buffer pooling where it improves throughput without complicating ownership.
- [ ] Measure latency percentiles under load.
- [ ] Add Linux-specific socket tuning documentation.
- [ ] Evaluate runtime pinning and busy-spin options for low-latency deployments.
- [ ] Add mmap IPC and shared term-buffer throughput benchmarks.
- [ ] Add p99/p999 latency benchmarks for QUIC, IPC, and future UDP transports.
- [ ] Add fanout, multi-destination, archive replay, and replay-merge benchmarks.
- [ ] Add memory and GC profile benchmarks for publication, subscription, archive, and driver loops.

## Testing

- [x] Add unit tests for timeout, close, duplicate suppression, and invalid config behavior.
- [ ] Add integration tests for retransmission after packet loss.
- [ ] Add race tests to CI.
- [ ] Add soak tests for long-running publications/subscriptions.
- [ ] Add packet-loss and jitter simulation tests.
- [ ] Add cross-platform CI for macOS and Linux.
- [ ] Add driver restart, stale client, stale driver, and mark-file recovery tests.
- [ ] Add shared mmap buffer corruption and recovery tests.
- [ ] Add UDP transport, NAK repair, multicast, and multi-destination integration tests.
- [ ] Add response-channel and dynamic destination system tests.
- [ ] Add archive catalog, recording extension, replay merge, and replication tests.
- [ ] Add cluster network partition, leader failover, snapshot recovery, and backup tests.

## Documentation And Licensing

- [ ] Keep README clear that Bunshin is not an official Aeron distribution.
- [ ] Add Apache-2.0 license compatibility notes for any code or design borrowed from upstream projects.
- [ ] Avoid using Aeron trademarks in product naming.
- [ ] Add architecture diagrams for client, driver, transport, archive, and cluster layers.
- [ ] Add Bunshin protocol evolution and recording migration notes.
- [ ] Document Aeron feature parity gaps without implying interoperability.
