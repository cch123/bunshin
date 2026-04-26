# Bunshin Go Aeron-Inspired Roadmap

This checklist tracks the work needed to evolve Bunshin from the current transport prototype into a fuller Go implementation inspired by Aeron's open-source architecture.

## Protocol And Compatibility

- [x] Document the current frame format, wire assumptions, byte order, and versioning policy.
- [x] Decide that Bunshin uses a Go-native protocol and does not target Aeron wire compatibility.
- [x] Add protocol negotiation and explicit error frames.
- [x] Add optional reserved value support for application-level integrity metadata.
- [x] Add fuzz tests for frame decoding and malformed packets.
- [x] Document the intentional compatibility boundary: Aeron-inspired concepts, not Aeron wire/API compatibility.
- [x] Add protocol conformance tests for version negotiation, unknown frame handling, and error frames across future versions.
- [x] Add migration notes for evolving the Bunshin-native protocol without breaking existing recordings or peers.

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
- [x] Add a Bunshin-native UDP transport backend behind the publication/subscription API.
- [x] Add receiver status/position feedback frames for non-QUIC transports.
- [x] Add NAK-style repair for UDP/backend transports while keeping QUIC reliability as the default path.
- [x] Add RTT measurement and congestion-control hooks for non-QUIC transports.
- [x] Add multicast transport support for UDP publications and subscriptions.
- [x] Add multi-destination send and receive support with dynamic destination add/remove.
- [x] Add response-channel support for request/response without application-encoded reply addresses.
- [x] Add local spy subscriptions for observing outbound publications without network loopback.
- [x] Add name re-resolution and wildcard port management for UDP channels.

## Media Driver

- [x] Separate client API from a media-driver process or embeddable driver.
- [x] Add driver-managed publications, subscriptions, counters, and lifecycle state.
- [x] Add command/control channels between clients and driver.
- [x] Add shared-memory or memory-mapped transport for local IPC.
- [x] Add driver cleanup for stale clients and inactive sessions.
- [x] Define external driver directory layout, mark file, counters file, and loss/error report files.
- [x] Implement a typed driver command protocol over local IPC with correlation IDs and async driver events.
- [x] Add an out-of-process media driver binary with heartbeat, termination, and stale-driver detection.
- [x] Move publication/subscription resource ownership behind the external driver boundary while preserving embeddable mode.
- [x] Back driver-managed term buffers with mmap files shared between clients and driver.
- [x] Add driver conductor/sender/receiver agent loops with configurable threading and idle strategies.
- [x] Add active-directory detection and stale mark-file recovery.

## API

- [x] Stabilize public package names, constructors, and error types.
- [x] Add non-blocking offer/poll APIs similar to high-performance messaging systems.
- [x] Add context-aware blocking helpers on top of non-blocking primitives.
- [x] Add typed configuration with validation and sane defaults.
- [x] Add examples for publication, subscription, request/response, and embedded driver usage.
- [x] Add publication-level non-blocking `Offer` returning stream position or stable status values.
- [x] Add subscription-level pull `Poll`/`PollN` APIs with fragment limits.
- [x] Add an `Image` abstraction with source identity, join position, current position, and lifecycle callbacks.
- [x] Add available/unavailable image handlers for subscriptions.
- [x] Add controlled polling actions for continue, break, abort, and commit semantics.
- [x] Add vectored offer APIs for gathering multiple buffers into one message.
- [x] Add zero-copy claim/commit APIs for single-message writes.
- [x] Add an exclusive publication API for single-writer hot paths.
- [x] Add a Bunshin channel URI parser/builder for `bunshin:quic`, `bunshin:udp`, and `bunshin:ipc`.
- [x] Add channel-level dynamic destination APIs.

## Observability

- [x] Add counters for sent frames, received frames, retransmits, and drops.
- [x] Add structured logging hooks without forcing a logging dependency.
- [x] Add subscription lag reporting.
- [x] Add optional pprof/expvar integration examples.
- [x] Add benchmark output for latency and throughput.
- [x] Add CnC-style counters with stable type IDs, labels, and client-readable snapshots.
- [x] Add tooling-readable distinct error log files.
- [x] Add tooling-readable loss report files.
- [x] Add channel endpoint, publication, subscription, and image status counters.
- [x] Add driver duty-cycle and stall counters.
- [x] Add CLI tools for counters, errors, loss reports, stream status, and driver control.

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
- [x] Add leader election and heartbeat handling.
- [x] Add log replication and catch-up between cluster members.
- [x] Add snapshot and recovery support.
- [x] Add service container and deterministic service callback APIs.
- [x] Add optional learner nodes that follow the master log and build snapshots without joining consensus.
- [x] Add reliable cluster timers and inter-service messaging.
- [x] Add appointed-leader and single-node development modes.
- [x] Add backup and standby replication support.
- [x] Add deterministic service execution examples.
- [x] Add rolling upgrade and membership-change strategy.
- [x] Add cluster control tool for describe, snapshot, suspend, resume, shutdown, and validation.
- [x] Add cluster authentication and authorization hooks.
- [x] Add quorum commit semantics so services consume entries only after a majority has durably recorded the log.

## Performance

- [x] Add microbenchmarks for frame encode/decode, send/receive, and handler dispatch.
- [x] Reduce allocations in hot paths.
- [x] Add buffer pooling where it improves throughput without complicating ownership.
- [x] Measure latency percentiles under load.
- [x] Add Linux-specific socket tuning documentation.
- [x] Evaluate runtime pinning and busy-spin options for low-latency deployments.
- [x] Add mmap IPC and shared term-buffer throughput benchmarks.
- [x] Add p99/p999 latency benchmarks for QUIC, UDP, and IPC transports.
- [x] Add fanout, multi-destination, archive replay, and replay-merge benchmarks.
- [x] Add memory and GC profile benchmarks for publication, subscription, archive, and driver loops.

## Testing

- [x] Add unit tests for timeout, close, duplicate suppression, and invalid config behavior.
- [x] Add integration tests for retransmission after packet loss.
- [x] Add race tests to CI.
- [x] Add soak tests for long-running publications/subscriptions.
- [x] Add packet-loss and jitter simulation tests.
- [x] Add cross-platform CI for macOS and Linux.
- [x] Add driver restart, stale client, stale driver, and mark-file recovery tests.
- [x] Add shared mmap buffer corruption and recovery tests.
- [x] Add UDP transport integration tests.
- [x] Add NAK repair integration tests.
- [x] Add multi-destination integration tests.
- [x] Add multicast integration tests.
- [x] Add dynamic destination system tests.
- [x] Add response-channel system tests.
- [x] Add local spy integration tests.
- [x] Add UDP name re-resolution and wildcard port tests.
- [x] Add archive catalog, recording extension, replay merge, and replication tests.
- [x] Add cluster network partition, leader failover, snapshot recovery, and backup tests.

## Documentation And Licensing

- [x] Keep README clear that Bunshin is not an official Aeron distribution.
- [x] Add Apache-2.0 license compatibility notes for any code or design borrowed from upstream projects.
- [x] Avoid using Aeron trademarks in product naming.
- [x] Add architecture diagrams for client, driver, transport, archive, and cluster layers.
- [x] Add Bunshin protocol evolution and recording migration notes.
- [x] Document Aeron feature parity gaps without implying interoperability.

## Aeron Semantic Parity Backlog

These tasks are beyond the current Bunshin-native roadmap. They are required only if Bunshin should move closer to Aeron runtime semantics. They do not imply Aeron wire, API, file, or tool compatibility unless a task explicitly calls for an adapter.

- [ ] Keep `docs/aeron-parity.md` current as the source of truth for Aeron semantic parity gaps.
- [x] Add IPC-backed external-driver subscription polling so out-of-process clients can consume driver-owned subscriptions without embeddable callbacks.
- [x] Add IPC-backed external-driver controlled polling with break and abort semantics.
- [x] Expose external-driver subscription images, lag reports, and loss reports from driver snapshots.
- [x] Route external-driver command responses to per-client response rings so concurrent external clients cannot consume each other's events.
- [x] Move external-driver subscription payload delivery from command response events onto per-subscription mmap data rings.
- [x] Expose external-driver subscription data ring path, capacity, and occupancy in driver snapshots.
- [x] Add configurable external-driver subscription data ring capacity.
- [x] Reconcile IPC-server subscription data rings after driver-side stale-client cleanup.
- [x] Preserve common driver sentinel errors across IPC command-error responses.
- [x] Return explicit back-pressured poll events when external subscription data rings cannot accept more payloads.
- [x] Preflight external subscription data-ring capacity so back-pressured polls do not consume transport messages.
- [x] Fallback oversized external subscription payloads to correlated response events when the data ring cannot hold them.
- [x] Expose local external subscription data-ring snapshots on `DriverSubscription`.
- [x] Expose local external subscription fallback pending-message counts on `DriverSubscription`.
- [x] Pump external-driver subscriptions into mmap data rings from the driver process duty loop.
- [x] Queue background-pumped oversized external subscription payloads as ordered fallback poll messages.
- [x] Expose combined external subscription data-ring and fallback pending status on `DriverSubscription`.
- [x] Preserve external-driver `PollN` accumulation while keeping data-ring writes single-message safe.
- [x] Preserve external-driver controlled-poll abort semantics for mmap data-ring records.
- [x] Preserve oversized response-event fallback messages after handler errors or controlled-poll aborts.
- [x] Add a `bunshin-driver rings` command for external subscription data-ring diagnostics.
- [x] Persist external subscription data-ring diagnostics to driver directory reports.
- [x] Include server-side external subscription fallback pending counts in live and persisted rings diagnostics.
- [ ] Promote the external-driver subscription data path from IPC message batches to shared or mmap-backed images.
- [x] Add Aeron-style driver CnC, counter, error, and loss-report semantics, or document an explicit adapter boundary for each format that remains Bunshin-native.
- [ ] Deepen the UDP transport with richer loss recovery and congestion-control semantics comparable to Aeron's media-driver protocol. Receiver-side image rebuild buffers now report out-of-order rebuild lag before delivery.
- [x] Add full multi-destination-cast semantics, including manual and dynamic control modes, receiver liveness, and tagged or preferred receiver flow-control behavior.
- [x] Add bounded archive replay by recording ID, position, and length while keeping zero length open-ended.
- [x] Record raw stream/image frames in archive segments so replay can operate on recorded fragments rather than only delivered Bunshin messages.
- [x] Promote archive control and recording-event streams from an in-process API to an external protocol with correlation IDs and control-session semantics.
- [x] Implement archive live replication and follow-on replay merge against live stream state rather than only Bunshin message metadata.
- [x] Add remote cluster member transport for replication, consensus messages, ingress, egress, and snapshot transfer.
- [x] Add quorum commit semantics so services consume entries only after a majority has durably recorded the log.
- [x] Add quorum-aware leader failover and member catch-up tests over remote cluster member transport.
- [x] Integrate cluster log and snapshot storage with archive-style durable recordings.
- [x] Turn membership-change and rolling-upgrade planners into applied runtime membership transitions.
- [x] Add Aeron parity benchmarks comparing QUIC, Bunshin UDP/IPC, and any Aeron-backed adapter under the same workloads.
- [x] Decide which Aeron wire, API, and tool compatibility items remain explicit non-goals versus adapter projects.
