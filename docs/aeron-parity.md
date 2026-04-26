# Aeron Semantic Parity

This document tracks what remains if Bunshin should move beyond "Aeron-inspired" and closer to Aeron runtime semantics. It is not a compatibility promise. Bunshin remains Go-native unless a specific task introduces an adapter or compatibility layer.

## Current Boundary

Bunshin already implements a large Bunshin-native surface: publication/subscription APIs, term positions, UDP setup/status/NAK/RTT feedback, multicast and multi-destination sends, local spy subscriptions, an embeddable and external media-driver boundary, mmap-backed publication term buffers, archive catalog and replay primitives, cluster log/snapshot/quorum/learner/backup primitives, remote cluster member transport, membership runtime hooks, and JSON-based operational tooling.

The remaining gaps are mostly semantic depth, not just missing names. Aeron behavior is defined by the interaction among the media driver, log buffers, control protocol, counters, Archive, and Cluster. Bunshin often has the same conceptual component, but with a smaller local or Go-native implementation.

## Parity Matrix

| Area | Aeron reference behavior | Bunshin status | Gap if pursuing parity |
| --- | --- | --- | --- |
| Client and driver API | Clients communicate with an out-of-process media driver over IPC and consume publications/images through shared log buffers. | External clients can register resources, send through driver IPC, receive command responses on per-client response rings, poll driver-owned subscriptions through per-subscription mmap data rings, inspect data-ring capacity and occupancy in driver snapshots, and receive explicit back-pressured poll events when a data ring cannot accept more payloads. | Promote the data rings into shared-image polling, including image lifecycle and position tracking across the driver boundary. |
| Driver directory and CnC | Aeron tools read a CnC file, counters, error log, loss report, and driver mark files with stable binary layouts. | Bunshin writes JSON mark, counter, loss, error, and ring reports plus typed IPC rings. The adapter boundary for each format is documented in `docs/media-driver.md`. | Build explicit adapters only if Aeron tooling compatibility becomes a goal; otherwise keep these formats Bunshin-native. |
| Transport protocol | Aeron UDP uses data, setup, status, NAK, RTT, loss detection, retransmit, image rebuild, flow control, and congestion control inside the media-driver protocol. | QUIC is default. UDP has Bunshin DATA, HELLO setup, STATUS, NAK repair, RTT feedback, endpoint liveness snapshots, multicast, multi-destination, local spy behavior, and receiver-image rebuild buffers with high-water tracking during out-of-order rebuild. | Deepen UDP into a full media-driver data path with richer endpoint state, loss recovery, and congestion-control semantics. |
| Flow control and MDC | Aeron has unicast, min/max multicast, tagged/preferred multicast flow control, dynamic/manual multi-destination-cast, receiver liveness, and endpoint status counters. | Bunshin has unicast, max/min multicast, and preferred-receiver flow-control strategies, manual and dynamic UDP destinations, multicast support, re-resolution, and per-destination liveness snapshots. | Deepen endpoint counters and runtime policy only if future production feedback needs Aeron-shaped diagnostics. |
| Term buffers and zero copy | Aeron publications claim into shared log buffers and subscribers scan fragments/images from those buffers. | Bunshin has term positions, claim APIs, and mmap-backed publication term buffers for driver-managed publications. | Make mmap term buffers the actual cross-process data path for both publication and subscription images, not only driver-owned publication state. |
| Archive recording | Aeron Archive records selected subscription images as-is, preserving Aeron data stream format in segment files. | Bunshin can record delivered messages or raw Bunshin DATA frames after ordered delivery, using a Bunshin archive record header. | Add richer descriptor metadata for source image state and continue moving recording ownership toward driver-managed image state. |
| Archive control | Aeron Archive uses SBE control request, response, recording event, and recording signal streams with correlation IDs and control sessions. | Bunshin has an in-process typed control server plus a Bunshin-native JSON control protocol with correlation IDs, control sessions, replay events, and recording event streams. | Decide whether an Aeron SBE adapter is a goal; otherwise keep the JSON protocol as the explicit Bunshin-native boundary. |
| Archive replay and replication | Aeron replay supports recording ID, position, bounded length, open-ended replay, live replay merge, and replication that can follow a live source. | Bunshin replays from position, supports replay merge, replicates stopped recordings, and can follow an active recording through recording-event-driven live replication. | Deepen source/destination archive session management and tie replay merge more directly to live image state. |
| Cluster consensus | Aeron Cluster uses a strong leader, majority durable recording before service consumption, leader elections, catch-up, and archive-backed log/snapshot recovery. | Bunshin has local leader modes, heartbeat/election state, remote `ClusterLog` transport for replication and quorum append, quorum commit gating before service delivery, archive-backed log/snapshot storage, learners, backup, and standby. | Add quorum-aware leader failover and member catch-up orchestration over remote transport. |
| Cluster ingress and egress | Aeron Cluster defines client ingress, egress, sessions, redirects, authentication, and SBE cluster protocol messages. | Bunshin has in-process ingress/egress clients, authentication, authorization hooks, and a Bunshin-native JSON/TCP member protocol for remote ingress/egress. | Add redirect/failover semantics and decide whether an external cluster-client protocol should remain Bunshin-native or get an adapter. |
| Membership and upgrade | Aeron manages live cluster membership, elections, log catch-up, and operational cluster control. | Bunshin has membership-change and rolling-upgrade planners plus runtime application hooks for catch-up, leader transfer, upgrade, and validation. | Tie runtime hooks into a full automated cluster control plane with redirect/failover behavior. |
| Tooling | Aeron ships AeronStat, LossStat, ArchiveTool, ClusterTool, event logging, and SBE-aware diagnostics. | Bunshin ships native CLI tools over JSON driver/archive/cluster state. | Add Bunshin equivalents for the missing operational views, and separately decide whether Aeron tool compatibility is a goal. |
| Performance | Aeron is designed for low and predictable latency using tuned agents, direct buffers, SBE/Agrona, and extensive benchmark coverage. | Bunshin has Go benchmarks, QUIC/UDP/IPC parity baseline workloads, idle strategies, and profiling docs. | Run parity benchmarks regularly, add adapter rows if an Aeron-backed adapter is introduced, and use results to guide runtime pinning, allocation, GC, and socket-tuning work. |

## Recommended Sequence

1. Keep this document and `TASKS.md` aligned as the parity backlog changes.
2. Complete the external media-driver data path for subscriptions and shared images.
3. Deepen UDP transport semantics around richer endpoint state, loss recovery, and congestion control.
4. Move archive recording ownership closer to driver-managed image state and add live replication.
5. Add quorum-aware failover/catch-up over remote cluster member transport before expanding higher-level cluster controls.
6. Run the parity benchmark suite before choosing more low-level work or introducing any Aeron-backed adapter.

## Execution Checklist

Use this list for incremental Aeron-semantic alignment work. Checked items can still remain Bunshin-native; they only mean the named semantic gap has been narrowed.

- [x] Add bounded archive replay by position and length while preserving open-ended replay as the default.
- [x] Add raw stream/image recording so archive segments can replay recorded fragments instead of only delivered Bunshin messages.
- [x] Add an external archive control protocol with correlation IDs, control sessions, and recording event/signal streams.
- [x] Add live archive replication and follow-on replay merge tied to live stream state.
- [x] Add external-driver subscription polling over driver IPC message batches.
- [x] Add external-driver controlled polling over driver IPC without swallowing messages after `break`.
- [x] Expose external-driver subscription image, lag, and loss diagnostics from driver snapshots.
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
- [ ] Promote external-driver subscription polling to shared or mmap-backed image state.
- [x] Document the Aeron CnC/counter/error/loss-report adapter boundary for Bunshin-native driver reports.
- [x] Add receiver-side term/image rebuilding for UDP loss recovery.
- [x] Add Aeron-like UDP setup, status, NAK, RTT, and endpoint lifecycle semantics.
- [x] Add full multi-destination-cast control modes, receiver liveness, and tagged or preferred receiver flow control.
- [x] Add remote cluster member transport for consensus, replication, ingress, egress, and snapshots.
- [x] Add quorum commit semantics that gate service delivery on majority durable recording.
- [x] Integrate cluster log and snapshot storage with archive-style durable recordings.
- [x] Apply live cluster membership transitions instead of only producing plans.
- [x] Add Aeron baseline benchmarks for QUIC, Bunshin UDP, IPC, and future adapters.
- [x] Decide and document final non-goals for Aeron wire, API, file, and tool compatibility.

## Explicit Non-Goals Unless Reopened

These items are not part of Bunshin's core implementation:

- Aeron wire protocol compatibility for media-driver, archive, or cluster traffic.
- Aeron Java, C, C++, or .NET client API compatibility.
- Aeron SBE schema compatibility for archive or cluster control messages.
- Aeron CnC, counter, archive catalog, recording segment, or cluster file compatibility.
- Compatibility with AeronStat, LossStat, ArchiveTool, or ClusterTool.

Adapter projects may be added later, but they must be explicit boundaries over Bunshin-native state:

- A tool-output adapter may read Bunshin JSON driver reports and emit Aeron-shaped diagnostics.
- A protocol adapter may translate between Bunshin-native archive or cluster control messages and another schema, without changing Bunshin's persisted formats.
- A benchmark adapter may add Aeron-backed benchmark rows under the existing parity workload names.
- A file-format adapter may export or import selected catalog/report data, but Bunshin should not silently write Aeron-compatible binary files by default.
