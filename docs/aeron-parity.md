# Aeron Semantic Parity

This document tracks what remains if Bunshin should move beyond "Aeron-inspired" and closer to Aeron runtime semantics. It is not a compatibility promise. Bunshin remains Go-native unless a specific task introduces an adapter or compatibility layer.

## Current Boundary

Bunshin already implements a large Bunshin-native surface: publication/subscription APIs, term positions, UDP status and NAK repair, multicast and multi-destination sends, local spy subscriptions, an embeddable and external media-driver boundary, mmap-backed publication term buffers, archive catalog and replay primitives, cluster log/snapshot/quorum/learner/backup primitives, and JSON-based operational tooling.

The remaining gaps are mostly semantic depth, not just missing names. Aeron behavior is defined by the interaction among the media driver, log buffers, control protocol, counters, Archive, and Cluster. Bunshin often has the same conceptual component, but with a smaller local or Go-native implementation.

## Parity Matrix

| Area | Aeron reference behavior | Bunshin status | Gap if pursuing parity |
| --- | --- | --- | --- |
| Client and driver API | Clients communicate with an out-of-process media driver over IPC and consume publications/images through shared log buffers. | External clients can register resources, send through driver IPC, receive command responses on per-client response rings, poll driver-owned subscriptions through per-subscription mmap data rings, inspect data-ring capacity and occupancy in driver snapshots, and receive explicit back-pressured poll events when a data ring cannot accept more payloads. | Promote the data rings into shared-image polling, including image lifecycle and position tracking across the driver boundary. |
| Driver directory and CnC | Aeron tools read a CnC file, counters, error log, loss report, and driver mark files with stable binary layouts. | Bunshin writes JSON mark, counter, loss, and error reports plus typed IPC rings. | Decide whether to implement Aeron-compatible formats, build adapters, or keep Bunshin-native formats as an explicit non-goal. |
| Transport protocol | Aeron UDP uses data, setup, status, NAK, RTT, loss detection, retransmit, image rebuild, flow control, and congestion control inside the media-driver protocol. | QUIC is default. UDP has Bunshin DATA/STATUS/NAK, multicast, multi-destination, and local spy behavior. | Deepen UDP into a full media-driver data path with setup/status/NAK/RTT lifecycle, receiver-side term rebuilding, endpoint state, loss recovery, and congestion-control semantics. |
| Flow control and MDC | Aeron has unicast, min/max multicast, tagged/preferred multicast flow control, dynamic/manual multi-destination-cast, receiver liveness, and endpoint status counters. | Bunshin has unicast and multicast-oriented flow-control strategies, dynamic destinations, multicast support, and re-resolution. | Add full MDC control modes, receiver liveness semantics, tagged/preferred receiver behavior, and status counters that match the runtime decisions. |
| Term buffers and zero copy | Aeron publications claim into shared log buffers and subscribers scan fragments/images from those buffers. | Bunshin has term positions, claim APIs, and mmap-backed publication term buffers for driver-managed publications. | Make mmap term buffers the actual cross-process data path for both publication and subscription images, not only driver-owned publication state. |
| Archive recording | Aeron Archive records selected subscription images as-is, preserving Aeron data stream format in segment files. | Bunshin can record delivered messages or raw Bunshin DATA frames after ordered delivery, using a Bunshin archive record header. | Add richer descriptor metadata for source image state and continue moving recording ownership toward driver-managed image state. |
| Archive control | Aeron Archive uses SBE control request, response, recording event, and recording signal streams with correlation IDs and control sessions. | Bunshin has an in-process typed control server plus a Bunshin-native JSON control protocol with correlation IDs, control sessions, replay events, and recording event streams. | Decide whether an Aeron SBE adapter is a goal; otherwise keep the JSON protocol as the explicit Bunshin-native boundary. |
| Archive replay and replication | Aeron replay supports recording ID, position, bounded length, open-ended replay, live replay merge, and replication that can follow a live source. | Bunshin replays from position, supports replay merge, replicates stopped recordings, and can follow an active recording through recording-event-driven live replication. | Deepen source/destination archive session management and tie replay merge more directly to live image state. |
| Cluster consensus | Aeron Cluster uses a strong leader, majority durable recording before service consumption, leader elections, catch-up, and archive-backed log/snapshot recovery. | Bunshin has local leader modes, heartbeat/election state, local log replication from a `ClusterLog`, quorum commit gating over member logs before service delivery, archive-backed log/snapshot storage, learners, backup, and standby. | Add remote member transport, quorum-aware leader failover, and member catch-up over that transport. |
| Cluster ingress and egress | Aeron Cluster defines client ingress, egress, sessions, redirects, authentication, and SBE cluster protocol messages. | Bunshin has in-process ingress/egress clients, authentication, and authorization hooks. | Add networked ingress/egress sessions, redirect/failover semantics, and external protocol behavior. |
| Membership and upgrade | Aeron manages live cluster membership, elections, log catch-up, and operational cluster control. | Bunshin has membership-change and rolling-upgrade planners, but they do not mutate running nodes. | Convert planning helpers into applied runtime transitions with quorum validation and catch-up enforcement. |
| Tooling | Aeron ships AeronStat, LossStat, ArchiveTool, ClusterTool, event logging, and SBE-aware diagnostics. | Bunshin ships native CLI tools over JSON driver/archive/cluster state. | Add Bunshin equivalents for the missing operational views, and separately decide whether Aeron tool compatibility is a goal. |
| Performance | Aeron is designed for low and predictable latency using tuned agents, direct buffers, SBE/Agrona, and extensive benchmark coverage. | Bunshin has Go benchmarks, QUIC/UDP/IPC paths, idle strategies, and profiling docs. | Benchmark Bunshin QUIC, UDP, IPC, and any Aeron-backed adapter under the same workloads; use results to guide runtime pinning, allocation, GC, and socket-tuning work. |

## Recommended Sequence

1. Keep this document and `TASKS.md` aligned as the parity backlog changes.
2. Complete the external media-driver data path for subscriptions and shared images.
3. Deepen UDP transport semantics around setup/status/NAK/RTT, loss recovery, and image rebuilding.
4. Move archive recording ownership closer to driver-managed image state and add live replication.
5. Add remote cluster member transport and quorum-aware failover/catch-up before expanding higher-level cluster controls.
6. Build a benchmark suite that compares the Bunshin defaults with any Aeron-backed adapter before choosing more low-level work.

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
- [ ] Add receiver-side term/image rebuilding for UDP loss recovery.
- [ ] Add Aeron-like UDP setup, status, NAK, RTT, and endpoint lifecycle semantics.
- [ ] Add full multi-destination-cast control modes, receiver liveness, and tagged or preferred receiver flow control.
- [ ] Add remote cluster member transport for consensus, replication, ingress, egress, and snapshots.
- [x] Add quorum commit semantics that gate service delivery on majority durable recording.
- [x] Integrate cluster log and snapshot storage with archive-style durable recordings.
- [ ] Apply live cluster membership transitions instead of only producing plans.
- [ ] Add Aeron baseline benchmarks for QUIC, Bunshin UDP, IPC, and future adapters.
- [ ] Decide and document final non-goals for Aeron wire, API, file, and tool compatibility.

## Explicit Non-Goals Unless Reopened

- Aeron wire protocol compatibility.
- Aeron Java, C, C++, or .NET client API compatibility.
- Aeron SBE schema compatibility for archive or cluster control messages.
- Aeron CnC, counter, archive catalog, or cluster file compatibility.
- Compatibility with AeronStat, LossStat, ArchiveTool, or ClusterTool without an explicit adapter project.
