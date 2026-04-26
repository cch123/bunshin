# Aeron Semantic Parity

This document tracks what remains if Bunshin should move beyond "Aeron-inspired" and closer to Aeron runtime semantics. It is not a compatibility promise. Bunshin remains Go-native unless a specific task introduces an adapter or compatibility layer.

## Current Boundary

Bunshin already implements a large Bunshin-native surface: publication/subscription APIs, term positions, UDP status and NAK repair, multicast and multi-destination sends, local spy subscriptions, an embeddable and external media-driver boundary, mmap-backed publication term buffers, archive catalog and replay primitives, cluster log/snapshot/learner/backup primitives, and JSON-based operational tooling.

The remaining gaps are mostly semantic depth, not just missing names. Aeron behavior is defined by the interaction among the media driver, log buffers, control protocol, counters, Archive, and Cluster. Bunshin often has the same conceptual component, but with a smaller local or Go-native implementation.

## Parity Matrix

| Area | Aeron reference behavior | Bunshin status | Gap if pursuing parity |
| --- | --- | --- | --- |
| Client and driver API | Clients communicate with an out-of-process media driver over IPC and consume publications/images through shared log buffers. | External clients can register resources and send through driver IPC. External subscriptions are owned by the driver, but remote `Serve`/`Poll` is not supported. | Add shared-image polling for out-of-process clients, including image lifecycle, position tracking, and back-pressure semantics across the driver boundary. |
| Driver directory and CnC | Aeron tools read a CnC file, counters, error log, loss report, and driver mark files with stable binary layouts. | Bunshin writes JSON mark, counter, loss, and error reports plus typed IPC rings. | Decide whether to implement Aeron-compatible formats, build adapters, or keep Bunshin-native formats as an explicit non-goal. |
| Transport protocol | Aeron UDP uses data, setup, status, NAK, RTT, loss detection, retransmit, image rebuild, flow control, and congestion control inside the media-driver protocol. | QUIC is default. UDP has Bunshin DATA/STATUS/NAK, multicast, multi-destination, and local spy behavior. | Deepen UDP into a full media-driver data path with setup/status/NAK/RTT lifecycle, receiver-side term rebuilding, endpoint state, loss recovery, and congestion-control semantics. |
| Flow control and MDC | Aeron has unicast, min/max multicast, tagged/preferred multicast flow control, dynamic/manual multi-destination-cast, receiver liveness, and endpoint status counters. | Bunshin has unicast and multicast-oriented flow-control strategies, dynamic destinations, multicast support, and re-resolution. | Add full MDC control modes, receiver liveness semantics, tagged/preferred receiver behavior, and status counters that match the runtime decisions. |
| Term buffers and zero copy | Aeron publications claim into shared log buffers and subscribers scan fragments/images from those buffers. | Bunshin has term positions, claim APIs, and mmap-backed publication term buffers for driver-managed publications. | Make mmap term buffers the actual cross-process data path for both publication and subscription images, not only driver-owned publication state. |
| Archive recording | Aeron Archive records selected subscription images as-is, preserving Aeron data stream format in segment files. | Bunshin records delivered Bunshin messages after ordered delivery, using a Bunshin archive record header. | Add raw stream/image recording, descriptor metadata for source image state, and replay from raw fragments rather than only delivered messages. |
| Archive control | Aeron Archive uses SBE control request, response, recording event, and recording signal streams with correlation IDs and control sessions. | Bunshin has an in-process typed control server and callbacks. | Promote archive control and event streams to an external protocol with control-session semantics, or provide an explicit adapter layer. |
| Archive replay and replication | Aeron replay supports recording ID, position, bounded length, open-ended replay, live replay merge, and replication that can follow a live source. | Bunshin replays from position, supports replay merge over message metadata, and replicates stopped recordings. | Add bounded-length replay, live follow-on replication, source/destination archive sessions, and replay merge tied to live stream state. |
| Cluster consensus | Aeron Cluster uses a strong leader, majority durable recording before service consumption, leader elections, catch-up, and archive-backed log/snapshot recovery. | Bunshin has local leader modes, heartbeat/election state, local log replication from a `ClusterLog`, snapshots, learners, backup, and standby. | Add remote member transport, quorum append/commit, durable majority gating before service delivery, leader failover, member catch-up, and archive-backed log/snapshot storage. |
| Cluster ingress and egress | Aeron Cluster defines client ingress, egress, sessions, redirects, authentication, and SBE cluster protocol messages. | Bunshin has in-process ingress/egress clients, authentication, and authorization hooks. | Add networked ingress/egress sessions, redirect/failover semantics, and external protocol behavior. |
| Membership and upgrade | Aeron manages live cluster membership, elections, log catch-up, and operational cluster control. | Bunshin has membership-change and rolling-upgrade planners, but they do not mutate running nodes. | Convert planning helpers into applied runtime transitions with quorum validation and catch-up enforcement. |
| Tooling | Aeron ships AeronStat, LossStat, ArchiveTool, ClusterTool, event logging, and SBE-aware diagnostics. | Bunshin ships native CLI tools over JSON driver/archive/cluster state. | Add Bunshin equivalents for the missing operational views, and separately decide whether Aeron tool compatibility is a goal. |
| Performance | Aeron is designed for low and predictable latency using tuned agents, direct buffers, SBE/Agrona, and extensive benchmark coverage. | Bunshin has Go benchmarks, QUIC/UDP/IPC paths, idle strategies, and profiling docs. | Benchmark Bunshin QUIC, UDP, IPC, and any Aeron-backed adapter under the same workloads; use results to guide runtime pinning, allocation, GC, and socket-tuning work. |

## Recommended Sequence

1. Keep this document and `TASKS.md` aligned as the parity backlog changes.
2. Complete the external media-driver data path for subscriptions and shared images.
3. Deepen UDP transport semantics around setup/status/NAK/RTT, loss recovery, and image rebuilding.
4. Move archive recording closer to raw stream/image storage and add bounded replay plus live replication.
5. Add remote cluster member transport and quorum commit semantics before expanding higher-level cluster controls.
6. Build a benchmark suite that compares the Bunshin defaults with any Aeron-backed adapter before choosing more low-level work.

## Execution Checklist

Use this list for incremental Aeron-semantic alignment work. Checked items can still remain Bunshin-native; they only mean the named semantic gap has been narrowed.

- [x] Add bounded archive replay by position and length while preserving open-ended replay as the default.
- [ ] Add raw stream/image recording so archive segments can replay recorded fragments instead of only delivered Bunshin messages.
- [ ] Add an external archive control protocol with correlation IDs, control sessions, and recording event/signal streams.
- [ ] Add live archive replication and follow-on replay merge tied to live stream state.
- [ ] Add external-driver subscription polling over shared or mmap-backed image state.
- [ ] Add receiver-side term/image rebuilding for UDP loss recovery.
- [ ] Add Aeron-like UDP setup, status, NAK, RTT, and endpoint lifecycle semantics.
- [ ] Add full multi-destination-cast control modes, receiver liveness, and tagged or preferred receiver flow control.
- [ ] Add remote cluster member transport for consensus, replication, ingress, egress, and snapshots.
- [ ] Add quorum commit semantics that gate service delivery on majority durable recording.
- [ ] Integrate cluster log and snapshot storage with archive-style durable recordings.
- [ ] Apply live cluster membership transitions instead of only producing plans.
- [ ] Add Aeron baseline benchmarks for QUIC, Bunshin UDP, IPC, and future adapters.
- [ ] Decide and document final non-goals for Aeron wire, API, file, and tool compatibility.

## Explicit Non-Goals Unless Reopened

- Aeron wire protocol compatibility.
- Aeron Java, C, C++, or .NET client API compatibility.
- Aeron SBE schema compatibility for archive or cluster control messages.
- Aeron CnC, counter, archive catalog, or cluster file compatibility.
- Compatibility with AeronStat, LossStat, ArchiveTool, or ClusterTool without an explicit adapter project.
