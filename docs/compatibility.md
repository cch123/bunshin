# Compatibility And Licensing Notes

This document records the project boundary for Bunshin's Aeron-inspired design. It is engineering guidance, not legal advice.

## Project Identity

Bunshin is not Apache Aeron, not an official Aeron distribution, and not an Aeron-compatible wire or API implementation. The product name is Bunshin. References to Aeron in this repository are descriptive references to design inspiration, public documentation, or feature comparison.

Do not use Aeron marks in Bunshin package names, binary names, module paths, release names, or product branding. Prefer phrases such as "Aeron-inspired" or "Bunshin-native" when describing implementation choices.

## Apache-2.0 Notes

The Aeron open-source repository is published with an Apache-2.0 license. The Apache Software Foundation's official Apache License 2.0 page identifies the SPDX short identifier as `Apache-2.0` and describes the license as including copyright and patent grants. ASF guidance also calls out the normal distribution shape of a top-level `LICENSE` file and, when applicable, a `NOTICE` file.

Bunshin should keep design borrowing and code borrowing separate:

- Design concepts may be described in Bunshin docs with attribution links.
- Code copied from an Apache-2.0 project must preserve required notices and license headers.
- Files that are original Bunshin code should not imply they are Apache Aeron source files.
- Any future copied or adapted source should be reviewed for `LICENSE` and `NOTICE` obligations before commit.
- Dependency licenses should be reviewed before release artifacts are published.

References:

- Apache License 2.0: https://www.apache.org/licenses/LICENSE-2.0
- Apache guidance for applying Apache License 2.0: https://www.apache.org/legal/apply-license
- Aeron repository license listing: https://github.com/aeron-io/aeron

## Feature Parity Gaps

These gaps describe Bunshin implementation scope. They do not imply interoperability with Aeron.

Implemented Bunshin-native areas:

- Publication/subscription API with QUIC and explicit UDP transports.
- Term-buffer position model and back pressure.
- UDP HELLO setup, status, NAK repair, RTT feedback, sender endpoint diagnostics, receiver peer diagnostics, multicast, multi-destination, and local spy support.
- Embeddable and out-of-process media driver boundary with IPC command rings, per-client response rings, mmap-backed publication term buffers, per-subscription mmap shared images, and configurable driver agent loops.
- Archive recording, replay, catalog, segment operations, replay merge, and replication.
- Cluster replicated-log, snapshot, quorum, remote member transport, membership runtime hooks, backup, learner, and control primitives.
- Driver counters, loss reports, error reports, and CLI inspection.
- Aeron-parity benchmark baselines for Bunshin QUIC, UDP, and IPC ring workloads.

Known gaps versus a mature Aeron-style stack:

- Bunshin does not implement Aeron wire protocol, Aeron CnC files, Aeron Archive protocol, or Aeron Cluster protocol.
- Bunshin does not expose Aeron client APIs or guarantee behavior parity with Aeron tools.
- External driver subscriptions are polled by out-of-process clients through per-subscription mmap shared images. The legacy data-ring status fields remain as compatibility aliases for the same mapped image state.
- External driver publications still send payloads through IPC commands. Promoting that path to client-writable mmap log buffers remains a future Aeron-semantic parity task.
- Bunshin's mmap term buffers and subscription shared images do not yet provide the full Aeron `LogBuffers`/`Image` lifecycle, fragment/block/raw polling, unblock, and position-counter semantics.
- QUIC is the default reliable transport. The UDP backend has Bunshin-native setup, status, NAK repair with optional retry, RTT feedback, sender endpoint diagnostics, receiver peer diagnostics, receiver-image rebuild tracking, and optional AIMD congestion-window policy, but it is not a full Aeron congestion-control implementation.
- Bunshin Archive records delivered Bunshin messages and metadata. It does not yet record raw Aeron-style image fragments or expose SBE control and recording-event streams.
- Bunshin Cluster uses Bunshin-native remote member transport and quorum gating. It does not yet provide Aeron Cluster protocol compatibility, automated backup promotion, or complete external client redirect/failover behavior.
- Tooling reads Bunshin JSON reports and native catalogs, not Aeron CnC, catalog, SBE, AeronStat, LossStat, ArchiveTool, or ClusterTool formats. Adapter projects must explicitly map Bunshin JSON files into Aeron-shaped output if that is desired.
- Performance tuning, runtime pinning, socket tuning, and capacity planning still require environment-specific benchmark runs.

The deeper gap list is tracked in `docs/aeron-parity.md` and the "Aeron Semantic Parity Backlog" section of `TASKS.md`.

## Migration Boundary

Bunshin-native protocol evolution is documented in `docs/protocol.md`. Recordings should be migrated explicitly through archive tooling rather than silently rewritten during replay. Compatibility decisions should preserve old recording metadata and keep live peer negotiation separate from persisted recording migration.

## Adapter Boundary

Bunshin core remains Bunshin-native. Aeron compatibility work must be introduced as explicit adapter projects rather than quiet changes to default wire, file, or API behavior:

- Wire/API adapters can translate at process boundaries, but core publication/subscription, archive, and cluster APIs remain Go-native.
- Tool adapters can read Bunshin JSON reports and native catalogs, but Bunshin does not write Aeron CnC, counter, error-log, loss-report, archive, or cluster files by default.
- Benchmark adapters can add Aeron-backed rows to the existing parity benchmark workloads.
- File import/export adapters should preserve source metadata and should never imply that Bunshin recordings are Aeron segment files.
