# Project Memory

## Aeron-First Implementation Principle

For this repository, all implementation technical details must prioritize Aeron's upstream open-source implementation as the reference.

This applies to:

- Protocol and frame semantics.
- Public API shape and naming.
- Buffering and log/term concepts.
- Flow control and back pressure.
- Counters, observability, and tooling.
- Error handling and lifecycle behavior.
- Media-driver boundaries.
- Archive and cluster semantics.
- Performance-sensitive tradeoffs.

If Go, `quic-go`, or repository constraints require deviating from Aeron, document the reason explicitly before or alongside the implementation.
