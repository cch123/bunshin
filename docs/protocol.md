# Bunshin Protocol

This document describes the current Bunshin wire format. The protocol is intentionally small and Go-native while the transport is still a prototype.

## Compatibility Policy

Bunshin does not target Aeron wire compatibility. The current protocol is a Bunshin-specific baseline for iterating on transport behavior in Go.

Aeron concepts such as stream IDs, session IDs, term IDs, term offsets, and reserved values are used as design reference points only. Peers should treat Bunshin frames as a Go-native protocol, not as Aeron-compatible frames.

## Transport Assumptions

- Frames are sent over QUIC streams.
- Each QUIC stream carries one Bunshin request message and one Bunshin response frame. A fragmented request message is encoded as multiple DATA frames on the same stream.
- The maximum encoded frame size is 64 KiB.
- The default MTU is 64 KiB, so the default maximum DATA payload per frame is 65,488 bytes, calculated as 64 KiB minus the 48-byte frame header.
- Publications can lower `MTUBytes` to force smaller DATA frames. `MaxPayloadBytes` controls the maximum application payload, which can span multiple DATA frames.
- Delivery reliability, retransmission, flow control, congestion control, and TLS are provided by QUIC through `quic-go`.
- Bunshin ACK frames confirm application-level handling, not packet-level delivery.
- Publications apply a bounded send window before appending frames to the term log. The default window is one term buffer.
- Publication flow control is strategy-driven. The default strategy is unicast max-right-edge flow control.
- Subscriptions detect sequence gaps per stream/session/source and expose process-local loss reports.
- Subscriptions deliver messages to the application in sequence order per stream/session/source.
- Multicast, an external media-driver process, NAK repair, and receiver-side stream rebuilding are not implemented yet.

## Byte Order

All multi-byte integer fields use little-endian byte order. This follows Aeron's data-header convention rather than network byte order.

## Frame Header

Every frame starts with a fixed 48-byte header.

| Offset | Size | Type | Field | Description |
| --- | ---: | --- | --- | --- |
| 0 | 4 | bytes | Magic | Constant ASCII bytes `BSHN`. |
| 4 | 1 | uint8 | Version | Current protocol version, `4`. |
| 5 | 1 | uint8 | Type | Frame type. |
| 6 | 2 | uint16 | Flags | Frame flags. |
| 8 | 4 | uint32 | Stream ID | Logical stream identifier. |
| 12 | 4 | uint32 | Session ID | Publisher session identifier. |
| 16 | 4 | int32 | Term ID | Active term identifier for the publication log. |
| 20 | 4 | int32 | Term offset | Byte offset of the frame within the active term. |
| 24 | 8 | uint64 | Sequence | Monotonic publisher sequence number for the session. |
| 32 | 4 | uint32 | Payload length | Number of payload bytes following the header. |
| 36 | 8 | uint64 | Reserved value | Application-defined metadata, default `0`. |
| 44 | 2 | uint16 | Fragment index | Zero-based fragment index for DATA frames. |
| 46 | 2 | uint16 | Fragment count | Total number of fragments for the application message. |

The payload starts at offset 48 and is copied exactly as provided by the caller.

The reserved value follows Aeron's data-header pattern: the transport does not interpret or validate it, but applications can use it for checksums, timestamps, or other out-of-band metadata.

Reference: Aeron's checksum cookbook states that Aeron does not perform an additional UDP checksum validation and recommends `ReservedValueSupplier` when applications need checksums or signatures: https://aeron.io/docs/cookbook-content/aeron-app-checksum/

Reference: Aeron's publications documentation lists `ReservedValueSupplier` offer variants for injecting a header value such as a checksum or timestamp: https://aeron.io/docs/aeron/publications-subscriptions/

## Frame Flags

| Value | Name | Description |
| ---: | --- | --- |
| 1 | Fragment | DATA frame belongs to a fragmented application message. |

## Frame Types

| Value | Name | Payload | Description |
| ---: | --- | --- | --- |
| 1 | DATA | Application bytes | Carries user payload from a publication to a subscription. |
| 2 | ACK | Empty | Confirms receipt of a DATA frame with the same stream, session, term, and sequence metadata. |
| 3 | HELLO | Protocol version range | Negotiates the supported protocol version before data transfer. |
| 4 | ERROR | Error code and message | Reports protocol-level failures to the peer. |

Unknown frame types are decoded successfully by the low-level decoder but ignored by the current subscription/publication loops.

## Versioning

The only accepted protocol version is `4`. Decoding fails if the version byte differs.

Future incompatible changes should increment the version byte. Backward-compatible changes should use currently reserved fields only after the behavior is documented and tested.

Version `4` adds frame flags and DATA fragment index/count fields. Version `3` added term ID and term offset fields populated from the publication's active term buffer. Version `2` introduced the 36-byte little-endian header with an application-defined reserved value. Version `1` used a 32-byte big-endian header with a mandatory payload CRC32.

## Term Buffers

Each publication owns an in-memory log with three fixed-length term buffers. A term starts `clean`, becomes `active` while frames are appended, and becomes `dirty` after rotation. When the active term cannot fit the next aligned frame, the publication rotates to the next partition, increments the term ID, clears that partition, and appends at offset `0`.

Term lengths must be powers of two between 64 KiB and 1 GiB. Appended frames are aligned to 32-byte boundaries. The append position is computed as `(termID - initialTermID) * termLength + termOffset + alignedFrameLength`, matching Aeron's position model while Bunshin remains a Go-native protocol.

## Fragmentation

A publication splits an application payload into DATA frames whose encoded size is at most `MTUBytes`. All fragments for one application message use the same stream ID, session ID, sequence, reserved value, and fragment count. Each fragment has its own term ID and term offset because every encoded fragment is appended separately to the publication term log.

All fragments for a message are sent on one QUIC stream. The subscription reassembles them by fragment index, invokes the application handler once with the full payload, and sends one ACK matching the final fragment metadata. Missing, duplicate, mismatched, or incomplete fragments are protocol errors.

## Flow Control

`PublicationConfig.FlowControl` can provide a sender flow-control strategy. If unset, Bunshin uses `UnicastFlowControl`, which follows Aeron's default unicast behavior of advancing the sender limit to the maximum receiver right edge.

`MaxMulticastFlowControl` applies the same max-right-edge rule for multicast-style receiver sets. `MinMulticastFlowControl` tracks receiver right edges and uses the slowest active receiver until that receiver times out. Bunshin's current QUIC transport has one remote endpoint per publication, so the multicast strategies are available as API and testable policy hooks before multicast channels are added.

## Gap Detection

Each subscription tracks the next expected publisher sequence per stream, session, and remote source. If a later sequence arrives first, Bunshin records the missing sequence range as a loss observation and aggregates it in `LossReports`.

This is an application-level sequence report. It does not imply QUIC packet loss and does not trigger NAK repair yet. Late arrivals can fill an earlier sequence hole, but the original observation remains in the loss report for diagnostics.

## Ordered Delivery

Subscriptions gate handler invocation by stream, session, and remote source. A message with a later sequence can be read from QUIC and recorded as a gap, but it is buffered until earlier sequences for the same stream/session/source have been handled. ACK frames are sent only after the corresponding message has been delivered to the handler.

Duplicate or late sequences that have already been delivered are ACKed without invoking the handler again.

## Negotiation

A publication sends a `HELLO` frame before its first `DATA` frame. The HELLO payload is two bytes:

| Offset | Size | Type | Field | Description |
| --- | ---: | --- | --- | --- |
| 0 | 1 | uint8 | Minimum version | Lowest protocol version supported by the sender. |
| 1 | 1 | uint8 | Maximum version | Highest protocol version supported by the sender. |

The subscription responds with `HELLO` when the version ranges overlap. If no overlap exists, it responds with `ERROR`.

The current implementation supports only version `4`, so successful negotiation requires the peer range to include `4`.

## Error Handling

The decoder rejects:

- Frames shorter than 48 bytes.
- Frames with an invalid magic value.
- Frames with an unsupported version.
- Frames whose payload length extends beyond the received frame bytes.

Explicit protocol errors use an `ERROR` frame. The payload starts with a 2-byte little-endian code followed by a UTF-8 message.

| Code | Name | Description |
| ---: | --- | --- |
| 1 | Unsupported version | The peer does not support the required protocol version. |
| 2 | Unsupported type | The peer sent a frame type this implementation does not handle. |
| 3 | Malformed frame | The peer sent a frame that could not be decoded or had an invalid control payload. |

Runtime transport errors are still reported through Go errors in the publication/subscription API.
