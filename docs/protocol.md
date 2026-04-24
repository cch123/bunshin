# Bunshin Protocol

This document describes the current Bunshin wire format. The protocol is intentionally small and Go-native while the transport is still a prototype.

## Compatibility Policy

Bunshin does not currently target Aeron wire compatibility. The current protocol is a Bunshin-specific baseline for iterating on transport behavior in Go.

Wire compatibility may be revisited later, but doing so would require an explicit compatibility design and migration plan.

## Transport Assumptions

- Frames are sent over QUIC streams.
- Each QUIC stream currently carries exactly one Bunshin request frame and one Bunshin response frame.
- The maximum encoded frame size is 64 KiB.
- The current maximum payload size is 65,504 bytes, calculated as 64 KiB minus the 32-byte frame header.
- Fragmentation and reassembly are not implemented yet.
- Delivery reliability, retransmission, flow control, congestion control, and TLS are provided by QUIC through `quic-go`.
- Bunshin ACK frames confirm application-level handling, not packet-level delivery.
- Ordering is not guaranteed beyond the sequence number carried in each frame.
- Multicast, shared-memory IPC, NAK repair, and term buffers are not implemented yet.

## Byte Order

All multi-byte integer fields use network byte order, which is big-endian.

## Frame Header

Every frame starts with a fixed 32-byte header.

| Offset | Size | Type | Field | Description |
| --- | ---: | --- | --- | --- |
| 0 | 4 | uint32 | Magic | Constant `0x4253484e`, ASCII `BSHN`. |
| 4 | 1 | uint8 | Version | Current protocol version, `1`. |
| 5 | 1 | uint8 | Type | Frame type. |
| 6 | 2 | uint16 | Reserved | Must be encoded as `0`; currently ignored while decoding. |
| 8 | 4 | uint32 | Stream ID | Logical stream identifier. |
| 12 | 4 | uint32 | Session ID | Publisher session identifier. |
| 16 | 8 | uint64 | Sequence | Monotonic publisher sequence number for the session. |
| 24 | 4 | uint32 | Payload length | Number of payload bytes following the header. |
| 28 | 4 | uint32 | Payload checksum | IEEE CRC32 of the payload bytes. |

The payload starts at offset 32 and is copied exactly as provided by the caller.

## Frame Types

| Value | Name | Payload | Description |
| ---: | --- | --- | --- |
| 1 | DATA | Application bytes | Carries user payload from a publication to a subscription. |
| 2 | ACK | Empty | Confirms receipt of a DATA frame with the same stream, session, and sequence. |
| 3 | HELLO | Protocol version range | Negotiates the supported protocol version before data transfer. |
| 4 | ERROR | Error code and message | Reports protocol-level failures to the peer. |

Unknown frame types are decoded successfully by the low-level decoder but ignored by the current subscription/publication loops.

## Versioning

The only accepted protocol version is `1`. Decoding fails if the version byte differs.

Future incompatible changes should increment the version byte. Backward-compatible changes should use currently reserved fields only after the behavior is documented and tested.

## Negotiation

A publication sends a `HELLO` frame before its first `DATA` frame. The HELLO payload is two bytes:

| Offset | Size | Type | Field | Description |
| --- | ---: | --- | --- | --- |
| 0 | 1 | uint8 | Minimum version | Lowest protocol version supported by the sender. |
| 1 | 1 | uint8 | Maximum version | Highest protocol version supported by the sender. |

The subscription responds with `HELLO` when the version ranges overlap. If no overlap exists, it responds with `ERROR`.

The current implementation supports only version `1`, so successful negotiation requires the peer range to include `1`.

## Error Handling

The decoder rejects:

- Frames shorter than 28 bytes.
- Frames with an invalid magic value.
- Frames with an unsupported version.
- Frames whose payload length extends beyond the received datagram.
- Frames whose payload checksum does not match the received payload.

Explicit protocol errors use an `ERROR` frame. The payload starts with a 2-byte big-endian code followed by a UTF-8 message.

| Code | Name | Description |
| ---: | --- | --- |
| 1 | Unsupported version | The peer does not support the required protocol version. |
| 2 | Unsupported type | The peer sent a frame type this implementation does not handle. |
| 3 | Malformed frame | The peer sent a frame that could not be decoded or had an invalid control payload. |

Runtime transport errors are still reported through Go errors in the publication/subscription API.
