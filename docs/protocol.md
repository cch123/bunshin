# Bunshin Protocol

This document describes the current Bunshin wire format. The protocol is intentionally small and Go-native while transports evolve behind the publication/subscription API.

## Compatibility Policy

Bunshin does not target Aeron wire compatibility. The current protocol is a Bunshin-specific baseline for iterating on transport behavior in Go.

Aeron concepts such as stream IDs, session IDs, term IDs, term offsets, and reserved values are used as design reference points only. Peers should treat Bunshin frames as a Go-native protocol, not as Aeron-compatible frames.

The compatibility boundary is intentional:

- Bunshin may use Aeron-inspired vocabulary when that makes messaging behavior easier to reason about.
- Bunshin frames, driver commands, archive catalogs, cluster messages, and Go APIs are not Aeron wire/API contracts.
- Bunshin peers should negotiate Bunshin protocol versions only with other Bunshin peers.
- Tools should read Bunshin driver-directory JSON and IPC files as Bunshin formats, even when their purpose resembles Aeron CnC, counters, loss, or error tooling.
- Future Aeron-backed experiments must live behind explicit transport or tooling adapters instead of changing the default Bunshin-native protocol into an Aeron-compatible one.

## Transport Assumptions

- QUIC is the default transport. Frames are sent over QUIC streams.
- Each QUIC stream carries one Bunshin request message and one Bunshin response frame. A fragmented request message is encoded as multiple DATA frames on the same stream.
- UDP is available as an explicit `TransportUDP` backend. Each UDP datagram carries one Bunshin frame. Fragmented messages are sent as multiple DATA datagrams with the same stream, session, sequence, reserved value, and fragment count.
- The maximum encoded frame size is 64 KiB.
- The default QUIC MTU is 64 KiB, so the default maximum DATA payload per frame is 65,488 bytes, calculated as 64 KiB minus the 48-byte frame header.
- The default UDP MTU is 1,400 bytes to avoid common Ethernet fragmentation. Applications can override `MTUBytes`.
- Publications can lower `MTUBytes` to force smaller DATA frames. `MaxPayloadBytes` controls the maximum application payload, which can span multiple DATA frames.
- Delivery reliability, retransmission, flow control, congestion control, and TLS are provided by QUIC through `quic-go`.
- UDP transport performs per-destination HELLO setup, sends DATA frames to one or more unicast or multicast destinations, and waits for receiver STATUS plus application-level ACK or ERROR frames. It does not yet implement full congestion control or transport-level security.
- Bunshin ACK frames confirm application-level handling, not packet-level delivery.
- Publications apply a bounded send window before appending frames to the term log. The default window is one term buffer.
- Publication flow control is strategy-driven. The default strategy is unicast max-right-edge flow control.
- Subscriptions detect sequence gaps per stream/session/source and expose process-local loss reports.
- Subscriptions deliver messages to the application in sequence order per stream/session/source.
- Full congestion control is not implemented yet.

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

The payload starts at offset 48. DATA frames without response-channel metadata carry application bytes directly. DATA frames with the response-channel flag carry a small protocol envelope before the application bytes.

For STATUS frames, the Term ID and Term offset fields identify the receiver position reached after consuming the corresponding DATA frame. For DATA frames, they identify the frame's append position in the publication term log.

The reserved value follows Aeron's data-header pattern: the transport does not interpret or validate it, but applications can use it for checksums, timestamps, or other out-of-band metadata.

Reference: Aeron's checksum cookbook states that Aeron does not perform an additional UDP checksum validation and recommends `ReservedValueSupplier` when applications need checksums or signatures: https://aeron.io/docs/cookbook-content/aeron-app-checksum/

Reference: Aeron's publications documentation lists `ReservedValueSupplier` offer variants for injecting a header value such as a checksum or timestamp: https://aeron.io/docs/aeron/publications-subscriptions/

## Frame Flags

| Value | Name | Description |
| ---: | --- | --- |
| 1 | Fragment | DATA frame belongs to a fragmented application message. |
| 2 | Response channel | DATA frame payload starts with response-channel metadata. This flag is valid only on unfragmented DATA frames or the first fragment. |

## Frame Types

| Value | Name | Payload | Description |
| ---: | --- | --- | --- |
| 1 | DATA | Application bytes | Carries user payload from a publication to a subscription. |
| 2 | ACK | Empty | Confirms receipt of a DATA frame with the same stream, session, term, and sequence metadata. |
| 3 | HELLO | Protocol version range | Negotiates the supported protocol version before data transfer. |
| 4 | ERROR | Error code and message | Reports protocol-level failures to the peer. |
| 5 | STATUS | Receiver window length | Reports the receiver position and available receive window for UDP flow control. |
| 6 | NAK | Missing sequence range | Requests retransmission of missing UDP DATA messages by publisher sequence. |

Unknown frame types are decoded successfully by the low-level decoder. Subscription and publication loops reject unsupported types with protocol errors.

## Versioning

The only accepted protocol version is `4`. Decoding fails if the version byte differs.

Future incompatible changes should increment the version byte. Backward-compatible changes should use currently reserved fields only after the behavior is documented and tested.

Version `4` adds frame flags and DATA fragment index/count fields. Version `3` added term ID and term offset fields populated from the publication's active term buffer. Version `2` introduced the 36-byte little-endian header with an application-defined reserved value. Version `1` used a 32-byte big-endian header with a mandatory payload CRC32.

## Protocol Evolution And Recording Migration

Protocol changes must keep live peer negotiation and persisted recordings separate:

- Incompatible wire changes increment the frame version and are rejected by older decoders before payload interpretation.
- Compatible additions use new frame types, flags, or payload extensions only after conformance tests document how older peers reject or ignore them.
- New `ERROR` codes are safe to add because peers preserve the numeric code and message even when the code is unknown to their source version.
- Archive recordings must keep the protocol version and descriptor metadata needed to decode the frames they contain.
- Recording migration should be explicit: read the old catalog and segments, validate integrity, write a new recording/catalog with the target format, and preserve source metadata for audit.
- Replay should not silently rewrite old recordings. If replay requires format conversion, tooling should perform a migration step first or expose the conversion in the replay report.
- Rolling upgrades should prefer a version range that overlaps during deployment. When no overlap exists, the peer must return `ERROR` code `Unsupported version`.

## Term Buffers

Each publication owns an in-memory log with three fixed-length term buffers. A term starts `clean`, becomes `active` while frames are appended, and becomes `dirty` after rotation. When the active term cannot fit the next aligned frame, the publication rotates to the next partition, increments the term ID, clears that partition, and appends at offset `0`.

Term lengths must be powers of two between 64 KiB and 1 GiB. Appended frames are aligned to 32-byte boundaries. The append position is computed as `(termID - initialTermID) * termLength + termOffset + alignedFrameLength`, matching Aeron's position model while Bunshin remains a Go-native protocol.

## Publication Offer

`Publication.Offer` is the non-blocking publication entry point for callers that want stable status values instead of blocking for send-window capacity. It returns `PublicationOfferResult` with `OfferAccepted`, `OfferBackPressured`, `OfferClosed`, `OfferPayloadTooLarge`, or `OfferFailed`. Accepted offers include the resulting stream position from the publication term log.

Offer does not wait for window capacity to become available. If the current publication window cannot reserve the message immediately, it returns `OfferBackPressured`. On the current direct transport implementation, an accepted offer still completes the transport send and application-level ACK/STATUS path before returning; the media-driver path can later make that completion asynchronous behind the same status API.

`Publication.OfferVectored` and `Publication.SendVectored` gather multiple byte slices into one application message before applying the same fragmentation, flow-control, response-channel, and ACK/STATUS behavior.

`Publication.Claim` returns a single-message `PublicationClaim` buffer. Callers write payload bytes directly into `claim.Buffer()` and then call `Commit`, which offers that buffer through the same status path as `Publication.Offer`. `Abort` discards the claim without publishing. The current direct transports still encode frames into transport buffers during commit; the claim API removes an application-side payload copy and gives the media-driver path a stable claim/commit surface to back with shared term buffers later.

`DialExclusivePublication` creates an `ExclusivePublication`, a single-writer facade over the same publication transport. It exposes send, offer, vectored offer, claim/commit, destination management, and lifecycle methods without promising multi-goroutine safety. This gives hot-path callers and the future media-driver path a distinct single-writer API surface.

## Subscription Polling

`Subscription.Poll` and `Subscription.PollN` provide pull-based receive loops on the same delivery path as `Serve`. They still use ordered delivery, archive recording, response-channel decoding, UDP STATUS/ACK, and handler error propagation.

`Poll` delivers at most one complete message. `PollN` accepts a DATA fragment budget and delivers every complete message made available within that budget. For fragmented UDP payloads, a call can consume some fragments and return zero delivered messages; later polls continue from the preserved fragment state and deliver the message once all fragments arrive. QUIC DATA streams are processed as complete stream units because all fragments for one message arrive on the same QUIC stream.

`Subscription.ControlledPoll` and `Subscription.ControlledPollN` use a `ControlledHandler` that returns `ControlledPollContinue`, `ControlledPollBreak`, `ControlledPollAbort`, or `ControlledPollCommit`. Continue and commit accept the current message and keep polling, break accepts the current message and stops the current poll, and abort rejects the current message through the existing handler-failure path and stops the current poll. On the current direct transports, the network frame has already been consumed by the time abort is returned; abort is therefore a reject/stop signal rather than a durable rewind.

## Subscription Images

An `Image` represents a subscription-side stream/session/source tuple. It records the source identity, initial term ID, term buffer length, join position, observed position, current consumed position, last observed/delivered sequence, lag bytes, and availability timestamps. `Message.Image` points at the image used for that delivery, and `Message.Position` carries the consumed position after the message.

`SubscriptionConfig.AvailableImage` runs when the first message for a source creates an image. `SubscriptionConfig.UnavailableImage` runs when the subscription closes and marks all active images unavailable. `Subscription.Images` returns snapshots for tooling and diagnostics, and `Subscription.LagReports` returns the same source/session state as lag-focused reports.

## Fragmentation

A publication splits an application payload into DATA frames whose encoded size is at most `MTUBytes`. All fragments for one application message use the same stream ID, session ID, sequence, reserved value, and fragment count. Each fragment has its own term ID and term offset because every encoded fragment is appended separately to the publication term log.

All fragments for a message are sent on one QUIC stream or as separate UDP datagrams. The subscription reassembles them by fragment index, invokes the application handler once with the full payload, and sends one ACK matching the final fragment metadata. Missing, duplicate, mismatched, or incomplete fragments are protocol errors.

## Response Channels

`PublicationConfig.ResponseChannel` attaches a protocol-level reply address to each DATA message sent by that publication. Subscriptions expose it as `Message.ResponseChannel`; handlers can call `Message.Respond` to publish a reply without parsing a reply address from the application payload.

When a response channel is present, the first DATA frame payload starts with a two-byte little-endian response metadata length, followed by the response metadata, followed by the application payload bytes for that fragment. The response metadata layout is:

| Offset | Size | Type | Field | Description |
| --- | ---: | --- | --- | --- |
| 0 | 4 | uint32 | Stream ID | Stream to use for the response publication. |
| 4 | 2 | uint16 | Transport length | Byte length of the transport string. |
| 6 | 2 | uint16 | Remote address length | Byte length of the response remote address string. |
| 8 | variable | bytes | Transport | `quic` or `udp`. |
| 8 + transport length | variable | bytes | Remote address | Address passed to the response publication as `RemoteAddr`. |

The response-channel envelope is counted against the first frame's MTU payload capacity. If the application payload is fragmented, only fragment `0` carries the envelope; reassembly strips the envelope before invoking the handler.

## Local Spy Subscriptions

`SubscriptionConfig.LocalSpy` creates an in-process subscription that observes outbound publications without opening a network listener. The spy matches by transport, stream ID, and endpoint: a spy subscription's `LocalAddr` must equal the publication destination endpoint. Channel URIs can set `spy=true`, and `ChannelURI.SubscriptionConfig` maps that parameter to `LocalSpy`.

Local spies are passive observers. A publication still sends through its configured transport and waits for the real receiver ACK/STATUS path. After a send succeeds, Bunshin dispatches a cloned `Message` to matching local spies. If a spy channel is full, the spy drops the observation and increments its dropped-frame metric rather than applying back pressure to the publication.

For UDP multi-destination sends, a spy can observe any active destination it matches. For multicast sends, the spy endpoint is the multicast group address.

## Flow Control

`PublicationConfig.FlowControl` can provide a sender flow-control strategy. If unset, Bunshin uses `UnicastFlowControl`, which follows Aeron's default unicast behavior of advancing the sender limit to the maximum receiver right edge.

`MaxMulticastFlowControl` applies the same max-right-edge rule for multicast-style receiver sets. `MinMulticastFlowControl` tracks receiver right edges and uses the slowest active receiver until that receiver times out. `PreferredMulticastFlowControl` gives a configured receiver ID set priority; if any preferred receiver is active, the sender limit is based on the slowest preferred receiver, otherwise it falls back to the slowest active receiver. QUIC publications currently have one remote endpoint. UDP publications can send to multiple unicast destinations or a multicast group, so these strategies can track multiple receiver right edges.

## UDP Destinations

`PublicationConfig.RemoteAddr` supplies the initial UDP destination. `PublicationConfig.UDPDestinations` can add more destinations at dial time. Channel URIs can also carry repeated `destination=` parameters, and `ChannelURI.PublicationConfig` maps them into the UDP destination list. This covers manual MDC configuration. `Publication.AddDestination`, `Publication.RemoveDestination`, and `Publication.Destinations` provide dynamic MDC control after dial.

Each UDP `Send` first ensures that each destination has a fresh setup handshake. A publication sends a `HELLO` datagram before the first DATA for a destination and refreshes that setup after the destination has been silent longer than `PublicationConfig.UDPReceiverTimeout`. Setup frames do not consume DATA sequence numbers. After setup, the send writes the DATA datagrams to every active destination and waits for each destination to ACK the sequence. STATUS frames are applied independently per receiver ID, so multicast-oriented flow-control strategies can track multiple receiver right edges.

UDP publications preserve the configured destination endpoint strings separately from the current resolved UDP addresses. `Publication.ReResolveDestinations` refreshes all destination addresses on demand. `PublicationConfig.UDPNameResolutionInterval` refreshes them before sends when the interval has elapsed, and channel URIs can set the same behavior with `name-resolution-interval=10s`. `Publication.DestinationEndpoints` returns the configured endpoint strings; `Publication.Destinations` returns the current resolved addresses.

`Publication.DestinationStatuses` returns a liveness snapshot for each active UDP destination: configured endpoint, resolved remote address, active flag, last setup time, last STATUS time, last ACK time, last feedback time, last sequence, last RTT, and retransmitted frame count. `PublicationConfig.UDPReceiverTimeout` controls how long a destination remains active after feedback; it defaults to the same timeout used by multicast flow-control receiver tracking.

UDP channel endpoints may use wildcard ports such as `endpoint=127.0.0.1:0`. After a subscription binds, `Subscription.ChannelURI` reports the concrete bound endpoint with the actual port. `Publication.ChannelURI` reports the configured publication endpoint, dynamic destinations, and name-resolution interval.

## UDP Multicast

If a UDP subscription `LocalAddr` resolves to a multicast group, Bunshin joins that group with `net.ListenMulticastUDP`. `SubscriptionConfig.UDPMulticastInterface` can name the interface to join on; if empty, the OS default multicast interface is used.

UDP publications can use a multicast group as `RemoteAddr` or as a dynamic destination. `PublicationConfig.UDPMulticastInterface` can name the outbound multicast interface. For multicast destinations, Bunshin enables multicast loopback and uses TTL/hop-limit `1` by default. Because receiver ACKs come from concrete receiver addresses rather than the group address, the publisher treats the first receiver ACK as satisfying the multicast destination while applying STATUS frames under each receiver's real address.

## Receiver Status

UDP subscriptions send a STATUS frame before the application-level ACK for each delivered message. The STATUS frame uses the DATA stream, session, and sequence values. Its Term ID and Term offset carry the receiver position after the final DATA fragment. Its payload is four bytes:

| Offset | Size | Type | Field | Description |
| --- | ---: | --- | --- | --- |
| 0 | 4 | uint32 | Window length | Receiver window in bytes. Defaults to `SubscriptionConfig.ReceiverWindowBytes`, which defaults to one minimum term length. |

UDP publications apply STATUS frames through the configured `FlowControlStrategy`. If an older peer only sends ACK, the publication falls back to the local append position and configured publication window.

## NAK Repair

UDP subscriptions send NAK frames when sequence-gap detection observes missing messages. The NAK frame uses the DATA stream and session values. Its payload is sixteen bytes:

| Offset | Size | Type | Field | Description |
| --- | ---: | --- | --- | --- |
| 0 | 8 | uint64 | From sequence | First missing publisher sequence, inclusive. |
| 8 | 8 | uint64 | To sequence | Last missing publisher sequence, inclusive. |

UDP publications keep a bounded retransmit cache of recently sent DATA datagrams. `PublicationConfig.UDPRetransmitBufferBytes` controls the cache size and defaults to 1 MiB on the UDP transport. When a NAK range matches cached DATA, the publication retransmits those datagrams and increments the retransmit counter. NAK ranges are capped to avoid unbounded repair loops.

## Gap Detection

Each subscription tracks the next expected publisher sequence per stream, session, and remote source. If a later sequence arrives first, Bunshin records the missing sequence range as a loss observation and aggregates it in `LossReports`.

This is an application-level sequence report. It does not imply QUIC packet loss. On UDP, the same observation also drives NAK repair. Late arrivals can fill an earlier sequence hole, but the original observation remains in the loss report for diagnostics.

## Ordered Delivery

Subscriptions gate handler invocation by stream, session, and remote source. A message with a later sequence can be read from the transport and recorded as a gap, but it is buffered until earlier sequences for the same stream/session/source have been handled. ACK frames are sent only after the corresponding message has been delivered to the handler.

Duplicate or late sequences that have already been delivered are ACKed without invoking the handler again.

For UDP, a complete DATA message that arrives ahead of a sequence gap is attached to the receiver image before it is delivered. The receiver writes the DATA frames into an image-local rebuild buffer by term ID and term offset. The image `ObservedPosition` and `LastObservedSequence` advance to the highest complete DATA seen, while `CurrentPosition` and `LastSequence` advance only after ordered delivery. `Subscription.Images` and `Subscription.LagReports` expose `RebuildMessages`, `RebuildFrames`, and `RebuildBytes` so NAK repair lag is visible while missing sequences are rebuilt.

## Negotiation

QUIC publications send a `HELLO` frame before the first `DATA` frame. UDP publications send a `HELLO` datagram before the first `DATA` datagram for each destination and refresh it when the destination has been silent past the configured receiver timeout. The HELLO payload is two bytes:

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
