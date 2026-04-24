# Bunshin

Bunshin is a small Go transport prototype inspired by the public Apache-2.0 Aeron projects, not a 1:1 source-code translation and not an official Aeron distribution.

The first cut focuses on a narrow, testable slice:

- UDP unicast publication/subscription.
- Binary frame header with stream, session, sequence, and payload fields.
- ACK-based reliable send with configurable retransmission.
- Subscriber-side duplicate suppression.

## Example

```go
sub, _ := bunshin.ListenSubscription(bunshin.SubscriptionConfig{
    StreamID:  1,
    LocalAddr: "127.0.0.1:40456",
})
go sub.Serve(context.Background(), func(ctx context.Context, msg bunshin.Message) error {
    fmt.Printf("%s\n", msg.Payload)
    return nil
})

pub, _ := bunshin.DialPublication(bunshin.PublicationConfig{
    StreamID:   1,
    RemoteAddr: "127.0.0.1:40456",
})
_ = pub.Send(context.Background(), []byte("hello"))
```

## Scope

A full Aeron-compatible Go implementation would still need term buffers, flow control, loss reports, status messages, NAK repair, media-driver separation, archive, cluster, counters, tooling, and protocol compatibility work. This repository currently establishes the Go API and a working reliable UDP baseline to extend from.
