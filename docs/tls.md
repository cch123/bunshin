# TLS Configuration

Bunshin's default transport uses QUIC through `quic-go`, so TLS is part of the transport contract.

This is a deliberate deviation from Aeron's native UDP/media-driver model. The project still follows Aeron first for messaging semantics, buffering, flow control, and lifecycle design, but QUIC requires TLS for connection establishment and transport security.

## Development Defaults

If no TLS configuration is provided:

- `ListenSubscription` generates a short-lived self-signed certificate.
- `DialPublication` uses `InsecureSkipVerify`.

This default exists only so local tests and examples can run without certificate setup. Do not use it in production.

## Server

```go
serverTLS, err := bunshin.ServerTLSConfigFromFiles(bunshin.ServerTLSFiles{
    CertFile: "/etc/bunshin/server.crt",
    KeyFile:  "/etc/bunshin/server.key",
})
if err != nil {
    return err
}

sub, err := bunshin.ListenSubscription(bunshin.SubscriptionConfig{
    StreamID:  1,
    LocalAddr: "0.0.0.0:40456",
    TLSConfig: serverTLS,
})
```

For mutual TLS, set `ClientCA`:

```go
serverTLS, err := bunshin.ServerTLSConfigFromFiles(bunshin.ServerTLSFiles{
    CertFile: "/etc/bunshin/server.crt",
    KeyFile:  "/etc/bunshin/server.key",
    ClientCA: "/etc/bunshin/client-ca.crt",
})
```

## Client

```go
clientTLS, err := bunshin.ClientTLSConfigFromFiles(bunshin.ClientTLSFiles{
    CAFile:     "/etc/bunshin/server-ca.crt",
    ServerName: "bunshin.example.com",
})
if err != nil {
    return err
}

pub, err := bunshin.DialPublication(bunshin.PublicationConfig{
    StreamID:   1,
    RemoteAddr: "bunshin.example.com:40456",
    TLSConfig:  clientTLS,
})
```

For mutual TLS, include the client certificate and key:

```go
clientTLS, err := bunshin.ClientTLSConfigFromFiles(bunshin.ClientTLSFiles{
    CAFile:     "/etc/bunshin/server-ca.crt",
    CertFile:   "/etc/bunshin/client.crt",
    KeyFile:    "/etc/bunshin/client.key",
    ServerName: "bunshin.example.com",
})
```

## Operational Notes

- Certificates must be rotated outside Bunshin for now.
- The configured certificate `ServerName` must match the QUIC endpoint.
- `NextProtos` is set to `bunshin/3` by the helper functions.
- TLS minimum version is TLS 1.3.
