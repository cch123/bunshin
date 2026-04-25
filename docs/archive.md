# Archive

Bunshin includes a local append-only archive for recording and replaying delivered subscription messages. It is process-local and file-backed; replication and media-driver ownership are still future work.

## Opening An Archive

```go
archive, err := bunshin.OpenArchive(bunshin.ArchiveConfig{
    Path: "/var/lib/bunshin/stream.bsar",
    Sync: true,
})
defer archive.Close()
```

`Sync` calls `fsync` after each appended record and after truncate/purge operations. Leave it disabled for tests or buffered local development.

## Recording

Attach an archive to a subscription to record messages after ordered delivery and before the application handler is invoked:

```go
sub, err := bunshin.ListenSubscription(bunshin.SubscriptionConfig{
    StreamID:  1,
    LocalAddr: "0.0.0.0:40456",
    Archive:   archive,
})
```

You can also record manually:

```go
record, err := archive.Record(msg)
fmt.Println(record.Position, record.NextPosition)
```

`Archive.RecordingHandler` wraps an existing handler when explicit composition is more convenient than `SubscriptionConfig.Archive`.

## Replay

Replay starts from an archive byte position. A zero `StreamID` or `SessionID` in the replay config means "all".

```go
err := archive.Replay(ctx, bunshin.ArchiveReplayConfig{
    FromPosition: 0,
    StreamID:      1,
}, func(ctx context.Context, msg bunshin.Message) error {
    fmt.Printf("%d %s\n", msg.Sequence, msg.Payload)
    return nil
})
```

Replay uses a snapshot of the file size taken when replay begins, so concurrent appends after that point are not included until the next replay.

## Maintenance

`IntegrityScan` reads every record and validates the per-record checksum:

```go
report, err := archive.IntegrityScan()
fmt.Println(report.Records, report.Bytes, report.CorruptPosition, err)
```

`Truncate(position)` trims the archive at a valid record boundary. `Purge()` truncates it to zero bytes.

## File Format

Each record is a 64-byte little-endian header followed by the payload bytes.

| Offset | Size | Field |
| --- | ---: | --- |
| 0 | 4 | Magic `BSAR` |
| 4 | 1 | Archive format version, currently `1` |
| 5 | 1 | Flags, currently `0` |
| 6 | 2 | Header length, currently `64` |
| 8 | 8 | Total record length |
| 16 | 4 | Stream ID |
| 20 | 4 | Session ID |
| 24 | 4 | Term ID |
| 28 | 4 | Term offset |
| 32 | 8 | Sequence |
| 40 | 8 | Reserved value |
| 48 | 8 | Recorded timestamp as Unix nanoseconds |
| 56 | 4 | Payload length |
| 60 | 4 | CRC32 over header bytes `0..59` and payload bytes |

The archive persists Bunshin message metadata and payload bytes. It does not persist `Message.Remote`, because replay is local and not tied to the original QUIC peer address.
