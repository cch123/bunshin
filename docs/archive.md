# Archive

Bunshin includes a local append-only archive for recording and replaying delivered subscription messages. It is process-local and file-backed; replication, replay merge, and media-driver ownership are still future work.

The storage layout follows Aeron's Archive shape at a high level: an archive directory contains a recording catalog and one or more segment files per recording. The wire format remains Bunshin-native.

## Opening An Archive

```go
archive, err := bunshin.OpenArchive(bunshin.ArchiveConfig{
    Path:          "/var/lib/bunshin/archive",
    SegmentLength: 64 * 1024 * 1024,
    Sync:          true,
})
defer archive.Close()
```

`Path` is an archive directory, not a single recording file. `Sync` calls `fsync` after segment writes and catalog updates. Leave it disabled for tests or buffered local development.

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
fmt.Println(record.RecordingID, record.Position, record.NextPosition)
```

`StartRecording(streamID, sessionID)` explicitly opens a new active recording. `StopRecording(recordingID)` closes the active recording boundary; the next `Record` call starts a new recording. If you only call `Record`, Bunshin automatically starts the first recording and keeps appending to the active recording.

`Archive.RecordingHandler` wraps an existing handler when explicit composition is more convenient than `SubscriptionConfig.Archive`.

`Archive.ListRecordings` returns catalog descriptors. Each descriptor tracks `RecordingID`, start/stop positions, segment length, stream/session metadata when stable for the recording, and timestamps.

## Recording Events

`ArchiveConfig.RecordingProgressHandler` is called after a message has been durably appended to a recording. `ArchiveConfig.RecordingSignalHandler` is called for recording lifecycle signals: `start`, `extend`, `stop`, `truncate`, and `purge`.

```go
archive, err := bunshin.OpenArchive(bunshin.ArchiveConfig{
    Path: "/var/lib/bunshin/archive",
    RecordingProgressHandler: func(event bunshin.ArchiveRecordingEvent) {
        fmt.Println(event.RecordingID, event.NextPosition)
    },
    RecordingSignalHandler: func(event bunshin.ArchiveRecordingEvent) {
        fmt.Println(event.Signal, event.RecordingID, event.Position)
    },
})
```

Callbacks run after the archive lock is released, so they may query descriptors or hand the event to an application-owned control plane. Progress events include stream/session/sequence metadata and payload length, but not payload bytes.

## Control API

`StartArchiveControlServer` provides an in-process typed control channel for archive operations. It is Bunshin-native, not Aeron wire compatible.

```go
control, err := bunshin.StartArchiveControlServer(archive, bunshin.ArchiveControlConfig{})
defer control.Close()

client := control.Client()
descriptor, err := client.StartRecording(ctx, 1, 2)
recordings, err := client.ListRecordings(ctx)
queried, err := client.QueryRecording(ctx, descriptor.RecordingID)
result, err := client.Replay(ctx, bunshin.ArchiveReplayConfig{
    RecordingID: descriptor.RecordingID,
}, handler)
err = client.TruncateRecording(ctx, descriptor.RecordingID, queried.StartPosition)
err = client.Purge(ctx)
```

The control client supports start, stop, list, query, replay, truncate, purge, and integrity scan requests. The current server is an in-process goroutine with a command queue; an external command protocol can be layered on top later.

## Replay

Replay starts from a recording position. A zero `RecordingID` replays all recordings in catalog order. A zero `StreamID` or `SessionID` in the replay config means "all".

```go
err := archive.Replay(ctx, bunshin.ArchiveReplayConfig{
    RecordingID:  1,
    FromPosition: 0,
    StreamID:      1,
}, func(ctx context.Context, msg bunshin.Message) error {
    fmt.Printf("%d %s\n", msg.Sequence, msg.Payload)
    return nil
})
```

Replay uses a snapshot of the catalog descriptors taken when replay begins, so concurrent appends after that point are not included until the next replay.

## Maintenance

`IntegrityScan` reads every record and validates the per-record checksum:

```go
report, err := archive.IntegrityScan()
fmt.Println(report.Records, report.Bytes, report.CorruptPosition, err)
```

`Truncate(position)` trims the latest recording at a valid record boundary. `TruncateRecording(recordingID, position)` targets a specific recording. `Purge()` deletes segment files and resets the catalog.

## Storage Layout

The archive directory contains:

- `catalog.json`: recording descriptors and the next recording ID.
- `<recordingId>-<segmentBasePosition>.rec`: segment files for recording data, matching Aeron's segment naming pattern.

Each segment stores a sequence of Bunshin archive records. Each record is a 64-byte little-endian header followed by the payload bytes. When a record cannot fit in the remaining bytes of a segment, Bunshin writes a padding record when there is room for a header, then continues at the next segment base position.

| Offset | Size | Field |
| --- | ---: | --- |
| 0 | 4 | Magic `BSAR` |
| 4 | 1 | Archive format version, currently `1` |
| 5 | 1 | Flags; `1` marks segment padding |
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
