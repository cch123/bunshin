# Archive

Bunshin includes a local append-only archive for recording and replaying stream data. It is process-local and file-backed; external archive protocols and media-driver ownership are still future work.

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

Attach an archive to a subscription to record the received Bunshin DATA frames after ordered delivery and before the application handler is invoked:

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

For lower-level stream capture, `RecordFrame` and `RecordFrames` append encoded Bunshin DATA frames directly. Fragmented frames are stored as separate archive records, and replay reassembles them before invoking the handler.

`StartRecording(streamID, sessionID)` explicitly opens a new active recording. `StopRecording(recordingID)` closes the active recording boundary; the next `Record` call starts a new recording. If you only call `Record`, Bunshin automatically starts the first recording and keeps appending to the active recording.

`Archive.RecordingHandler` wraps an existing handler when explicit composition is more convenient than `SubscriptionConfig.Archive`.

`Archive.ListRecordings` returns catalog descriptors. Each descriptor tracks `RecordingID`, start/stop positions, segment length, stream/session metadata when stable for the recording, creation/update timestamps, and `StoppedAt` when the recording has been explicitly stopped.

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

The control client supports start, stop, list, query, replay, truncate, purge, integrity scan, and segment lifecycle requests. The in-process server is a goroutine with a command queue.

`ListenArchiveControlProtocol` exposes the same control surface over a Bunshin-native JSON protocol on a `net.Listener`. Clients first send `open_session`, then each request carries a `control_session_id` and `correlation_id`. Responses echo the correlation ID. Replay delivers `replay_message` events before the final replay response, and archive recording lifecycle/progress callbacks are streamed as `recording_event` messages on the same connection.

```go
server, err := bunshin.ListenArchiveControlProtocol(ctx, archive, bunshin.ArchiveControlProtocolConfig{
    Network: "tcp",
    Addr:    "127.0.0.1:40470",
})
defer server.Close()
```

The protocol is JSON and Bunshin-native. It is intentionally not Aeron SBE compatible.

`ArchiveControlConfig.Authorizer` can reject control operations before they touch the archive. The hook receives the request context and action, so an external server can attach authenticated identity to `context.Context` and centralize authorization.

```go
control, err := bunshin.StartArchiveControlServer(archive, bunshin.ArchiveControlConfig{
    Authorizer: func(ctx context.Context, action bunshin.ArchiveControlAction) error {
        if action == bunshin.ArchiveControlActionPurge && !isAdmin(ctx) {
            return errors.New("unauthorized")
        }
        return nil
    },
})
```

## Replay

Replay starts from a recording position. A zero `RecordingID` replays all recordings in catalog order. A zero `StreamID` or `SessionID` in the replay config means "all". A zero `Length` is open-ended and replays to the recording stop position. A positive `Length` bounds replay to the archive-position byte range `[FromPosition, FromPosition+Length)`. If the range ends inside a record, replay stops before delivering that partial record. If replay starts or ends inside a fragmented raw-frame batch, Bunshin does not deliver the incomplete message.

```go
err := archive.Replay(ctx, bunshin.ArchiveReplayConfig{
    RecordingID:  1,
    FromPosition: 0,
    Length:       1024 * 1024,
    StreamID:      1,
}, func(ctx context.Context, msg bunshin.Message) error {
    fmt.Printf("%d %s\n", msg.Sequence, msg.Payload)
    return nil
})
```

Replay uses a snapshot of the catalog descriptors taken when replay begins, so concurrent appends after that point are not included until the next replay.

## Replay Merge

`Archive.NewReplayMerge` helps an application catch up from recorded history and then transition to live delivery. Start the live subscription with `merge.LiveHandler()` first so live messages are buffered while archive replay catches up, then call `merge.Replay(ctx)`.

```go
merge, err := archive.NewReplayMerge(bunshin.ArchiveReplayMergeConfig{
    Replay: bunshin.ArchiveReplayConfig{
        RecordingID:  descriptor.RecordingID,
        FromPosition: descriptor.StartPosition,
    },
    LiveBufferLimit: 4096,
}, handler)

go func() {
    _ = sub.Serve(ctx, merge.LiveHandler())
}()

result, err := merge.Replay(ctx)
fmt.Println(result.Replayed, result.Live, result.DroppedLive)
```

Replay merge is based on Bunshin message metadata, not Aeron protocol state. Live messages that arrive before replay completes are buffered in memory. After replay, buffered and future live messages with a non-zero sequence less than or equal to the last delivered sequence for the same stream/session are dropped as duplicates.

## Replication

`ReplicateArchive` copies a stopped recording from one archive to another. The destination imports it as a new recording ID and rewrites segment filenames to that ID.

`ReplicateArchiveLive` follows an active source recording. It first imports the current source snapshot, then subscribes to recording events and copies newly appended archive records until the source recording stops or the context is canceled. The destination archive must not already have an active recording because the replicated recording is active while follow-on replication is running.

```go
src, err := bunshin.OpenArchive(bunshin.ArchiveConfig{Path: "/var/lib/bunshin/archive-a"})
dst, err := bunshin.OpenArchive(bunshin.ArchiveConfig{Path: "/var/lib/bunshin/archive-b"})

report, err := bunshin.ReplicateArchive(ctx, src, dst, bunshin.ArchiveReplicationConfig{
    RecordingID: descriptor.RecordingID,
})
fmt.Println(report.SourceRecordingID, report.DestinationRecordingID, report.Segments)

liveReport, err := bunshin.ReplicateArchiveLive(ctx, src, dst, bunshin.ArchiveLiveReplicationConfig{
    RecordingID: descriptor.RecordingID,
})
fmt.Println(liveReport.Complete, liveReport.StopPosition)
```

Snapshot replication still requires stopped recordings with attached segments. Live replication accepts an active source recording and stops when the source emits its stop signal. Both replication modes are Bunshin-native; they do not implement Aeron's archive replication wire protocol.

## Maintenance

`IntegrityScan` reads every record and validates the per-record checksum:

```go
report, err := archive.IntegrityScan()
fmt.Println(report.Records, report.Bytes, report.CorruptPosition, err)
```

`Truncate(position)` trims the latest recording at a valid record boundary. `TruncateRecording(recordingID, position)` targets a specific recording. `Purge()` deletes segment files and resets the catalog.

## Segment Management

`ListRecordingSegments(recordingID)` reports each segment base and whether the file is `attached`, `detached`, or `missing`. Segment attach/detach follows the Aeron Archive lifecycle shape, but uses Bunshin-native filenames.

```go
segments, err := archive.ListRecordingSegments(recordingID)
detached, err := archive.DetachRecordingSegment(recordingID, segments[0].SegmentBase)
attached, err := archive.AttachRecordingSegment(recordingID, detached.SegmentBase)
err = archive.DeleteDetachedRecordingSegment(recordingID, detached.SegmentBase)
migrated, err := archive.MigrateDetachedRecordingSegment(recordingID, detached.SegmentBase, "/var/lib/bunshin/archive-cold")
```

Only complete historical segments can be detached. The active or final partial segment stays attached because replay and append still depend on it. A detached segment is renamed from `<recordingId>-<segmentBasePosition>.rec` to `<recordingId>-<segmentBasePosition>.detached`; replay that needs a detached or migrated segment fails until the segment is attached again.

## Tools

The archive package exposes tool-oriented helpers that are also available through the `bunshin-archive` command.

```go
description, err := bunshin.DescribeArchive("/var/lib/bunshin/archive")
verification, err := bunshin.VerifyArchive("/var/lib/bunshin/archive")
migration, err := bunshin.MigrateArchive("/var/lib/bunshin/archive", "/var/lib/bunshin/archive-copy")
```

```sh
bunshin-archive describe /var/lib/bunshin/archive
bunshin-archive verify /var/lib/bunshin/archive
bunshin-archive migrate /var/lib/bunshin/archive /var/lib/bunshin/archive-copy
bunshin-archive replicate /var/lib/bunshin/archive-a /var/lib/bunshin/archive-b 1
```

`DescribeArchive` reports catalog descriptors and segment states. `VerifyArchive` runs an integrity scan and flags detached or missing segments. `MigrateArchive` copies a healthy archive to an empty destination directory, then verifies the destination before returning.

## Storage Layout

The archive directory contains:

- `catalog.json`: recording descriptors and the next recording ID.
- `<recordingId>-<segmentBasePosition>.rec`: segment files for recording data, matching Aeron's segment naming pattern.
- `<recordingId>-<segmentBasePosition>.detached`: detached segment files kept in the archive directory until attached, deleted, or migrated.

Each segment stores a sequence of Bunshin archive records. Each record is a 64-byte little-endian header followed by the payload bytes. When a record cannot fit in the remaining bytes of a segment, Bunshin writes a padding record when there is room for a header, then continues at the next segment base position.

| Offset | Size | Field |
| --- | ---: | --- |
| 0 | 4 | Magic `BSAR` |
| 4 | 1 | Archive format version, currently `1` |
| 5 | 1 | Flags; `1` marks segment padding, `2` marks a raw Bunshin frame record |
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

The archive persists either Bunshin message metadata and payload bytes or encoded Bunshin DATA frames. It does not persist `Message.Remote`, because replay is local and not tied to the original peer address.
