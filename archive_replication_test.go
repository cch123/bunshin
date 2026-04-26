package bunshin

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestReplicateArchiveStoppedRecording(t *testing.T) {
	src, first := openStoppedArchiveForReplication(t)
	defer src.Close()

	dst, err := OpenArchive(ArchiveConfig{
		Path:          filepath.Join(t.TempDir(), "dst"),
		SegmentLength: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()

	report, err := ReplicateArchive(context.Background(), src, dst, ArchiveReplicationConfig{RecordingID: first.RecordingID})
	if err != nil {
		t.Fatal(err)
	}
	if report.SourceRecordingID != first.RecordingID || report.DestinationRecordingID == 0 ||
		report.Segments != 2 || report.Bytes == 0 || report.RecordingBytes == 0 {
		t.Fatalf("unexpected replication report: %#v", report)
	}

	descriptor, err := dst.RecordingDescriptor(report.DestinationRecordingID)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.StoppedAt == nil || descriptor.SegmentLength != 192 || descriptor.StopPosition <= first.NextPosition {
		t.Fatalf("unexpected replicated descriptor: %#v", descriptor)
	}

	var replayed []Message
	if err := dst.Replay(context.Background(), ArchiveReplayConfig{RecordingID: report.DestinationRecordingID}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, msg)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 2 || replayed[0].Sequence != 1 || replayed[1].Sequence != 2 ||
		string(replayed[0].Payload) != "0123456789012345678901234567890123456789" {
		t.Fatalf("unexpected replicated replay: %#v", replayed)
	}
	verify, err := VerifyArchive(dst.dir)
	if err != nil {
		t.Fatal(err)
	}
	if !verify.OK || verify.Integrity.Records != 2 {
		t.Fatalf("unexpected replicated verification: %#v", verify)
	}
}

func TestReplicateArchiveRejectsActiveRecording(t *testing.T) {
	src := openTestArchive(t)
	defer src.Close()
	first, err := src.Record(Message{StreamID: 1, SessionID: 1, Sequence: 1, Payload: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}
	dst := openTestArchive(t)
	defer dst.Close()

	if _, err := ReplicateArchive(context.Background(), src, dst, ArchiveReplicationConfig{RecordingID: first.RecordingID}); !errors.Is(err, ErrArchiveReplicationActiveRecording) {
		t.Fatalf("ReplicateArchive() err = %v, want %v", err, ErrArchiveReplicationActiveRecording)
	}
}

func TestReplicateArchiveLiveFollowsActiveRecordingUntilStop(t *testing.T) {
	src := openTestArchive(t)
	defer src.Close()
	first, err := src.Record(Message{StreamID: 1, SessionID: 1, Sequence: 1, Payload: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}

	dst := openTestArchive(t)
	defer dst.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type liveResult struct {
		report ArchiveReplicationReport
		err    error
	}
	done := make(chan liveResult, 1)
	go func() {
		report, err := ReplicateArchiveLive(ctx, src, dst, ArchiveLiveReplicationConfig{RecordingID: first.RecordingID})
		done <- liveResult{report: report, err: err}
	}()

	dstRecordingID := waitForReplicatedRecording(t, dst)
	if _, err := src.Record(Message{StreamID: 1, SessionID: 1, Sequence: 2, Payload: []byte("two")}); err != nil {
		t.Fatal(err)
	}
	third, err := src.Record(Message{StreamID: 1, SessionID: 1, Sequence: 3, Payload: []byte("three")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.StopRecording(first.RecordingID); err != nil {
		t.Fatal(err)
	}

	var result liveResult
	select {
	case result = <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.report.SourceRecordingID != first.RecordingID || result.report.DestinationRecordingID != dstRecordingID ||
		!result.report.Complete || result.report.Live || result.report.StopPosition != third.NextPosition {
		t.Fatalf("unexpected live replication report: %#v dstRecordingID=%d third=%#v", result.report, dstRecordingID, third)
	}

	descriptor, err := dst.RecordingDescriptor(dstRecordingID)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.StoppedAt == nil || descriptor.StopPosition != third.NextPosition {
		t.Fatalf("unexpected destination descriptor: %#v", descriptor)
	}

	var replayed []Message
	if err := dst.Replay(context.Background(), ArchiveReplayConfig{RecordingID: dstRecordingID}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, msg)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 3 || replayed[0].Sequence != 1 || replayed[1].Sequence != 2 || replayed[2].Sequence != 3 ||
		string(replayed[0].Payload) != "one" || string(replayed[2].Payload) != "three" {
		t.Fatalf("unexpected live replicated replay: %#v", replayed)
	}
}

func TestReplicateArchiveRejectsDetachedSegment(t *testing.T) {
	src, first := openStoppedArchiveForReplication(t)
	defer src.Close()
	if _, err := src.DetachRecordingSegment(first.RecordingID, first.SegmentBase); err != nil {
		t.Fatal(err)
	}
	dst := openTestArchive(t)
	defer dst.Close()

	if _, err := ReplicateArchive(context.Background(), src, dst, ArchiveReplicationConfig{RecordingID: first.RecordingID}); !errors.Is(err, ErrArchiveCorrupt) {
		t.Fatalf("ReplicateArchive() err = %v, want %v", err, ErrArchiveCorrupt)
	}
}

func TestReplicateArchivePreservesDestinationActiveRecording(t *testing.T) {
	src, first := openStoppedArchiveForReplication(t)
	defer src.Close()

	dst := openTestArchive(t)
	defer dst.Close()
	active, err := dst.Record(Message{StreamID: 9, SessionID: 9, Sequence: 1, Payload: []byte("active")})
	if err != nil {
		t.Fatal(err)
	}
	report, err := ReplicateArchive(context.Background(), src, dst, ArchiveReplicationConfig{RecordingID: first.RecordingID})
	if err != nil {
		t.Fatal(err)
	}
	next, err := dst.Record(Message{StreamID: 9, SessionID: 9, Sequence: 2, Payload: []byte("still-active")})
	if err != nil {
		t.Fatal(err)
	}
	if next.RecordingID != active.RecordingID || report.DestinationRecordingID == active.RecordingID {
		t.Fatalf("replication changed active recording: active=%#v next=%#v report=%#v", active, next, report)
	}
}

func openStoppedArchiveForReplication(t *testing.T) (*Archive, ArchiveRecord) {
	t.Helper()

	archive, err := OpenArchive(ArchiveConfig{
		Path:          filepath.Join(t.TempDir(), "src"),
		SegmentLength: 192,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 1, Payload: []byte("0123456789012345678901234567890123456789")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 2, Payload: []byte("abcdefghijklmnopqrstuvwxyzabcdefghijklmn")}); err != nil {
		t.Fatal(err)
	}
	if _, err := archive.StopRecording(first.RecordingID); err != nil {
		t.Fatal(err)
	}
	return archive, first
}

func waitForReplicatedRecording(t *testing.T, archive *Archive) int64 {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last []ArchiveRecordingDescriptor
	var lastErr error
	for time.Now().Before(deadline) {
		recordings, err := archive.ListRecordings()
		if err != nil {
			lastErr = err
		} else if len(recordings) > 0 {
			return recordings[0].RecordingID
		} else {
			last = recordings
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for replicated recording: last=%#v err=%v", last, lastErr)
	return 0
}
