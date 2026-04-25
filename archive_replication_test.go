package bunshin

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
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
