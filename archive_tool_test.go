package bunshin

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestArchiveDescribeAndVerify(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	archive, err := OpenArchive(ArchiveConfig{
		Path:          dir,
		SegmentLength: 192,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 1, Payload: []byte("0123456789012345678901234567890123456789")})
	if err != nil {
		t.Fatal(err)
	}
	second, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 2, Payload: []byte("abcdefghijklmnopqrstuvwxyzabcdefghijklmn")})
	if err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}

	description, err := DescribeArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	if description.RecordingCount != 1 || description.SegmentCount != 2 || description.AttachedSegments != 2 ||
		description.DetachedSegments != 0 || description.MissingSegments != 0 || description.Bytes != second.NextPosition {
		t.Fatalf("unexpected archive description: %#v", description)
	}
	if description.Recordings[0].RecordingID != first.RecordingID {
		t.Fatalf("unexpected recording in description: %#v", description.Recordings)
	}

	report, err := VerifyArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !report.OK || report.Integrity.Records != 2 || report.Integrity.Recordings != 1 {
		t.Fatalf("unexpected verify report: %#v", report)
	}
}

func TestArchiveVerifyReportsDetachedSegment(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	archive, err := OpenArchive(ArchiveConfig{
		Path:          dir,
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
	if _, err := archive.DetachRecordingSegment(first.RecordingID, first.SegmentBase); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}

	report, err := VerifyArchive(dir)
	if !errors.Is(err, ErrArchiveCorrupt) {
		t.Fatalf("VerifyArchive() err = %v, want %v", err, ErrArchiveCorrupt)
	}
	if report.OK || len(report.DetachedSegments) != 1 || len(report.MissingSegments) != 0 {
		t.Fatalf("unexpected detached verify report: %#v", report)
	}
}

func TestArchiveMigrate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	archive, err := OpenArchive(ArchiveConfig{
		Path:          dir,
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
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}

	dst := filepath.Join(t.TempDir(), "migrated")
	report, err := MigrateArchive(dir, dst)
	if err != nil {
		t.Fatal(err)
	}
	if report.Files != 3 || report.Bytes == 0 || !report.Verification.OK {
		t.Fatalf("unexpected migration report: %#v", report)
	}

	migrated, err := OpenArchive(ArchiveConfig{Path: dst})
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	var replayed []Message
	if err := migrated.Replay(context.Background(), ArchiveReplayConfig{RecordingID: first.RecordingID}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, msg)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 2 || string(replayed[0].Payload) != "0123456789012345678901234567890123456789" {
		t.Fatalf("unexpected migrated replay: %#v", replayed)
	}
}

func TestArchiveMigrateRejectsNonEmptyDestination(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	archive, err := OpenArchive(ArchiveConfig{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "migrated")
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "existing"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := MigrateArchive(dir, dst); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("MigrateArchive() err = %v, want %v", err, ErrInvalidConfig)
	}
}
