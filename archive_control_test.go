package bunshin

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestArchiveControlServerRequests(t *testing.T) {
	archive, err := OpenArchive(ArchiveConfig{Path: filepath.Join(t.TempDir(), "archive")})
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()

	server, err := StartArchiveControlServer(archive, ArchiveControlConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := server.Client()
	ctx := context.Background()

	descriptor, err := client.StartRecording(ctx, 10, 20)
	if err != nil {
		t.Fatal(err)
	}
	record, err := archive.Record(Message{StreamID: 10, SessionID: 20, Sequence: 1, Payload: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}
	if record.RecordingID != descriptor.RecordingID {
		t.Fatalf("recording id = %d, want %d", record.RecordingID, descriptor.RecordingID)
	}

	queried, err := client.QueryRecording(ctx, descriptor.RecordingID)
	if err != nil {
		t.Fatal(err)
	}
	if queried.StopPosition != record.NextPosition {
		t.Fatalf("queried stop position = %d, want %d", queried.StopPosition, record.NextPosition)
	}
	recordings, err := client.ListRecordings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recordings) != 1 || recordings[0].RecordingID != descriptor.RecordingID {
		t.Fatalf("unexpected recordings: %#v", recordings)
	}

	var replayed []Message
	result, err := client.Replay(ctx, ArchiveReplayConfig{RecordingID: descriptor.RecordingID}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Records != 1 || len(replayed) != 1 || string(replayed[0].Payload) != "one" {
		t.Fatalf("unexpected replay result=%#v messages=%#v", result, replayed)
	}

	if err := client.TruncateRecording(ctx, descriptor.RecordingID, record.Position); err != nil {
		t.Fatal(err)
	}
	report, err := client.IntegrityScan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Records != 0 {
		t.Fatalf("records after truncate = %d, want 0", report.Records)
	}
	if err := client.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	recordings, err = client.ListRecordings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recordings) != 0 {
		t.Fatalf("recordings after purge: %#v", recordings)
	}
}

func TestArchiveControlServerSegmentRequests(t *testing.T) {
	archive, err := OpenArchive(ArchiveConfig{
		Path:          filepath.Join(t.TempDir(), "archive"),
		SegmentLength: 192,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()

	first, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 1, Payload: []byte("0123456789012345678901234567890123456789")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 2, Payload: []byte("abcdefghijklmnopqrstuvwxyzabcdefghijklmn")}); err != nil {
		t.Fatal(err)
	}
	server, err := StartArchiveControlServer(archive, ArchiveControlConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := server.Client()
	ctx := context.Background()

	segments, err := client.ListRecordingSegments(ctx, first.RecordingID)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 2 || segments[0].State != ArchiveSegmentAttached {
		t.Fatalf("unexpected control segments: %#v", segments)
	}
	detached, err := client.DetachRecordingSegment(ctx, first.RecordingID, first.SegmentBase)
	if err != nil {
		t.Fatal(err)
	}
	if detached.State != ArchiveSegmentDetached {
		t.Fatalf("unexpected detached control segment: %#v", detached)
	}
	attached, err := client.AttachRecordingSegment(ctx, first.RecordingID, first.SegmentBase)
	if err != nil {
		t.Fatal(err)
	}
	if attached.State != ArchiveSegmentAttached {
		t.Fatalf("unexpected attached control segment: %#v", attached)
	}
}

func TestArchiveControlServerAuthorizer(t *testing.T) {
	archive := openTestArchive(t)
	defer archive.Close()
	if _, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 1, Payload: []byte("one")}); err != nil {
		t.Fatal(err)
	}

	errUnauthorized := errors.New("unauthorized archive operation")
	var actions []ArchiveControlAction
	server, err := StartArchiveControlServer(archive, ArchiveControlConfig{
		Authorizer: func(_ context.Context, action ArchiveControlAction) error {
			actions = append(actions, action)
			if action == ArchiveControlActionPurge {
				return errUnauthorized
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := server.Client()

	if _, err := client.ListRecordings(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.Purge(context.Background()); !errors.Is(err, errUnauthorized) {
		t.Fatalf("Purge() err = %v, want %v", err, errUnauthorized)
	}
	recordings, err := archive.ListRecordings()
	if err != nil {
		t.Fatal(err)
	}
	if len(recordings) != 1 {
		t.Fatalf("recordings were purged despite authorization failure: %#v", recordings)
	}
	if len(actions) != 2 || actions[0] != ArchiveControlActionListRecordings || actions[1] != ArchiveControlActionPurge {
		t.Fatalf("unexpected authorized actions: %#v", actions)
	}
}

func TestArchiveControlServerRejectsAfterClose(t *testing.T) {
	archive := openTestArchive(t)
	defer archive.Close()

	server, err := StartArchiveControlServer(archive, ArchiveControlConfig{})
	if err != nil {
		t.Fatal(err)
	}
	client := server.Client()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListRecordings(context.Background()); !errors.Is(err, ErrArchiveControlClosed) {
		t.Fatalf("ListRecordings() err = %v, want %v", err, ErrArchiveControlClosed)
	}
}
