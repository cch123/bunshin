package bunshin

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestArchiveRecordReplayAndFilter(t *testing.T) {
	archive := openTestArchive(t)
	defer archive.Close()

	firstPayload := []byte("one")
	first, err := archive.Record(Message{
		StreamID:      10,
		SessionID:     20,
		TermID:        7,
		TermOffset:    32,
		Sequence:      1,
		ReservedValue: 99,
		Payload:       firstPayload,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstPayload[0] = 'x'

	second, err := archive.Record(Message{
		StreamID:  11,
		SessionID: 20,
		Sequence:  2,
		Payload:   []byte("two"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Position != 0 || first.NextPosition <= first.Position || second.Position != first.NextPosition {
		t.Fatalf("unexpected positions: first=%#v second=%#v", first, second)
	}

	var replayed []Message
	err = archive.Replay(context.Background(), ArchiveReplayConfig{FromPosition: 0}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 2 || string(replayed[0].Payload) != "one" || string(replayed[1].Payload) != "two" {
		t.Fatalf("unexpected replay: %#v", replayed)
	}
	if replayed[0].StreamID != 10 || replayed[0].SessionID != 20 || replayed[0].TermID != 7 ||
		replayed[0].TermOffset != 32 || replayed[0].Sequence != 1 || replayed[0].ReservedValue != 99 {
		t.Fatalf("unexpected first message metadata: %#v", replayed[0])
	}

	replayed = nil
	err = archive.Replay(context.Background(), ArchiveReplayConfig{
		FromPosition: first.Position,
		StreamID:     11,
	}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 1 || replayed[0].StreamID != 11 || string(replayed[0].Payload) != "two" {
		t.Fatalf("unexpected filtered replay: %#v", replayed)
	}
}

func TestSubscriptionRecordsLiveMessagesToArchive(t *testing.T) {
	archive := openTestArchive(t)
	defer archive.Close()

	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  111,
		LocalAddr: "127.0.0.1:0",
		Archive:   archive,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	delivered := make(chan Message, 1)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			delivered <- msg
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:   111,
		SessionID:  212,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Send(ctx, []byte("record me")); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-delivered:
		if msg.StreamID != 111 || msg.SessionID != 212 || string(msg.Payload) != "record me" {
			t.Fatalf("unexpected delivered message: %#v", msg)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	var replayed []Message
	err = archive.Replay(context.Background(), ArchiveReplayConfig{}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 1 || replayed[0].StreamID != 111 || replayed[0].SessionID != 212 || string(replayed[0].Payload) != "record me" {
		t.Fatalf("unexpected archive replay: %#v", replayed)
	}
}

func TestArchiveRecordingHandler(t *testing.T) {
	archive := openTestArchive(t)
	defer archive.Close()

	called := false
	handler := archive.RecordingHandler(func(context.Context, Message) error {
		called = true
		return nil
	})
	if err := handler(context.Background(), Message{StreamID: 1, SessionID: 2, Sequence: 3, Payload: []byte("payload")}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("wrapped handler was not called")
	}

	report, err := archive.IntegrityScan()
	if err != nil {
		t.Fatal(err)
	}
	if report.Records != 1 {
		t.Fatalf("records = %d, want 1", report.Records)
	}
}

func TestArchiveTruncatePurgeAndIntegrityScan(t *testing.T) {
	archive := openTestArchive(t)
	defer archive.Close()

	first, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 1, Payload: []byte("first")})
	if err != nil {
		t.Fatal(err)
	}
	second, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 2, Payload: []byte("second")})
	if err != nil {
		t.Fatal(err)
	}

	report, err := archive.IntegrityScan()
	if err != nil {
		t.Fatal(err)
	}
	if report.Records != 2 || report.Bytes != second.NextPosition || report.CorruptPosition != -1 {
		t.Fatalf("unexpected scan report: %#v", report)
	}

	if err := archive.Truncate(first.Position + 1); !errors.Is(err, ErrArchivePosition) {
		t.Fatalf("truncate invalid boundary err = %v, want %v", err, ErrArchivePosition)
	}
	if err := archive.Truncate(second.Position); err != nil {
		t.Fatal(err)
	}
	report, err = archive.IntegrityScan()
	if err != nil {
		t.Fatal(err)
	}
	if report.Records != 1 || report.Bytes != second.Position {
		t.Fatalf("unexpected scan after truncate: %#v", report)
	}

	var replayed []Message
	err = archive.Replay(context.Background(), ArchiveReplayConfig{}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 1 || string(replayed[0].Payload) != "first" {
		t.Fatalf("unexpected replay after truncate: %#v", replayed)
	}

	if err := archive.Purge(); err != nil {
		t.Fatal(err)
	}
	report, err = archive.IntegrityScan()
	if err != nil {
		t.Fatal(err)
	}
	if report.Records != 0 || report.Bytes != 0 {
		t.Fatalf("unexpected scan after purge: %#v", report)
	}
}

func TestArchiveIntegrityScanDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive.bsar")
	archive, err := OpenArchive(ArchiveConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()

	record, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 1, Payload: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{'x'}, record.Position+archiveHeaderLen); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	report, err := archive.IntegrityScan()
	if !errors.Is(err, ErrArchiveCorrupt) {
		t.Fatalf("IntegrityScan() err = %v, want %v", err, ErrArchiveCorrupt)
	}
	if report.CorruptPosition != record.Position || report.Records != 0 {
		t.Fatalf("unexpected corrupt report: %#v", report)
	}
}

func TestArchiveRejectsOperationsAfterClose(t *testing.T) {
	archive := openTestArchive(t)
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := archive.Record(Message{}); !errors.Is(err, ErrArchiveClosed) {
		t.Fatalf("Record() err = %v, want %v", err, ErrArchiveClosed)
	}
	if err := archive.Truncate(0); !errors.Is(err, ErrArchiveClosed) {
		t.Fatalf("Truncate() err = %v, want %v", err, ErrArchiveClosed)
	}
	if _, err := archive.IntegrityScan(); !errors.Is(err, ErrArchiveClosed) {
		t.Fatalf("IntegrityScan() err = %v, want %v", err, ErrArchiveClosed)
	}
}

func openTestArchive(t *testing.T) *Archive {
	t.Helper()

	archive, err := OpenArchive(ArchiveConfig{
		Path: filepath.Join(t.TempDir(), "archive.bsar"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return archive
}
