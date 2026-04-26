package bunshin

import (
	"bytes"
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
	if first.RecordingID == 0 || second.RecordingID != first.RecordingID {
		t.Fatalf("unexpected recording ids: first=%#v second=%#v", first, second)
	}
	descriptors, err := archive.ListRecordings()
	if err != nil {
		t.Fatal(err)
	}
	if len(descriptors) != 1 || descriptors[0].RecordingID != first.RecordingID || descriptors[0].StopPosition != second.NextPosition {
		t.Fatalf("unexpected catalog descriptors: %#v", descriptors)
	}
	if _, err := os.Stat(filepath.Join(archive.dir, archiveCatalogFile)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(archive.segmentPath(first.RecordingID, first.SegmentBase)); err != nil {
		t.Fatal(err)
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
		RecordingID:  first.RecordingID,
		FromPosition: first.Position,
		Length:       first.NextPosition - first.Position,
	}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 1 || replayed[0].Sequence != 1 || string(replayed[0].Payload) != "one" {
		t.Fatalf("unexpected bounded replay: %#v", replayed)
	}

	replayed = nil
	err = archive.Replay(context.Background(), ArchiveReplayConfig{
		RecordingID:  first.RecordingID,
		FromPosition: first.Position,
		Length:       first.NextPosition - first.Position - 1,
	}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 0 {
		t.Fatalf("short bounded replay crossed a record boundary: %#v", replayed)
	}

	if err := archive.Replay(context.Background(), ArchiveReplayConfig{Length: -1}, func(context.Context, Message) error {
		return nil
	}); !errors.Is(err, ErrArchivePosition) {
		t.Fatalf("Replay() err = %v, want %v", err, ErrArchivePosition)
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

func TestArchiveRecordFramesReplaysReassembledMessages(t *testing.T) {
	archive := openTestArchive(t)
	defer archive.Close()

	firstFrame, err := encodeFrame(frame{
		typ:           frameData,
		flags:         frameFlagFragment,
		streamID:      12,
		sessionID:     34,
		termID:        3,
		termOffset:    64,
		seq:           7,
		reserved:      99,
		fragmentIndex: 0,
		fragmentCount: 2,
		payload:       []byte("raw-"),
	})
	if err != nil {
		t.Fatal(err)
	}
	secondFrame, err := encodeFrame(frame{
		typ:           frameData,
		flags:         frameFlagFragment,
		streamID:      12,
		sessionID:     34,
		termID:        3,
		termOffset:    128,
		seq:           7,
		reserved:      99,
		fragmentIndex: 1,
		fragmentCount: 2,
		payload:       []byte("frames"),
	})
	if err != nil {
		t.Fatal(err)
	}

	records, err := archive.RecordFrames([][]byte{firstFrame, secondFrame})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || !bytes.Equal(records[0].RawFrame, firstFrame) || !bytes.Equal(records[1].RawFrame, secondFrame) {
		t.Fatalf("unexpected raw frame records: %#v", records)
	}
	if records[0].PayloadLength != len(firstFrame) || records[1].PayloadLength != len(secondFrame) {
		t.Fatalf("unexpected raw payload lengths: %#v", records)
	}

	report, err := archive.IntegrityScan()
	if err != nil {
		t.Fatal(err)
	}
	if report.Records != 2 {
		t.Fatalf("raw frame record count = %d, want 2", report.Records)
	}

	var replayed []Message
	err = archive.Replay(context.Background(), ArchiveReplayConfig{RecordingID: records[0].RecordingID}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 1 {
		t.Fatalf("replayed messages = %d, want 1: %#v", len(replayed), replayed)
	}
	msg := replayed[0]
	if msg.StreamID != 12 || msg.SessionID != 34 || msg.TermID != 3 || msg.TermOffset != 64 ||
		msg.Sequence != 7 || msg.ReservedValue != 99 || string(msg.Payload) != "raw-frames" {
		t.Fatalf("unexpected replayed raw frame message: %#v", msg)
	}

	replayed = nil
	err = archive.Replay(context.Background(), ArchiveReplayConfig{
		RecordingID:  records[0].RecordingID,
		FromPosition: records[1].Position,
	}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 0 {
		t.Fatalf("partial raw frame replay delivered a message: %#v", replayed)
	}
}

func TestArchiveRecordFrameRejectsMalformedFrame(t *testing.T) {
	archive := openTestArchive(t)
	defer archive.Close()

	if _, err := archive.RecordFrame([]byte("bad frame")); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("RecordFrame() err = %v, want %v", err, ErrInvalidConfig)
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
	recordings, err := archive.ListRecordings()
	if err != nil {
		t.Fatal(err)
	}
	if len(recordings) != 1 {
		t.Fatalf("recordings = %d, want 1", len(recordings))
	}
	entry, err := archive.readArchiveEntry(recordings[0], recordings[0].StartPosition)
	if err != nil {
		t.Fatal(err)
	}
	if !entry.rawFrame || len(entry.record.RawFrame) == 0 {
		t.Fatalf("subscription archive entry was not recorded as raw frame: %#v", entry.record)
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
	descriptors, err := archive.ListRecordings()
	if err != nil {
		t.Fatal(err)
	}
	if len(descriptors) != 0 {
		t.Fatalf("recordings were not purged: %#v", descriptors)
	}
}

func TestArchiveIntegrityScanDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive")
	archive, err := OpenArchive(ArchiveConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()

	record, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 1, Payload: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(archive.segmentPath(record.RecordingID, record.SegmentBase), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{'x'}, record.Position-record.SegmentBase+archiveHeaderLen); err != nil {
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

func TestArchiveRecordsAcrossSegmentFiles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	archive, err := OpenArchive(ArchiveConfig{
		Path:          dir,
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
	second, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 2, Payload: []byte("abcdefghijklmnopqrstuvwxyzabcdefghijklmn")})
	if err != nil {
		t.Fatal(err)
	}
	if first.SegmentBase != 0 || second.SegmentBase != 192 || second.Position != 192 {
		t.Fatalf("unexpected segment rollover: first=%#v second=%#v", first, second)
	}
	if _, err := os.Stat(archive.segmentPath(first.RecordingID, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(archive.segmentPath(second.RecordingID, 192)); err != nil {
		t.Fatal(err)
	}

	var replayed []Message
	if err := archive.Replay(context.Background(), ArchiveReplayConfig{RecordingID: first.RecordingID}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, msg)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 2 || replayed[0].Sequence != 1 || replayed[1].Sequence != 2 {
		t.Fatalf("unexpected replay across segments: %#v", replayed)
	}

	report, err := archive.IntegrityScan()
	if err != nil {
		t.Fatal(err)
	}
	if report.Recordings != 1 || report.Records != 2 || report.Bytes != second.NextPosition {
		t.Fatalf("unexpected segment scan report: %#v", report)
	}
}

func TestArchiveDetachAndAttachRecordingSegment(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	archive, err := OpenArchive(ArchiveConfig{
		Path:          dir,
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
	second, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 2, Payload: []byte("abcdefghijklmnopqrstuvwxyzabcdefghijklmn")})
	if err != nil {
		t.Fatal(err)
	}

	segments, err := archive.ListRecordingSegments(first.RecordingID)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 2 || segments[0].State != ArchiveSegmentAttached || segments[1].State != ArchiveSegmentAttached {
		t.Fatalf("unexpected attached segments: %#v", segments)
	}
	detached, err := archive.DetachRecordingSegment(first.RecordingID, first.SegmentBase)
	if err != nil {
		t.Fatal(err)
	}
	if detached.State != ArchiveSegmentDetached || detached.SegmentBase != first.SegmentBase {
		t.Fatalf("unexpected detached segment: %#v", detached)
	}
	if _, err := os.Stat(archive.segmentPath(first.RecordingID, first.SegmentBase)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("attached segment stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(archive.detachedSegmentPath(first.RecordingID, first.SegmentBase)); err != nil {
		t.Fatal(err)
	}

	err = archive.Replay(context.Background(), ArchiveReplayConfig{RecordingID: first.RecordingID}, func(context.Context, Message) error {
		return nil
	})
	if !errors.Is(err, ErrArchiveCorrupt) {
		t.Fatalf("Replay() err = %v, want %v", err, ErrArchiveCorrupt)
	}

	attached, err := archive.AttachRecordingSegment(first.RecordingID, first.SegmentBase)
	if err != nil {
		t.Fatal(err)
	}
	if attached.State != ArchiveSegmentAttached {
		t.Fatalf("unexpected attached segment: %#v", attached)
	}
	var replayed []Message
	if err := archive.Replay(context.Background(), ArchiveReplayConfig{RecordingID: first.RecordingID}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, msg)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 2 || replayed[0].Sequence != first.Message.Sequence || replayed[1].Sequence != second.Message.Sequence {
		t.Fatalf("unexpected replay after attach: %#v", replayed)
	}
	if _, err := archive.DetachRecordingSegment(first.RecordingID, second.SegmentBase); !errors.Is(err, ErrArchivePosition) {
		t.Fatalf("DetachRecordingSegment() err = %v, want %v", err, ErrArchivePosition)
	}
}

func TestArchiveMigrateAndDeleteDetachedRecordingSegments(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	archive, err := OpenArchive(ArchiveConfig{
		Path:          dir,
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
	if _, err := archive.DetachRecordingSegment(first.RecordingID, first.SegmentBase); err != nil {
		t.Fatal(err)
	}
	migrated, err := archive.MigrateDetachedRecordingSegment(first.RecordingID, first.SegmentBase, filepath.Join(t.TempDir(), "migrated"))
	if err != nil {
		t.Fatal(err)
	}
	if migrated.State != ArchiveSegmentDetached || migrated.Path == "" {
		t.Fatalf("unexpected migrated segment: %#v", migrated)
	}
	if _, err := os.Stat(migrated.Path); err != nil {
		t.Fatal(err)
	}
	segments, err := archive.ListRecordingSegments(first.RecordingID)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 2 || segments[0].State != ArchiveSegmentMissing {
		t.Fatalf("unexpected segments after migration: %#v", segments)
	}

	archive2, err := OpenArchive(ArchiveConfig{
		Path:          filepath.Join(t.TempDir(), "archive"),
		SegmentLength: 192,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer archive2.Close()
	first, err = archive2.Record(Message{StreamID: 1, SessionID: 1, Sequence: 1, Payload: []byte("0123456789012345678901234567890123456789")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := archive2.Record(Message{StreamID: 1, SessionID: 1, Sequence: 2, Payload: []byte("abcdefghijklmnopqrstuvwxyzabcdefghijklmn")}); err != nil {
		t.Fatal(err)
	}
	if _, err := archive2.DetachRecordingSegment(first.RecordingID, first.SegmentBase); err != nil {
		t.Fatal(err)
	}
	if err := archive2.DeleteDetachedRecordingSegment(first.RecordingID, first.SegmentBase); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(archive2.detachedSegmentPath(first.RecordingID, first.SegmentBase)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("detached segment stat err = %v, want not exist", err)
	}
}

func TestArchiveReopensCatalogAndExtendsRecording(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	archive, err := OpenArchive(ArchiveConfig{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	first, err := archive.Record(Message{StreamID: 7, SessionID: 8, Sequence: 1, Payload: []byte("first")})
	if err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenArchive(ArchiveConfig{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	descriptor, err := reopened.RecordingDescriptor(first.RecordingID)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.StopPosition != first.NextPosition {
		t.Fatalf("reopened descriptor stop = %d, want %d", descriptor.StopPosition, first.NextPosition)
	}

	second, err := reopened.Record(Message{StreamID: 7, SessionID: 8, Sequence: 2, Payload: []byte("second")})
	if err != nil {
		t.Fatal(err)
	}
	if second.RecordingID != first.RecordingID || second.Position != first.NextPosition {
		t.Fatalf("recording was not extended: first=%#v second=%#v", first, second)
	}

	var replayed []Message
	if err := reopened.Replay(context.Background(), ArchiveReplayConfig{RecordingID: first.RecordingID}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, msg)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 2 || string(replayed[0].Payload) != "first" || string(replayed[1].Payload) != "second" {
		t.Fatalf("unexpected replay after reopen: %#v", replayed)
	}
}

func TestArchiveRecordingEvents(t *testing.T) {
	var events []ArchiveRecordingEvent
	collect := func(event ArchiveRecordingEvent) {
		events = append(events, event)
	}
	archive, err := OpenArchive(ArchiveConfig{
		Path:                     filepath.Join(t.TempDir(), "archive"),
		RecordingProgressHandler: collect,
		RecordingSignalHandler:   collect,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()

	first, err := archive.Record(Message{StreamID: 1, SessionID: 2, Sequence: 10, Payload: []byte("first")})
	if err != nil {
		t.Fatal(err)
	}
	second, err := archive.Record(Message{StreamID: 1, SessionID: 2, Sequence: 11, Payload: []byte("second")})
	if err != nil {
		t.Fatal(err)
	}
	if err := archive.Truncate(second.Position); err != nil {
		t.Fatal(err)
	}
	if err := archive.Purge(); err != nil {
		t.Fatal(err)
	}

	wantSignals := []ArchiveRecordingSignal{
		ArchiveRecordingSignalStart,
		ArchiveRecordingSignalProgress,
		ArchiveRecordingSignalProgress,
		ArchiveRecordingSignalTruncate,
		ArchiveRecordingSignalPurge,
	}
	if len(events) != len(wantSignals) {
		t.Fatalf("events = %#v, want %d events", events, len(wantSignals))
	}
	for i, signal := range wantSignals {
		if events[i].Signal != signal {
			t.Fatalf("event %d signal = %q, want %q; events=%#v", i, events[i].Signal, signal, events)
		}
		if events[i].RecordingID != first.RecordingID {
			t.Fatalf("event %d recording id = %d, want %d", i, events[i].RecordingID, first.RecordingID)
		}
	}
	if events[1].Position != first.Position || events[1].NextPosition != first.NextPosition ||
		events[1].StreamID != 1 || events[1].SessionID != 2 || events[1].Sequence != 10 ||
		events[1].PayloadLength != len("first") {
		t.Fatalf("unexpected first progress event: %#v", events[1])
	}
	if events[2].Position != second.Position || events[2].NextPosition != second.NextPosition ||
		events[2].Descriptor.StopPosition != second.NextPosition {
		t.Fatalf("unexpected second progress event: %#v", events[2])
	}
	if events[3].Position != second.Position || events[3].Descriptor.StopPosition != second.Position {
		t.Fatalf("unexpected truncate event: %#v", events[3])
	}
	if events[4].Descriptor.StopPosition != second.Position {
		t.Fatalf("unexpected purge event: %#v", events[4])
	}
}

func TestArchiveRecordingExtendSignal(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	archive, err := OpenArchive(ArchiveConfig{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	first, err := archive.Record(Message{StreamID: 7, SessionID: 8, Sequence: 1, Payload: []byte("first")})
	if err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}

	var events []ArchiveRecordingEvent
	collect := func(event ArchiveRecordingEvent) {
		events = append(events, event)
	}
	reopened, err := OpenArchive(ArchiveConfig{
		Path:                     dir,
		RecordingProgressHandler: collect,
		RecordingSignalHandler:   collect,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	second, err := reopened.Record(Message{StreamID: 7, SessionID: 8, Sequence: 2, Payload: []byte("second")})
	if err != nil {
		t.Fatal(err)
	}
	wantSignals := []ArchiveRecordingSignal{
		ArchiveRecordingSignalExtend,
		ArchiveRecordingSignalProgress,
	}
	if len(events) != len(wantSignals) {
		t.Fatalf("events = %#v, want %d events", events, len(wantSignals))
	}
	for i, signal := range wantSignals {
		if events[i].Signal != signal {
			t.Fatalf("event %d signal = %q, want %q", i, events[i].Signal, signal)
		}
	}
	if events[0].RecordingID != first.RecordingID || events[0].Position != first.NextPosition ||
		events[0].NextPosition != second.NextPosition || events[0].Descriptor.StopPosition != second.NextPosition {
		t.Fatalf("unexpected extend event: %#v", events[0])
	}
}

func TestArchiveStartStopCreatesRecordingBoundaries(t *testing.T) {
	var events []ArchiveRecordingEvent
	collect := func(event ArchiveRecordingEvent) {
		events = append(events, event)
	}
	archive, err := OpenArchive(ArchiveConfig{
		Path:                     filepath.Join(t.TempDir(), "archive"),
		RecordingProgressHandler: collect,
		RecordingSignalHandler:   collect,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()

	firstDesc, err := archive.StartRecording(10, 20)
	if err != nil {
		t.Fatal(err)
	}
	first, err := archive.Record(Message{StreamID: 10, SessionID: 20, Sequence: 1, Payload: []byte("first")})
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := archive.StopRecording(firstDesc.RecordingID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := archive.Record(Message{StreamID: 10, SessionID: 20, Sequence: 2, Payload: []byte("second")})
	if err != nil {
		t.Fatal(err)
	}
	if first.RecordingID != firstDesc.RecordingID || stopped.StopPosition != first.NextPosition {
		t.Fatalf("unexpected first recording boundary: desc=%#v first=%#v stopped=%#v", firstDesc, first, stopped)
	}
	if second.RecordingID == first.RecordingID || second.Position != 0 {
		t.Fatalf("second record did not start a new recording: first=%#v second=%#v", first, second)
	}
	descriptors, err := archive.ListRecordings()
	if err != nil {
		t.Fatal(err)
	}
	if len(descriptors) != 2 || descriptors[0].RecordingID != first.RecordingID || descriptors[1].RecordingID != second.RecordingID {
		t.Fatalf("unexpected descriptors after start/stop: %#v", descriptors)
	}

	wantSignals := []ArchiveRecordingSignal{
		ArchiveRecordingSignalStart,
		ArchiveRecordingSignalProgress,
		ArchiveRecordingSignalStop,
		ArchiveRecordingSignalStart,
		ArchiveRecordingSignalProgress,
	}
	if len(events) != len(wantSignals) {
		t.Fatalf("events = %#v, want %d events", events, len(wantSignals))
	}
	for i, signal := range wantSignals {
		if events[i].Signal != signal {
			t.Fatalf("event %d signal = %q, want %q", i, events[i].Signal, signal)
		}
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
		Path: filepath.Join(t.TempDir(), "archive"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return archive
}
