package bunshin

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestArchiveReplayMergeBuffersLiveUntilReplayCompletes(t *testing.T) {
	archive := openTestArchive(t)
	defer archive.Close()

	first, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 1, Payload: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}
	second, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 2, Payload: []byte("two")})
	if err != nil {
		t.Fatal(err)
	}

	var delivered []Message
	merge, err := archive.NewReplayMerge(ArchiveReplayMergeConfig{
		Replay: ArchiveReplayConfig{RecordingID: first.RecordingID},
	}, func(_ context.Context, msg Message) error {
		delivered = append(delivered, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	live := merge.LiveHandler()
	if err := live(context.Background(), Message{StreamID: 1, SessionID: 1, Sequence: 2, Payload: []byte("two-live-duplicate")}); err != nil {
		t.Fatal(err)
	}
	if err := live(context.Background(), Message{StreamID: 1, SessionID: 1, Sequence: 3, Payload: []byte("three")}); err != nil {
		t.Fatal(err)
	}
	if len(delivered) != 0 {
		t.Fatalf("live messages delivered before replay: %#v", delivered)
	}

	result, err := merge.Replay(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Replayed != 2 || result.Live != 1 || result.BufferedLive != 2 || result.DroppedLive != 1 {
		t.Fatalf("unexpected replay merge result: %#v", result)
	}
	if len(delivered) != 3 ||
		string(delivered[0].Payload) != "one" ||
		string(delivered[1].Payload) != "two" ||
		string(delivered[2].Payload) != "three" {
		t.Fatalf("unexpected replay merge delivery: %#v second=%#v", delivered, second)
	}
}

func TestArchiveReplayMergeDeliversLiveAfterReplay(t *testing.T) {
	archive := openTestArchive(t)
	defer archive.Close()

	record, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 1, Payload: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}

	var delivered []Message
	merge, err := archive.NewReplayMerge(ArchiveReplayMergeConfig{
		Replay: ArchiveReplayConfig{RecordingID: record.RecordingID},
	}, func(_ context.Context, msg Message) error {
		delivered = append(delivered, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := merge.Replay(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := merge.LiveHandler()(context.Background(), Message{StreamID: 1, SessionID: 1, Sequence: 2, Payload: []byte("two")}); err != nil {
		t.Fatal(err)
	}
	if len(delivered) != 2 || string(delivered[0].Payload) != "one" || string(delivered[1].Payload) != "two" {
		t.Fatalf("unexpected delivery after replay: %#v", delivered)
	}
	result := merge.Result()
	if result.Replayed != 1 || result.Live != 1 || result.DroppedLive != 0 {
		t.Fatalf("unexpected merge result: %#v", result)
	}
}

func TestArchiveReplayMergeRejectsLiveBufferOverflow(t *testing.T) {
	archive := openTestArchive(t)
	defer archive.Close()

	merge, err := archive.NewReplayMerge(ArchiveReplayMergeConfig{LiveBufferLimit: 1}, func(context.Context, Message) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	live := merge.LiveHandler()
	if err := live(context.Background(), Message{StreamID: 1, SessionID: 1, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	if err := live(context.Background(), Message{StreamID: 1, SessionID: 1, Sequence: 2}); !errors.Is(err, ErrArchiveReplayMergeBufferFull) {
		t.Fatalf("LiveHandler() err = %v, want %v", err, ErrArchiveReplayMergeBufferFull)
	}
}

func TestArchiveReopensStoppedRecordingAsInactive(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	archive, err := OpenArchive(ArchiveConfig{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	first, err := archive.Record(Message{StreamID: 1, SessionID: 1, Sequence: 1, Payload: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := archive.StopRecording(first.RecordingID); err != nil {
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
	second, err := reopened.Record(Message{StreamID: 1, SessionID: 1, Sequence: 2, Payload: []byte("two")})
	if err != nil {
		t.Fatal(err)
	}
	if second.RecordingID == first.RecordingID || second.Position != 0 {
		t.Fatalf("reopened archive appended to stopped recording: first=%#v second=%#v", first, second)
	}
	firstDesc, err := reopened.RecordingDescriptor(first.RecordingID)
	if err != nil {
		t.Fatal(err)
	}
	if firstDesc.StoppedAt == nil {
		t.Fatalf("stopped descriptor missing stopped timestamp: %#v", firstDesc)
	}
}
