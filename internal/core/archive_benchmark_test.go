package core

import (
	"context"
	"errors"
	"testing"
)

var errArchiveBenchmarkDone = errors.New("archive benchmark complete")

func BenchmarkArchiveReplay(b *testing.B) {
	archive, recordingID := openBenchmarkArchive(b, 4096)
	defer archive.Close()

	var replayed int
	handler := func(context.Context, Message) error {
		replayed++
		if replayed >= b.N {
			return errArchiveBenchmarkDone
		}
		return nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for replayed < b.N {
		err := archive.Replay(context.Background(), ArchiveReplayConfig{RecordingID: recordingID}, handler)
		if err != nil && !errors.Is(err, errArchiveBenchmarkDone) {
			b.Fatal(err)
		}
	}
}

func BenchmarkArchiveReplayMerge(b *testing.B) {
	archive, recordingID := openBenchmarkArchive(b, 4096)
	defer archive.Close()

	var delivered int
	handler := func(context.Context, Message) error {
		delivered++
		if delivered >= b.N {
			return errArchiveBenchmarkDone
		}
		return nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for delivered < b.N {
		merge, err := archive.NewReplayMerge(ArchiveReplayMergeConfig{
			Replay: ArchiveReplayConfig{RecordingID: recordingID},
		}, handler)
		if err != nil {
			b.Fatal(err)
		}
		_, err = merge.Replay(context.Background())
		if err != nil && !errors.Is(err, errArchiveBenchmarkDone) {
			b.Fatal(err)
		}
	}
}

func openBenchmarkArchive(b *testing.B, records int) (*Archive, int64) {
	b.Helper()

	archive, err := OpenArchive(ArchiveConfig{Path: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 256)
	var recordingID int64
	for i := 0; i < records; i++ {
		record, err := archive.Record(Message{
			StreamID:  170,
			SessionID: 270,
			Sequence:  uint64(i + 1),
			Payload:   payload,
		})
		if err != nil {
			_ = archive.Close()
			b.Fatal(err)
		}
		recordingID = record.RecordingID
	}
	if _, err := archive.StopRecording(recordingID); err != nil {
		_ = archive.Close()
		b.Fatal(err)
	}
	return archive, recordingID
}
