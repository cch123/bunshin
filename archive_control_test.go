package bunshin

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"
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

func TestArchiveControlProtocolSessionRequestsAndEvents(t *testing.T) {
	archive := openTestArchive(t)
	defer archive.Close()

	server, conn, encoder, decoder := startArchiveControlProtocolForTest(t, archive, ArchiveControlProtocolConfig{})
	defer server.Close()
	defer conn.Close()

	sessionID := openArchiveControlProtocolSessionForTest(t, conn, encoder, decoder)
	if sessionID == 0 {
		t.Fatal("protocol session id was not assigned")
	}

	if err := encoder.Encode(ArchiveControlProtocolMessage{
		Version:          ArchiveControlProtocolVersion,
		Type:             ArchiveControlProtocolRequest,
		CorrelationID:    2,
		ControlSessionID: sessionID,
		Action:           ArchiveControlActionStartRecording,
		StreamID:         10,
		StreamSessionID:  20,
	}); err != nil {
		t.Fatal(err)
	}
	startMessages := readArchiveControlProtocolMessagesForTest(t, conn, decoder, 2)
	startResponse := findArchiveControlProtocolResponseForTest(t, startMessages, 2)
	if startResponse.Error != "" || startResponse.Descriptor.RecordingID == 0 {
		t.Fatalf("unexpected start response: %#v", startResponse)
	}
	startEvent := findArchiveControlProtocolRecordingEventForTest(t, startMessages, ArchiveRecordingSignalStart)
	if startEvent.ControlSessionID != sessionID || startEvent.RecordingEvent.RecordingID != startResponse.Descriptor.RecordingID {
		t.Fatalf("unexpected start event: %#v", startEvent)
	}

	record, err := archive.Record(Message{StreamID: 10, SessionID: 20, Sequence: 1, Payload: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}
	progressEvent := readArchiveControlProtocolRecordingEventForTest(t, conn, decoder, ArchiveRecordingSignalProgress)
	if progressEvent.RecordingEvent.NextPosition != record.NextPosition || progressEvent.RecordingEvent.PayloadLength != len("one") {
		t.Fatalf("unexpected progress event: %#v", progressEvent)
	}

	if err := encoder.Encode(ArchiveControlProtocolMessage{
		Version:          ArchiveControlProtocolVersion,
		Type:             ArchiveControlProtocolRequest,
		CorrelationID:    3,
		ControlSessionID: sessionID,
		Action:           ArchiveControlActionListRecordings,
	}); err != nil {
		t.Fatal(err)
	}
	listResponse := readArchiveControlProtocolResponseForTest(t, conn, decoder, 3)
	if listResponse.Error != "" || len(listResponse.Recordings) != 1 || listResponse.Recordings[0].RecordingID != record.RecordingID {
		t.Fatalf("unexpected list response: %#v", listResponse)
	}

	if err := encoder.Encode(ArchiveControlProtocolMessage{
		Version:          ArchiveControlProtocolVersion,
		Type:             ArchiveControlProtocolRequest,
		CorrelationID:    4,
		ControlSessionID: sessionID,
		Action:           ArchiveControlActionReplay,
		Replay:           ArchiveReplayConfig{RecordingID: record.RecordingID},
	}); err != nil {
		t.Fatal(err)
	}
	replayMessages := readArchiveControlProtocolMessagesForTest(t, conn, decoder, 2)
	replayEvent := findArchiveControlProtocolReplayEventForTest(t, replayMessages, 4)
	if replayEvent.Message == nil || string(replayEvent.Message.Payload) != "one" || replayEvent.Message.Sequence != 1 {
		t.Fatalf("unexpected replay event: %#v", replayEvent)
	}
	replayResponse := findArchiveControlProtocolResponseForTest(t, replayMessages, 4)
	if replayResponse.Error != "" || replayResponse.ReplayResult.Records != 1 {
		t.Fatalf("unexpected replay response: %#v", replayResponse)
	}

	if err := encoder.Encode(ArchiveControlProtocolMessage{
		Version:          ArchiveControlProtocolVersion,
		Type:             ArchiveControlProtocolRequest,
		CorrelationID:    5,
		ControlSessionID: sessionID,
		Action:           ArchiveControlActionCloseSession,
	}); err != nil {
		t.Fatal(err)
	}
	closeResponse := readArchiveControlProtocolResponseForTest(t, conn, decoder, 5)
	if closeResponse.Error != "" {
		t.Fatalf("unexpected close response: %#v", closeResponse)
	}
}

func TestArchiveControlProtocolRejectsInvalidSession(t *testing.T) {
	archive := openTestArchive(t)
	defer archive.Close()

	server, conn, encoder, decoder := startArchiveControlProtocolForTest(t, archive, ArchiveControlProtocolConfig{})
	defer server.Close()
	defer conn.Close()

	sessionID := openArchiveControlProtocolSessionForTest(t, conn, encoder, decoder)
	if err := encoder.Encode(ArchiveControlProtocolMessage{
		Version:          ArchiveControlProtocolVersion,
		Type:             ArchiveControlProtocolRequest,
		CorrelationID:    2,
		ControlSessionID: sessionID + 1,
		Action:           ArchiveControlActionListRecordings,
	}); err != nil {
		t.Fatal(err)
	}
	response := readArchiveControlProtocolResponseForTest(t, conn, decoder, 2)
	if response.Error == "" {
		t.Fatalf("expected invalid session error, got %#v", response)
	}
}

func startArchiveControlProtocolForTest(t *testing.T, archive *Archive, cfg ArchiveControlProtocolConfig) (*ArchiveControlProtocolServer, net.Conn, *json.Encoder, *json.Decoder) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	server, err := ListenArchiveControlProtocol(ctx, archive, cfg)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial(server.Addr().Network(), server.Addr().String())
	if err != nil {
		_ = server.Close()
		t.Fatal(err)
	}
	return server, conn, json.NewEncoder(conn), json.NewDecoder(conn)
}

func openArchiveControlProtocolSessionForTest(t *testing.T, conn net.Conn, encoder *json.Encoder, decoder *json.Decoder) uint64 {
	t.Helper()
	if err := encoder.Encode(ArchiveControlProtocolMessage{
		Version:       ArchiveControlProtocolVersion,
		Type:          ArchiveControlProtocolRequest,
		CorrelationID: 1,
		Action:        ArchiveControlActionOpenSession,
	}); err != nil {
		t.Fatal(err)
	}
	response := readArchiveControlProtocolResponseForTest(t, conn, decoder, 1)
	if response.Error != "" {
		t.Fatalf("open session failed: %#v", response)
	}
	return response.ControlSessionID
}

func readArchiveControlProtocolResponseForTest(t *testing.T, conn net.Conn, decoder *json.Decoder, correlationID uint64) ArchiveControlProtocolMessage {
	t.Helper()
	for {
		msg := readArchiveControlProtocolMessageForTest(t, conn, decoder)
		if msg.Type == ArchiveControlProtocolResponse && msg.CorrelationID == correlationID {
			return msg
		}
	}
}

func readArchiveControlProtocolMessagesForTest(t *testing.T, conn net.Conn, decoder *json.Decoder, count int) []ArchiveControlProtocolMessage {
	t.Helper()
	messages := make([]ArchiveControlProtocolMessage, 0, count)
	for len(messages) < count {
		messages = append(messages, readArchiveControlProtocolMessageForTest(t, conn, decoder))
	}
	return messages
}

func findArchiveControlProtocolResponseForTest(t *testing.T, messages []ArchiveControlProtocolMessage, correlationID uint64) ArchiveControlProtocolMessage {
	t.Helper()
	for _, msg := range messages {
		if msg.Type == ArchiveControlProtocolResponse && msg.CorrelationID == correlationID {
			return msg
		}
	}
	t.Fatalf("response with correlation id %d not found in %#v", correlationID, messages)
	return ArchiveControlProtocolMessage{}
}

func findArchiveControlProtocolRecordingEventForTest(t *testing.T, messages []ArchiveControlProtocolMessage, signal ArchiveRecordingSignal) ArchiveControlProtocolMessage {
	t.Helper()
	for _, msg := range messages {
		if msg.Type == ArchiveControlProtocolEvent && msg.Event == ArchiveControlProtocolRecordingEvent &&
			msg.RecordingEvent != nil && msg.RecordingEvent.Signal == signal {
			return msg
		}
	}
	t.Fatalf("recording event %s not found in %#v", signal, messages)
	return ArchiveControlProtocolMessage{}
}

func findArchiveControlProtocolReplayEventForTest(t *testing.T, messages []ArchiveControlProtocolMessage, correlationID uint64) ArchiveControlProtocolMessage {
	t.Helper()
	for _, msg := range messages {
		if msg.Type == ArchiveControlProtocolEvent && msg.Event == ArchiveControlProtocolReplayMessage && msg.CorrelationID == correlationID {
			return msg
		}
	}
	t.Fatalf("replay event with correlation id %d not found in %#v", correlationID, messages)
	return ArchiveControlProtocolMessage{}
}

func readArchiveControlProtocolRecordingEventForTest(t *testing.T, conn net.Conn, decoder *json.Decoder, signal ArchiveRecordingSignal) ArchiveControlProtocolMessage {
	t.Helper()
	for {
		msg := readArchiveControlProtocolMessageForTest(t, conn, decoder)
		if msg.Type == ArchiveControlProtocolEvent && msg.Event == ArchiveControlProtocolRecordingEvent &&
			msg.RecordingEvent != nil && msg.RecordingEvent.Signal == signal {
			return msg
		}
	}
}

func readArchiveControlProtocolReplayEventForTest(t *testing.T, conn net.Conn, decoder *json.Decoder, correlationID uint64) ArchiveControlProtocolMessage {
	t.Helper()
	for {
		msg := readArchiveControlProtocolMessageForTest(t, conn, decoder)
		if msg.Type == ArchiveControlProtocolEvent && msg.Event == ArchiveControlProtocolReplayMessage && msg.CorrelationID == correlationID {
			return msg
		}
	}
}

func readArchiveControlProtocolMessageForTest(t *testing.T, conn net.Conn, decoder *json.Decoder) ArchiveControlProtocolMessage {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var msg ArchiveControlProtocolMessage
	if err := decoder.Decode(&msg); err != nil {
		t.Fatal(err)
	}
	return msg
}
