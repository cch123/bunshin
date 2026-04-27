package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

const (
	ArchiveControlProtocolVersion        = 1
	defaultArchiveControlProtocolNetwork = "tcp"
	defaultArchiveControlProtocolAddr    = "127.0.0.1:0"
	defaultArchiveControlProtocolEvents  = 64
)

type ArchiveControlProtocolMessageType string

const (
	ArchiveControlProtocolRequest  ArchiveControlProtocolMessageType = "request"
	ArchiveControlProtocolResponse ArchiveControlProtocolMessageType = "response"
	ArchiveControlProtocolEvent    ArchiveControlProtocolMessageType = "event"
)

type ArchiveControlProtocolEventType string

const (
	ArchiveControlProtocolRecordingEvent ArchiveControlProtocolEventType = "recording_event"
	ArchiveControlProtocolReplayMessage  ArchiveControlProtocolEventType = "replay_message"
)

type ArchiveControlProtocolConfig struct {
	Network       string
	Addr          string
	Listener      net.Listener
	CommandBuffer int
	EventBuffer   int
	Authorizer    ArchiveControlAuthorizer
}

type ArchiveControlProtocolServer struct {
	archive       *Archive
	control       *ArchiveControlServer
	listener      net.Listener
	eventBuffer   int
	nextSessionID atomic.Uint64

	done  chan struct{}
	once  sync.Once
	wg    sync.WaitGroup
	conns map[net.Conn]struct{}
	mu    sync.Mutex
}

type ArchiveControlProtocolMessage struct {
	Version          int                               `json:"version"`
	Type             ArchiveControlProtocolMessageType `json:"type"`
	Event            ArchiveControlProtocolEventType   `json:"event,omitempty"`
	CorrelationID    uint64                            `json:"correlation_id,omitempty"`
	ControlSessionID uint64                            `json:"control_session_id,omitempty"`
	Action           ArchiveControlAction              `json:"action,omitempty"`
	Error            string                            `json:"error,omitempty"`

	StreamID        uint32 `json:"stream_id,omitempty"`
	StreamSessionID uint32 `json:"stream_session_id,omitempty"`
	RecordingID     int64  `json:"recording_id,omitempty"`
	Position        int64  `json:"position,omitempty"`
	SegmentBase     int64  `json:"segment_base,omitempty"`
	DestinationPath string `json:"destination_path,omitempty"`

	Replay         ArchiveReplayConfig          `json:"replay,omitempty"`
	ReplayResult   ArchiveReplayResult          `json:"replay_result,omitempty"`
	Descriptor     ArchiveRecordingDescriptor   `json:"descriptor,omitempty"`
	Recordings     []ArchiveRecordingDescriptor `json:"recordings,omitempty"`
	Integrity      ArchiveIntegrityReport       `json:"integrity,omitempty"`
	Segments       []ArchiveSegmentDescriptor   `json:"segments,omitempty"`
	Segment        ArchiveSegmentDescriptor     `json:"segment,omitempty"`
	RecordingEvent *ArchiveRecordingEvent       `json:"recording_event,omitempty"`
	Message        *Message                     `json:"message,omitempty"`
}

func ListenArchiveControlProtocol(ctx context.Context, archive *Archive, cfg ArchiveControlProtocolConfig) (*ArchiveControlProtocolServer, error) {
	if archive == nil {
		return nil, invalidConfigf("archive is required")
	}
	if cfg.EventBuffer < 0 {
		return nil, invalidConfigf("invalid archive control protocol event buffer: %d", cfg.EventBuffer)
	}
	if cfg.EventBuffer == 0 {
		cfg.EventBuffer = defaultArchiveControlProtocolEvents
	}
	if ctx == nil {
		ctx = context.Background()
	}

	control, err := StartArchiveControlServer(archive, ArchiveControlConfig{
		CommandBuffer: cfg.CommandBuffer,
		Authorizer:    cfg.Authorizer,
	})
	if err != nil {
		return nil, err
	}

	listener := cfg.Listener
	if listener == nil {
		network := cfg.Network
		if network == "" {
			network = defaultArchiveControlProtocolNetwork
		}
		addr := cfg.Addr
		if addr == "" {
			addr = defaultArchiveControlProtocolAddr
		}
		listener, err = net.Listen(network, addr)
		if err != nil {
			_ = control.Close()
			return nil, fmt.Errorf("listen archive control protocol: %w", err)
		}
	}

	server := &ArchiveControlProtocolServer{
		archive:     archive,
		control:     control,
		listener:    listener,
		eventBuffer: cfg.EventBuffer,
		done:        make(chan struct{}),
		conns:       make(map[net.Conn]struct{}),
	}
	server.wg.Add(1)
	go server.acceptLoop(ctx)
	go func() {
		select {
		case <-ctx.Done():
			_ = server.Close()
		case <-server.done:
		}
	}()
	return server, nil
}

func (s *ArchiveControlProtocolServer) Addr() net.Addr {
	if s == nil || s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

func (s *ArchiveControlProtocolServer) Close() error {
	if s == nil {
		return nil
	}
	var err error
	s.once.Do(func() {
		close(s.done)
		if s.listener != nil {
			err = errors.Join(err, s.listener.Close())
		}
		s.mu.Lock()
		for conn := range s.conns {
			err = errors.Join(err, conn.Close())
		}
		s.mu.Unlock()
		err = errors.Join(err, s.control.Close())
		s.wg.Wait()
	})
	return err
}

func (s *ArchiveControlProtocolServer) acceptLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.done:
				return
			case <-ctx.Done():
				return
			default:
				return
			}
		}
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.serveConn(ctx, conn)
	}
}

func (s *ArchiveControlProtocolServer) serveConn(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		_ = conn.Close()
	}()

	events, unsubscribe, err := s.archive.SubscribeRecordingEvents(s.eventBuffer)
	if err != nil {
		_ = json.NewEncoder(conn).Encode(archiveControlProtocolError(0, 0, err))
		return
	}
	defer unsubscribe()

	sessionID := s.nextSessionID.Add(1)
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var sessionOpen atomic.Bool
	var writeMu sync.Mutex
	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	eventDone := make(chan struct{})
	go func() {
		defer close(eventDone)
		for {
			select {
			case <-sessionCtx.Done():
				return
			case event, ok := <-events:
				if !ok {
					return
				}
				if !sessionOpen.Load() {
					continue
				}
				msg := ArchiveControlProtocolMessage{
					Version:          ArchiveControlProtocolVersion,
					Type:             ArchiveControlProtocolEvent,
					Event:            ArchiveControlProtocolRecordingEvent,
					ControlSessionID: sessionID,
					RecordingEvent:   &event,
				}
				if err := writeArchiveControlProtocolMessage(encoder, &writeMu, msg); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		<-eventDone
	}()

	client := s.control.Client()
	for {
		var request ArchiveControlProtocolMessage
		if err := decoder.Decode(&request); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(sessionCtx.Err(), context.Canceled) {
				return
			}
			_ = writeArchiveControlProtocolMessage(encoder, &writeMu, archiveControlProtocolError(sessionID, request.CorrelationID, err))
			return
		}
		if request.Version != ArchiveControlProtocolVersion {
			_ = writeArchiveControlProtocolMessage(encoder, &writeMu, archiveControlProtocolError(sessionID, request.CorrelationID, fmt.Errorf("%w: unsupported archive control protocol version %d", ErrArchiveControlClosed, request.Version)))
			continue
		}
		if request.Type != ArchiveControlProtocolRequest {
			_ = writeArchiveControlProtocolMessage(encoder, &writeMu, archiveControlProtocolError(sessionID, request.CorrelationID, fmt.Errorf("%w: expected request message", ErrArchiveControlClosed)))
			continue
		}
		if request.CorrelationID == 0 {
			_ = writeArchiveControlProtocolMessage(encoder, &writeMu, archiveControlProtocolError(sessionID, 0, invalidConfigf("archive control protocol correlation id is required")))
			continue
		}
		if request.Action == ArchiveControlActionOpenSession {
			sessionOpen.Store(true)
			response := archiveControlProtocolResponse(sessionID, request.CorrelationID)
			if err := writeArchiveControlProtocolMessage(encoder, &writeMu, response); err != nil {
				return
			}
			continue
		}
		if !sessionOpen.Load() || request.ControlSessionID != sessionID {
			err := fmt.Errorf("%w: invalid control session %d", ErrArchiveControlClosed, request.ControlSessionID)
			_ = writeArchiveControlProtocolMessage(encoder, &writeMu, archiveControlProtocolError(sessionID, request.CorrelationID, err))
			continue
		}
		if request.Action == ArchiveControlActionCloseSession {
			response := archiveControlProtocolResponse(sessionID, request.CorrelationID)
			_ = writeArchiveControlProtocolMessage(encoder, &writeMu, response)
			return
		}

		response := s.handleProtocolRequest(sessionCtx, client, request, encoder, &writeMu)
		if err := writeArchiveControlProtocolMessage(encoder, &writeMu, response); err != nil {
			return
		}
	}
}

func (s *ArchiveControlProtocolServer) handleProtocolRequest(ctx context.Context, client *ArchiveControlClient, request ArchiveControlProtocolMessage, encoder *json.Encoder, writeMu *sync.Mutex) ArchiveControlProtocolMessage {
	response := archiveControlProtocolResponse(request.ControlSessionID, request.CorrelationID)
	switch request.Action {
	case ArchiveControlActionStartRecording:
		descriptor, err := client.StartRecording(ctx, request.StreamID, request.StreamSessionID)
		if err != nil {
			return archiveControlProtocolError(request.ControlSessionID, request.CorrelationID, err)
		}
		response.Descriptor = descriptor
	case ArchiveControlActionStopRecording:
		descriptor, err := client.StopRecording(ctx, request.RecordingID)
		if err != nil {
			return archiveControlProtocolError(request.ControlSessionID, request.CorrelationID, err)
		}
		response.Descriptor = descriptor
	case ArchiveControlActionListRecordings:
		recordings, err := client.ListRecordings(ctx)
		if err != nil {
			return archiveControlProtocolError(request.ControlSessionID, request.CorrelationID, err)
		}
		response.Recordings = recordings
	case ArchiveControlActionQueryRecording:
		descriptor, err := client.QueryRecording(ctx, request.RecordingID)
		if err != nil {
			return archiveControlProtocolError(request.ControlSessionID, request.CorrelationID, err)
		}
		response.Descriptor = descriptor
	case ArchiveControlActionReplay:
		result, err := client.Replay(ctx, request.Replay, func(_ context.Context, msg Message) error {
			event := ArchiveControlProtocolMessage{
				Version:          ArchiveControlProtocolVersion,
				Type:             ArchiveControlProtocolEvent,
				Event:            ArchiveControlProtocolReplayMessage,
				CorrelationID:    request.CorrelationID,
				ControlSessionID: request.ControlSessionID,
				Message:          &msg,
			}
			return writeArchiveControlProtocolMessage(encoder, writeMu, event)
		})
		if err != nil {
			return archiveControlProtocolError(request.ControlSessionID, request.CorrelationID, err)
		}
		response.ReplayResult = result
	case ArchiveControlActionTruncateRecording:
		if err := client.TruncateRecording(ctx, request.RecordingID, request.Position); err != nil {
			return archiveControlProtocolError(request.ControlSessionID, request.CorrelationID, err)
		}
	case ArchiveControlActionPurge:
		if err := client.Purge(ctx); err != nil {
			return archiveControlProtocolError(request.ControlSessionID, request.CorrelationID, err)
		}
	case ArchiveControlActionIntegrityScan:
		integrity, err := client.IntegrityScan(ctx)
		if err != nil {
			return archiveControlProtocolError(request.ControlSessionID, request.CorrelationID, err)
		}
		response.Integrity = integrity
	case ArchiveControlActionListRecordingSegments:
		segments, err := client.ListRecordingSegments(ctx, request.RecordingID)
		if err != nil {
			return archiveControlProtocolError(request.ControlSessionID, request.CorrelationID, err)
		}
		response.Segments = segments
	case ArchiveControlActionDetachRecordingSegment:
		segment, err := client.DetachRecordingSegment(ctx, request.RecordingID, request.SegmentBase)
		if err != nil {
			return archiveControlProtocolError(request.ControlSessionID, request.CorrelationID, err)
		}
		response.Segment = segment
	case ArchiveControlActionAttachRecordingSegment:
		segment, err := client.AttachRecordingSegment(ctx, request.RecordingID, request.SegmentBase)
		if err != nil {
			return archiveControlProtocolError(request.ControlSessionID, request.CorrelationID, err)
		}
		response.Segment = segment
	case ArchiveControlActionDeleteRecordingSegment:
		if err := client.DeleteDetachedRecordingSegment(ctx, request.RecordingID, request.SegmentBase); err != nil {
			return archiveControlProtocolError(request.ControlSessionID, request.CorrelationID, err)
		}
	case ArchiveControlActionMigrateRecordingSegment:
		segment, err := client.MigrateDetachedRecordingSegment(ctx, request.RecordingID, request.SegmentBase, request.DestinationPath)
		if err != nil {
			return archiveControlProtocolError(request.ControlSessionID, request.CorrelationID, err)
		}
		response.Segment = segment
	default:
		return archiveControlProtocolError(request.ControlSessionID, request.CorrelationID, invalidConfigf("unsupported archive control protocol action: %s", request.Action))
	}
	return response
}

func archiveControlProtocolResponse(sessionID, correlationID uint64) ArchiveControlProtocolMessage {
	return ArchiveControlProtocolMessage{
		Version:          ArchiveControlProtocolVersion,
		Type:             ArchiveControlProtocolResponse,
		CorrelationID:    correlationID,
		ControlSessionID: sessionID,
	}
}

func archiveControlProtocolError(sessionID, correlationID uint64, err error) ArchiveControlProtocolMessage {
	response := archiveControlProtocolResponse(sessionID, correlationID)
	if err != nil {
		response.Error = err.Error()
	}
	return response
}

func writeArchiveControlProtocolMessage(encoder *json.Encoder, mu *sync.Mutex, msg ArchiveControlProtocolMessage) error {
	mu.Lock()
	defer mu.Unlock()
	return encoder.Encode(msg)
}
