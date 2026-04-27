package core

import (
	"context"
	"errors"
	"sync"
)

const defaultArchiveControlBuffer = 64

var ErrArchiveControlClosed = errors.New("bunshin archive control: closed")

type ArchiveControlConfig struct {
	CommandBuffer int
	Authorizer    ArchiveControlAuthorizer
}

type ArchiveControlServer struct {
	archive    *Archive
	commands   chan archiveControlCommand
	done       chan struct{}
	authorizer ArchiveControlAuthorizer
	once       sync.Once
}

type ArchiveControlClient struct {
	server *ArchiveControlServer
}

type ArchiveReplayResult struct {
	Records uint64
}

type ArchiveControlAction string

const (
	ArchiveControlActionOpenSession             ArchiveControlAction = "open_session"
	ArchiveControlActionCloseSession            ArchiveControlAction = "close_session"
	ArchiveControlActionStartRecording          ArchiveControlAction = "start_recording"
	ArchiveControlActionStopRecording           ArchiveControlAction = "stop_recording"
	ArchiveControlActionListRecordings          ArchiveControlAction = "list_recordings"
	ArchiveControlActionQueryRecording          ArchiveControlAction = "query_recording"
	ArchiveControlActionReplay                  ArchiveControlAction = "replay"
	ArchiveControlActionTruncateRecording       ArchiveControlAction = "truncate_recording"
	ArchiveControlActionPurge                   ArchiveControlAction = "purge"
	ArchiveControlActionIntegrityScan           ArchiveControlAction = "integrity_scan"
	ArchiveControlActionListRecordingSegments   ArchiveControlAction = "list_recording_segments"
	ArchiveControlActionDetachRecordingSegment  ArchiveControlAction = "detach_recording_segment"
	ArchiveControlActionAttachRecordingSegment  ArchiveControlAction = "attach_recording_segment"
	ArchiveControlActionDeleteRecordingSegment  ArchiveControlAction = "delete_recording_segment"
	ArchiveControlActionMigrateRecordingSegment ArchiveControlAction = "migrate_recording_segment"
)

type ArchiveControlAuthorizer func(context.Context, ArchiveControlAction) error

type archiveControlCommand struct {
	ctx    context.Context
	action ArchiveControlAction
	apply  func() (any, error)
	reply  chan archiveControlResult
}

type archiveControlResult struct {
	value any
	err   error
}

func StartArchiveControlServer(archive *Archive, cfg ArchiveControlConfig) (*ArchiveControlServer, error) {
	if archive == nil {
		return nil, invalidConfigf("archive is required")
	}
	if cfg.CommandBuffer < 0 {
		return nil, invalidConfigf("invalid archive control command buffer: %d", cfg.CommandBuffer)
	}
	if cfg.CommandBuffer == 0 {
		cfg.CommandBuffer = defaultArchiveControlBuffer
	}
	server := &ArchiveControlServer{
		archive:    archive,
		commands:   make(chan archiveControlCommand, cfg.CommandBuffer),
		done:       make(chan struct{}),
		authorizer: cfg.Authorizer,
	}
	go server.run()
	return server, nil
}

func (s *ArchiveControlServer) Client() *ArchiveControlClient {
	return &ArchiveControlClient{server: s}
}

func (s *ArchiveControlServer) Close() error {
	s.once.Do(func() {
		close(s.done)
	})
	return nil
}

func (s *ArchiveControlServer) run() {
	for {
		select {
		case <-s.done:
			return
		case command := <-s.commands:
			if s.authorizer != nil {
				if err := s.authorizer(command.ctx, command.action); err != nil {
					command.reply <- archiveControlResult{err: err}
					continue
				}
			}
			value, err := command.apply()
			command.reply <- archiveControlResult{value: value, err: err}
		}
	}
}

func (c *ArchiveControlClient) StartRecording(ctx context.Context, streamID, sessionID uint32) (ArchiveRecordingDescriptor, error) {
	value, err := c.dispatch(ctx, ArchiveControlActionStartRecording, func() (any, error) {
		return c.server.archive.StartRecording(streamID, sessionID)
	})
	if err != nil {
		return ArchiveRecordingDescriptor{}, err
	}
	return value.(ArchiveRecordingDescriptor), nil
}

func (c *ArchiveControlClient) StopRecording(ctx context.Context, recordingID int64) (ArchiveRecordingDescriptor, error) {
	value, err := c.dispatch(ctx, ArchiveControlActionStopRecording, func() (any, error) {
		return c.server.archive.StopRecording(recordingID)
	})
	if err != nil {
		return ArchiveRecordingDescriptor{}, err
	}
	return value.(ArchiveRecordingDescriptor), nil
}

func (c *ArchiveControlClient) ListRecordings(ctx context.Context) ([]ArchiveRecordingDescriptor, error) {
	value, err := c.dispatch(ctx, ArchiveControlActionListRecordings, func() (any, error) {
		return c.server.archive.ListRecordings()
	})
	if err != nil {
		return nil, err
	}
	return value.([]ArchiveRecordingDescriptor), nil
}

func (c *ArchiveControlClient) QueryRecording(ctx context.Context, recordingID int64) (ArchiveRecordingDescriptor, error) {
	return c.RecordingDescriptor(ctx, recordingID)
}

func (c *ArchiveControlClient) RecordingDescriptor(ctx context.Context, recordingID int64) (ArchiveRecordingDescriptor, error) {
	value, err := c.dispatch(ctx, ArchiveControlActionQueryRecording, func() (any, error) {
		return c.server.archive.RecordingDescriptor(recordingID)
	})
	if err != nil {
		return ArchiveRecordingDescriptor{}, err
	}
	return value.(ArchiveRecordingDescriptor), nil
}

func (c *ArchiveControlClient) Replay(ctx context.Context, cfg ArchiveReplayConfig, handler Handler) (ArchiveReplayResult, error) {
	value, err := c.dispatch(ctx, ArchiveControlActionReplay, func() (any, error) {
		if handler == nil {
			return ArchiveReplayResult{}, errors.New("handler is required")
		}
		var result ArchiveReplayResult
		err := c.server.archive.Replay(ctx, cfg, func(ctx context.Context, msg Message) error {
			result.Records++
			return handler(ctx, msg)
		})
		return result, err
	})
	if err != nil {
		return ArchiveReplayResult{}, err
	}
	return value.(ArchiveReplayResult), nil
}

func (c *ArchiveControlClient) TruncateRecording(ctx context.Context, recordingID, position int64) error {
	_, err := c.dispatch(ctx, ArchiveControlActionTruncateRecording, func() (any, error) {
		return nil, c.server.archive.TruncateRecording(recordingID, position)
	})
	return err
}

func (c *ArchiveControlClient) Purge(ctx context.Context) error {
	_, err := c.dispatch(ctx, ArchiveControlActionPurge, func() (any, error) {
		return nil, c.server.archive.Purge()
	})
	return err
}

func (c *ArchiveControlClient) IntegrityScan(ctx context.Context) (ArchiveIntegrityReport, error) {
	value, err := c.dispatch(ctx, ArchiveControlActionIntegrityScan, func() (any, error) {
		return c.server.archive.IntegrityScan()
	})
	if err != nil {
		return ArchiveIntegrityReport{}, err
	}
	return value.(ArchiveIntegrityReport), nil
}

func (c *ArchiveControlClient) ListRecordingSegments(ctx context.Context, recordingID int64) ([]ArchiveSegmentDescriptor, error) {
	value, err := c.dispatch(ctx, ArchiveControlActionListRecordingSegments, func() (any, error) {
		return c.server.archive.ListRecordingSegments(recordingID)
	})
	if err != nil {
		return nil, err
	}
	return value.([]ArchiveSegmentDescriptor), nil
}

func (c *ArchiveControlClient) DetachRecordingSegment(ctx context.Context, recordingID, segmentBase int64) (ArchiveSegmentDescriptor, error) {
	value, err := c.dispatch(ctx, ArchiveControlActionDetachRecordingSegment, func() (any, error) {
		return c.server.archive.DetachRecordingSegment(recordingID, segmentBase)
	})
	if err != nil {
		return ArchiveSegmentDescriptor{}, err
	}
	return value.(ArchiveSegmentDescriptor), nil
}

func (c *ArchiveControlClient) AttachRecordingSegment(ctx context.Context, recordingID, segmentBase int64) (ArchiveSegmentDescriptor, error) {
	value, err := c.dispatch(ctx, ArchiveControlActionAttachRecordingSegment, func() (any, error) {
		return c.server.archive.AttachRecordingSegment(recordingID, segmentBase)
	})
	if err != nil {
		return ArchiveSegmentDescriptor{}, err
	}
	return value.(ArchiveSegmentDescriptor), nil
}

func (c *ArchiveControlClient) DeleteDetachedRecordingSegment(ctx context.Context, recordingID, segmentBase int64) error {
	_, err := c.dispatch(ctx, ArchiveControlActionDeleteRecordingSegment, func() (any, error) {
		return nil, c.server.archive.DeleteDetachedRecordingSegment(recordingID, segmentBase)
	})
	return err
}

func (c *ArchiveControlClient) MigrateDetachedRecordingSegment(ctx context.Context, recordingID, segmentBase int64, dstDir string) (ArchiveSegmentDescriptor, error) {
	value, err := c.dispatch(ctx, ArchiveControlActionMigrateRecordingSegment, func() (any, error) {
		return c.server.archive.MigrateDetachedRecordingSegment(recordingID, segmentBase, dstDir)
	})
	if err != nil {
		return ArchiveSegmentDescriptor{}, err
	}
	return value.(ArchiveSegmentDescriptor), nil
}

func (c *ArchiveControlClient) dispatch(ctx context.Context, action ArchiveControlAction, apply func() (any, error)) (any, error) {
	if c == nil || c.server == nil {
		return nil, ErrArchiveControlClosed
	}
	reply := make(chan archiveControlResult, 1)
	command := archiveControlCommand{
		ctx:    ctx,
		action: action,
		apply:  apply,
		reply:  reply,
	}
	select {
	case c.server.commands <- command:
	case <-c.server.done:
		return nil, ErrArchiveControlClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case result := <-reply:
		return result.value, result.err
	case <-c.server.done:
		return nil, ErrArchiveControlClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
