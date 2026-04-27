package archive

import (
	"context"
	"github.com/xargin/bunshin/internal/core"
)

const (
	ArchiveControlActionAttachRecordingSegment  = core.ArchiveControlActionAttachRecordingSegment
	ArchiveControlActionCloseSession            = core.ArchiveControlActionCloseSession
	ArchiveControlActionDeleteRecordingSegment  = core.ArchiveControlActionDeleteRecordingSegment
	ArchiveControlActionDetachRecordingSegment  = core.ArchiveControlActionDetachRecordingSegment
	ArchiveControlActionIntegrityScan           = core.ArchiveControlActionIntegrityScan
	ArchiveControlActionListRecordingSegments   = core.ArchiveControlActionListRecordingSegments
	ArchiveControlActionListRecordings          = core.ArchiveControlActionListRecordings
	ArchiveControlActionMigrateRecordingSegment = core.ArchiveControlActionMigrateRecordingSegment
	ArchiveControlActionOpenSession             = core.ArchiveControlActionOpenSession
	ArchiveControlActionPurge                   = core.ArchiveControlActionPurge
	ArchiveControlActionQueryRecording          = core.ArchiveControlActionQueryRecording
	ArchiveControlActionReplay                  = core.ArchiveControlActionReplay
	ArchiveControlActionStartRecording          = core.ArchiveControlActionStartRecording
	ArchiveControlActionStopRecording           = core.ArchiveControlActionStopRecording
	ArchiveControlActionTruncateRecording       = core.ArchiveControlActionTruncateRecording
	ArchiveControlProtocolEvent                 = core.ArchiveControlProtocolEvent
	ArchiveControlProtocolRecordingEvent        = core.ArchiveControlProtocolRecordingEvent
	ArchiveControlProtocolReplayMessage         = core.ArchiveControlProtocolReplayMessage
	ArchiveControlProtocolRequest               = core.ArchiveControlProtocolRequest
	ArchiveControlProtocolResponse              = core.ArchiveControlProtocolResponse
	ArchiveControlProtocolVersion               = core.ArchiveControlProtocolVersion
	ArchiveRecordingSignalExtend                = core.ArchiveRecordingSignalExtend
	ArchiveRecordingSignalProgress              = core.ArchiveRecordingSignalProgress
	ArchiveRecordingSignalPurge                 = core.ArchiveRecordingSignalPurge
	ArchiveRecordingSignalStart                 = core.ArchiveRecordingSignalStart
	ArchiveRecordingSignalStop                  = core.ArchiveRecordingSignalStop
	ArchiveRecordingSignalTruncate              = core.ArchiveRecordingSignalTruncate
	ArchiveSegmentAttached                      = core.ArchiveSegmentAttached
	ArchiveSegmentDetached                      = core.ArchiveSegmentDetached
	ArchiveSegmentMissing                       = core.ArchiveSegmentMissing
)

type (
	Archive                           = core.Archive
	ArchiveConfig                     = core.ArchiveConfig
	ArchiveControlAction              = core.ArchiveControlAction
	ArchiveControlAuthorizer          = core.ArchiveControlAuthorizer
	ArchiveControlClient              = core.ArchiveControlClient
	ArchiveControlConfig              = core.ArchiveControlConfig
	ArchiveControlProtocolConfig      = core.ArchiveControlProtocolConfig
	ArchiveControlProtocolEventType   = core.ArchiveControlProtocolEventType
	ArchiveControlProtocolMessage     = core.ArchiveControlProtocolMessage
	ArchiveControlProtocolMessageType = core.ArchiveControlProtocolMessageType
	ArchiveControlProtocolServer      = core.ArchiveControlProtocolServer
	ArchiveControlServer              = core.ArchiveControlServer
	ArchiveDescription                = core.ArchiveDescription
	ArchiveIntegrityReport            = core.ArchiveIntegrityReport
	ArchiveLiveReplicationConfig      = core.ArchiveLiveReplicationConfig
	ArchiveMigrationReport            = core.ArchiveMigrationReport
	ArchiveRecord                     = core.ArchiveRecord
	ArchiveRecordingDescriptor        = core.ArchiveRecordingDescriptor
	ArchiveRecordingEvent             = core.ArchiveRecordingEvent
	ArchiveRecordingEventHandler      = core.ArchiveRecordingEventHandler
	ArchiveRecordingSignal            = core.ArchiveRecordingSignal
	ArchiveReplayConfig               = core.ArchiveReplayConfig
	ArchiveReplayMerge                = core.ArchiveReplayMerge
	ArchiveReplayMergeConfig          = core.ArchiveReplayMergeConfig
	ArchiveReplayMergeResult          = core.ArchiveReplayMergeResult
	ArchiveReplayResult               = core.ArchiveReplayResult
	ArchiveReplicationConfig          = core.ArchiveReplicationConfig
	ArchiveReplicationReport          = core.ArchiveReplicationReport
	ArchiveSegmentDescriptor          = core.ArchiveSegmentDescriptor
	ArchiveSegmentState               = core.ArchiveSegmentState
	ArchiveVerificationReport         = core.ArchiveVerificationReport
)

var (
	ErrArchiveClosed                     = core.ErrArchiveClosed
	ErrArchiveControlClosed              = core.ErrArchiveControlClosed
	ErrArchiveCorrupt                    = core.ErrArchiveCorrupt
	ErrArchivePosition                   = core.ErrArchivePosition
	ErrArchiveRecordingActive            = core.ErrArchiveRecordingActive
	ErrArchiveRecordingNotActive         = core.ErrArchiveRecordingNotActive
	ErrArchiveReplayMergeBufferFull      = core.ErrArchiveReplayMergeBufferFull
	ErrArchiveReplayMergeClosed          = core.ErrArchiveReplayMergeClosed
	ErrArchiveReplicationActiveRecording = core.ErrArchiveReplicationActiveRecording
)

func DescribeArchive(path string) (ArchiveDescription, error) {
	return core.DescribeArchive(path)
}

func ListenArchiveControlProtocol(ctx context.Context, archive *Archive, cfg ArchiveControlProtocolConfig) (*ArchiveControlProtocolServer, error) {
	return core.ListenArchiveControlProtocol(ctx, archive, cfg)
}

func MigrateArchive(srcPath, dstPath string) (ArchiveMigrationReport, error) {
	return core.MigrateArchive(srcPath, dstPath)
}

func OpenArchive(cfg ArchiveConfig) (*Archive, error) {
	return core.OpenArchive(cfg)
}

func ReplicateArchive(ctx context.Context, src, dst *Archive, cfg ArchiveReplicationConfig) (ArchiveReplicationReport, error) {
	return core.ReplicateArchive(ctx, src, dst, cfg)
}

func ReplicateArchiveLive(ctx context.Context, src, dst *Archive, cfg ArchiveLiveReplicationConfig) (ArchiveReplicationReport, error) {
	return core.ReplicateArchiveLive(ctx, src, dst, cfg)
}

func StartArchiveControlServer(archive *Archive, cfg ArchiveControlConfig) (*ArchiveControlServer, error) {
	return core.StartArchiveControlServer(archive, cfg)
}

func VerifyArchive(path string) (ArchiveVerificationReport, error) {
	return core.VerifyArchive(path)
}
