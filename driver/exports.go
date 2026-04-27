package driver

import (
	"context"
	"github.com/xargin/bunshin/internal/core"
	"time"
)

const (
	DriverDirectoryStatusActive       = core.DriverDirectoryStatusActive
	DriverDirectoryStatusClosed       = core.DriverDirectoryStatusClosed
	DriverIPCCommandAddPublication    = core.DriverIPCCommandAddPublication
	DriverIPCCommandAddSubscription   = core.DriverIPCCommandAddSubscription
	DriverIPCCommandCloseClient       = core.DriverIPCCommandCloseClient
	DriverIPCCommandClosePublication  = core.DriverIPCCommandClosePublication
	DriverIPCCommandCloseSubscription = core.DriverIPCCommandCloseSubscription
	DriverIPCCommandFlushReports      = core.DriverIPCCommandFlushReports
	DriverIPCCommandHeartbeatClient   = core.DriverIPCCommandHeartbeatClient
	DriverIPCCommandOpenClient        = core.DriverIPCCommandOpenClient
	DriverIPCCommandPollSubscription  = core.DriverIPCCommandPollSubscription
	DriverIPCCommandSendPublication   = core.DriverIPCCommandSendPublication
	DriverIPCCommandSnapshot          = core.DriverIPCCommandSnapshot
	DriverIPCCommandTerminate         = core.DriverIPCCommandTerminate
	DriverIPCEventClientClosed        = core.DriverIPCEventClientClosed
	DriverIPCEventClientHeartbeat     = core.DriverIPCEventClientHeartbeat
	DriverIPCEventClientOpened        = core.DriverIPCEventClientOpened
	DriverIPCEventCommandError        = core.DriverIPCEventCommandError
	DriverIPCEventPublicationAdded    = core.DriverIPCEventPublicationAdded
	DriverIPCEventPublicationClosed   = core.DriverIPCEventPublicationClosed
	DriverIPCEventPublicationSent     = core.DriverIPCEventPublicationSent
	DriverIPCEventReportsFlushed      = core.DriverIPCEventReportsFlushed
	DriverIPCEventSnapshot            = core.DriverIPCEventSnapshot
	DriverIPCEventSubscriptionAdded   = core.DriverIPCEventSubscriptionAdded
	DriverIPCEventSubscriptionClosed  = core.DriverIPCEventSubscriptionClosed
	DriverIPCEventSubscriptionPolled  = core.DriverIPCEventSubscriptionPolled
	DriverIPCEventTerminated          = core.DriverIPCEventTerminated
	DriverIPCProtocolVersion          = core.DriverIPCProtocolVersion
	DriverThreadingDedicated          = core.DriverThreadingDedicated
	DriverThreadingShared             = core.DriverThreadingShared
)

type (
	DriverClient                      = core.DriverClient
	DriverClientID                    = core.DriverClientID
	DriverClientSnapshot              = core.DriverClientSnapshot
	DriverConfig                      = core.DriverConfig
	DriverConnectionConfig            = core.DriverConnectionConfig
	DriverCounters                    = core.DriverCounters
	DriverCountersFile                = core.DriverCountersFile
	DriverDirectoryLayout             = core.DriverDirectoryLayout
	DriverDirectoryReport             = core.DriverDirectoryReport
	DriverDirectoryStatus             = core.DriverDirectoryStatus
	DriverErrorReport                 = core.DriverErrorReport
	DriverErrorReportFile             = core.DriverErrorReportFile
	DriverIPC                         = core.DriverIPC
	DriverIPCCommand                  = core.DriverIPCCommand
	DriverIPCCommandHandler           = core.DriverIPCCommandHandler
	DriverIPCCommandType              = core.DriverIPCCommandType
	DriverIPCConfig                   = core.DriverIPCConfig
	DriverIPCEvent                    = core.DriverIPCEvent
	DriverIPCEventHandler             = core.DriverIPCEventHandler
	DriverIPCEventType                = core.DriverIPCEventType
	DriverIPCMessage                  = core.DriverIPCMessage
	DriverIPCPublicationConfig        = core.DriverIPCPublicationConfig
	DriverIPCServer                   = core.DriverIPCServer
	DriverIPCSubscriptionConfig       = core.DriverIPCSubscriptionConfig
	DriverImageSnapshot               = core.DriverImageSnapshot
	DriverLossReportFile              = core.DriverLossReportFile
	DriverLossReportSnapshot          = core.DriverLossReportSnapshot
	DriverMarkFile                    = core.DriverMarkFile
	DriverProcessConfig               = core.DriverProcessConfig
	DriverProcessStatus               = core.DriverProcessStatus
	DriverPublication                 = core.DriverPublication
	DriverPublicationSnapshot         = core.DriverPublicationSnapshot
	DriverResourceID                  = core.DriverResourceID
	DriverRingsReportFile             = core.DriverRingsReportFile
	DriverSnapshot                    = core.DriverSnapshot
	DriverStatusCounters              = core.DriverStatusCounters
	DriverStreamsReportFile           = core.DriverStreamsReportFile
	DriverSubscription                = core.DriverSubscription
	DriverSubscriptionDataImageStatus = core.DriverSubscriptionDataImageStatus
	DriverSubscriptionDataRingStatus  = core.DriverSubscriptionDataRingStatus
	DriverSubscriptionImage           = core.DriverSubscriptionImage
	DriverSubscriptionImageConfig     = core.DriverSubscriptionImageConfig
	DriverSubscriptionImageSnapshot   = core.DriverSubscriptionImageSnapshot
	DriverSubscriptionRingSnapshot    = core.DriverSubscriptionRingSnapshot
	DriverSubscriptionSnapshot        = core.DriverSubscriptionSnapshot
	DriverThreadingMode               = core.DriverThreadingMode
	MediaDriver                       = core.MediaDriver
)

var (
	ErrDriverClientClosed         = core.ErrDriverClientClosed
	ErrDriverClosed               = core.ErrDriverClosed
	ErrDriverDirectoryActive      = core.ErrDriverDirectoryActive
	ErrDriverDirectoryUnavailable = core.ErrDriverDirectoryUnavailable
	ErrDriverExternalUnsupported  = core.ErrDriverExternalUnsupported
	ErrDriverIPCClosed            = core.ErrDriverIPCClosed
	ErrDriverIPCProtocol          = core.ErrDriverIPCProtocol
	ErrDriverProcessUnavailable   = core.ErrDriverProcessUnavailable
	ErrDriverResourceNotFound     = core.ErrDriverResourceNotFound
)

func BuildDriverRingsReport(snapshot DriverSnapshot, now time.Time) DriverRingsReportFile {
	return core.BuildDriverRingsReport(snapshot, now)
}

func CheckDriverProcess(directory string, staleTimeout time.Duration) (DriverProcessStatus, error) {
	return core.CheckDriverProcess(directory, staleTimeout)
}

func ConnectMediaDriver(ctx context.Context, cfg DriverConnectionConfig) (*DriverClient, error) {
	return core.ConnectMediaDriver(ctx, cfg)
}

func NewDriverIPCServer(driver *MediaDriver, ipc *DriverIPC) (*DriverIPCServer, error) {
	return core.NewDriverIPCServer(driver, ipc)
}

func OpenDriverIPC(cfg DriverIPCConfig) (*DriverIPC, error) {
	return core.OpenDriverIPC(cfg)
}

func OpenDriverSubscriptionImage(cfg DriverSubscriptionImageConfig) (*DriverSubscriptionImage, error) {
	return core.OpenDriverSubscriptionImage(cfg)
}

func ReadDriverMarkFile(directory string) (DriverMarkFile, error) {
	return core.ReadDriverMarkFile(directory)
}

func ResolveDriverDirectoryLayout(directory string) (DriverDirectoryLayout, error) {
	return core.ResolveDriverDirectoryLayout(directory)
}

func RunMediaDriverProcess(ctx context.Context, cfg DriverProcessConfig) error {
	return core.RunMediaDriverProcess(ctx, cfg)
}

func StartMediaDriver(cfg DriverConfig) (*MediaDriver, error) {
	return core.StartMediaDriver(cfg)
}

func TerminateDriverProcess(ctx context.Context, directory string) (DriverIPCEvent, error) {
	return core.TerminateDriverProcess(ctx, directory)
}
