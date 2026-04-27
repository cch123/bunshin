package cluster

import (
	"context"
	"github.com/xargin/bunshin/internal/core"
)

const (
	ClusterAuthorizationActionCancelTimer    = core.ClusterAuthorizationActionCancelTimer
	ClusterAuthorizationActionIngress        = core.ClusterAuthorizationActionIngress
	ClusterAuthorizationActionOpenSession    = core.ClusterAuthorizationActionOpenSession
	ClusterAuthorizationActionScheduleTimer  = core.ClusterAuthorizationActionScheduleTimer
	ClusterAuthorizationActionServiceMessage = core.ClusterAuthorizationActionServiceMessage
	ClusterAuthorizationActionSnapshot       = core.ClusterAuthorizationActionSnapshot
	ClusterControlActionDescribe             = core.ClusterControlActionDescribe
	ClusterControlActionResume               = core.ClusterControlActionResume
	ClusterControlActionShutdown             = core.ClusterControlActionShutdown
	ClusterControlActionSnapshot             = core.ClusterControlActionSnapshot
	ClusterControlActionSuspend              = core.ClusterControlActionSuspend
	ClusterControlActionValidate             = core.ClusterControlActionValidate
	ClusterLogEntryIngress                   = core.ClusterLogEntryIngress
	ClusterLogEntryServiceMessage            = core.ClusterLogEntryServiceMessage
	ClusterLogEntryTimerCancel               = core.ClusterLogEntryTimerCancel
	ClusterLogEntryTimerFire                 = core.ClusterLogEntryTimerFire
	ClusterLogEntryTimerSchedule             = core.ClusterLogEntryTimerSchedule
	ClusterMemberActionAppendLog             = core.ClusterMemberActionAppendLog
	ClusterMemberActionAppendLogAck          = core.ClusterMemberActionAppendLogAck
	ClusterMemberActionAppendLogBatchAck     = core.ClusterMemberActionAppendLogBatchAck
	ClusterMemberActionDescribe              = core.ClusterMemberActionDescribe
	ClusterMemberActionLastPosition          = core.ClusterMemberActionLastPosition
	ClusterMemberActionLoadSnapshot          = core.ClusterMemberActionLoadSnapshot
	ClusterMemberActionSaveSnapshot          = core.ClusterMemberActionSaveSnapshot
	ClusterMemberActionSnapshotLog           = core.ClusterMemberActionSnapshotLog
	ClusterMemberActionSubmitIngress         = core.ClusterMemberActionSubmitIngress
	ClusterMemberActionSubmitIngressBatch    = core.ClusterMemberActionSubmitIngressBatch
	ClusterMemberActionTakeSnapshot          = core.ClusterMemberActionTakeSnapshot
	ClusterMemberProtocolRequest             = core.ClusterMemberProtocolRequest
	ClusterMemberProtocolResponse            = core.ClusterMemberProtocolResponse
	ClusterMemberProtocolVersion             = core.ClusterMemberProtocolVersion
	ClusterMembershipActionAddStandby        = core.ClusterMembershipActionAddStandby
	ClusterMembershipActionCatchUp           = core.ClusterMembershipActionCatchUp
	ClusterMembershipActionDemoteVoter       = core.ClusterMembershipActionDemoteVoter
	ClusterMembershipActionPromoteVoter      = core.ClusterMembershipActionPromoteVoter
	ClusterMembershipActionRemoveMember      = core.ClusterMembershipActionRemoveMember
	ClusterMembershipActionTransferLeader    = core.ClusterMembershipActionTransferLeader
	ClusterMembershipActionUpgradeMember     = core.ClusterMembershipActionUpgradeMember
	ClusterMembershipActionValidate          = core.ClusterMembershipActionValidate
	ClusterModeAppointedLeader               = core.ClusterModeAppointedLeader
	ClusterModeLearner                       = core.ClusterModeLearner
	ClusterModeSingleNode                    = core.ClusterModeSingleNode
	ClusterRoleBackup                        = core.ClusterRoleBackup
	ClusterRoleFollower                      = core.ClusterRoleFollower
	ClusterRoleLeader                        = core.ClusterRoleLeader
	ClusterRoleLearner                       = core.ClusterRoleLearner
	ClusterRoleStandby                       = core.ClusterRoleStandby
	DefaultClusterClientMaxBatchDelay        = core.DefaultClusterClientMaxBatchDelay
	DefaultClusterClientMaxBatchSize         = core.DefaultClusterClientMaxBatchSize
)

type (
	Archive                          = core.Archive
	ArchiveClusterLog                = core.ArchiveClusterLog
	ArchiveClusterSnapshotStore      = core.ArchiveClusterSnapshotStore
	ClusterAuthenticationRequest     = core.ClusterAuthenticationRequest
	ClusterAuthenticator             = core.ClusterAuthenticator
	ClusterAuthorizationAction       = core.ClusterAuthorizationAction
	ClusterAuthorizationRequest      = core.ClusterAuthorizationRequest
	ClusterAuthorizer                = core.ClusterAuthorizer
	ClusterBackup                    = core.ClusterBackup
	ClusterBackupConfig              = core.ClusterBackupConfig
	ClusterBackupStatus              = core.ClusterBackupStatus
	ClusterBackupSyncResult          = core.ClusterBackupSyncResult
	ClusterClient                    = core.ClusterClient
	ClusterClientConfig              = core.ClusterClientConfig
	ClusterConfig                    = core.ClusterConfig
	ClusterControlAction             = core.ClusterControlAction
	ClusterControlAuthorizer         = core.ClusterControlAuthorizer
	ClusterControlClient             = core.ClusterControlClient
	ClusterControlConfig             = core.ClusterControlConfig
	ClusterControlServer             = core.ClusterControlServer
	ClusterCorrelationID             = core.ClusterCorrelationID
	ClusterDescription               = core.ClusterDescription
	ClusterEgress                    = core.ClusterEgress
	ClusterElectionConfig            = core.ClusterElectionConfig
	ClusterElectionMemberStatus      = core.ClusterElectionMemberStatus
	ClusterElectionStatus            = core.ClusterElectionStatus
	ClusterHandler                   = core.ClusterHandler
	ClusterHeartbeat                 = core.ClusterHeartbeat
	ClusterIngress                   = core.ClusterIngress
	ClusterLearnerConfig             = core.ClusterLearnerConfig
	ClusterLearnerStatus             = core.ClusterLearnerStatus
	ClusterLearnerSyncResult         = core.ClusterLearnerSyncResult
	ClusterLifecycleService          = core.ClusterLifecycleService
	ClusterLog                       = core.ClusterLog
	ClusterLogEntry                  = core.ClusterLogEntry
	ClusterLogEntryType              = core.ClusterLogEntryType
	ClusterMember                    = core.ClusterMember
	ClusterMemberClient              = core.ClusterMemberClient
	ClusterMemberClientConfig        = core.ClusterMemberClientConfig
	ClusterMemberProtocolAction      = core.ClusterMemberProtocolAction
	ClusterMemberProtocolMessage     = core.ClusterMemberProtocolMessage
	ClusterMemberProtocolMessageType = core.ClusterMemberProtocolMessageType
	ClusterMemberTransportConfig     = core.ClusterMemberTransportConfig
	ClusterMemberTransportServer     = core.ClusterMemberTransportServer
	ClusterMembership                = core.ClusterMembership
	ClusterMembershipAction          = core.ClusterMembershipAction
	ClusterMembershipApplyResult     = core.ClusterMembershipApplyResult
	ClusterMembershipChange          = core.ClusterMembershipChange
	ClusterMembershipPlan            = core.ClusterMembershipPlan
	ClusterMembershipRuntime         = core.ClusterMembershipRuntime
	ClusterMembershipRuntimeConfig   = core.ClusterMembershipRuntimeConfig
	ClusterMembershipRuntimeHooks    = core.ClusterMembershipRuntimeHooks
	ClusterMembershipStep            = core.ClusterMembershipStep
	ClusterMessage                   = core.ClusterMessage
	ClusterMode                      = core.ClusterMode
	ClusterNode                      = core.ClusterNode
	ClusterNodeID                    = core.ClusterNodeID
	ClusterPrincipal                 = core.ClusterPrincipal
	ClusterQuorumConfig              = core.ClusterQuorumConfig
	ClusterQuorumStatus              = core.ClusterQuorumStatus
	ClusterReplicationConfig         = core.ClusterReplicationConfig
	ClusterReplicationStatus         = core.ClusterReplicationStatus
	ClusterReplicationSyncResult     = core.ClusterReplicationSyncResult
	ClusterRole                      = core.ClusterRole
	ClusterRollingUpgradeConfig      = core.ClusterRollingUpgradeConfig
	ClusterRollingUpgradePlan        = core.ClusterRollingUpgradePlan
	ClusterService                   = core.ClusterService
	ClusterServiceContext            = core.ClusterServiceContext
	ClusterServiceMessage            = core.ClusterServiceMessage
	ClusterSessionID                 = core.ClusterSessionID
	ClusterSnapshot                  = core.ClusterSnapshot
	ClusterSnapshotService           = core.ClusterSnapshotService
	ClusterSnapshotStore             = core.ClusterSnapshotStore
	ClusterStateSnapshot             = core.ClusterStateSnapshot
	ClusterTimer                     = core.ClusterTimer
	ClusterTimerID                   = core.ClusterTimerID
	ClusterValidationReport          = core.ClusterValidationReport
	InMemoryClusterLog               = core.InMemoryClusterLog
	InMemoryClusterSnapshotStore     = core.InMemoryClusterSnapshotStore
)

var (
	ErrClusterBackupClosed             = core.ErrClusterBackupClosed
	ErrClusterBackupUnsupported        = core.ErrClusterBackupUnsupported
	ErrClusterClosed                   = core.ErrClusterClosed
	ErrClusterControlClosed            = core.ErrClusterControlClosed
	ErrClusterElectionUnavailable      = core.ErrClusterElectionUnavailable
	ErrClusterLearnerUnavailable       = core.ErrClusterLearnerUnavailable
	ErrClusterLogClosed                = core.ErrClusterLogClosed
	ErrClusterLogEntryType             = core.ErrClusterLogEntryType
	ErrClusterLogPosition              = core.ErrClusterLogPosition
	ErrClusterNotLeader                = core.ErrClusterNotLeader
	ErrClusterQuorumUnavailable        = core.ErrClusterQuorumUnavailable
	ErrClusterReplicationUnavailable   = core.ErrClusterReplicationUnavailable
	ErrClusterServiceUnavailable       = core.ErrClusterServiceUnavailable
	ErrClusterSnapshotClosed           = core.ErrClusterSnapshotClosed
	ErrClusterSnapshotStoreUnavailable = core.ErrClusterSnapshotStoreUnavailable
	ErrClusterSnapshotUnsupported      = core.ErrClusterSnapshotUnsupported
	ErrClusterSuspended                = core.ErrClusterSuspended
	ErrClusterTimerNotFound            = core.ErrClusterTimerNotFound
)

func ClusterPrincipalFromContext(ctx context.Context) (ClusterPrincipal, bool) {
	return core.ClusterPrincipalFromContext(ctx)
}

func ContextWithClusterPrincipal(ctx context.Context, principal ClusterPrincipal) context.Context {
	return core.ContextWithClusterPrincipal(ctx, principal)
}

func DefaultClusterClientConfig() ClusterClientConfig {
	return core.DefaultClusterClientConfig()
}

func DialClusterMember(ctx context.Context, cfg ClusterMemberClientConfig) (*ClusterMemberClient, error) {
	return core.DialClusterMember(ctx, cfg)
}

func ListenClusterMemberTransport(ctx context.Context, node *ClusterNode, cfg ClusterMemberTransportConfig) (*ClusterMemberTransportServer, error) {
	return core.ListenClusterMemberTransport(ctx, node, cfg)
}

func NewArchiveClusterLog(archive *Archive) (*ArchiveClusterLog, error) {
	return core.NewArchiveClusterLog(archive)
}

func NewArchiveClusterSnapshotStore(archive *Archive) (*ArchiveClusterSnapshotStore, error) {
	return core.NewArchiveClusterSnapshotStore(archive)
}

func NewClusterMembershipRuntime(cfg ClusterMembershipRuntimeConfig) (*ClusterMembershipRuntime, error) {
	return core.NewClusterMembershipRuntime(cfg)
}

func NewInMemoryClusterLog() *InMemoryClusterLog {
	return core.NewInMemoryClusterLog()
}

func NewInMemoryClusterSnapshotStore() *InMemoryClusterSnapshotStore {
	return core.NewInMemoryClusterSnapshotStore()
}

func PlanClusterMembershipChange(current ClusterMembership, change ClusterMembershipChange) (ClusterMembershipPlan, error) {
	return core.PlanClusterMembershipChange(current, change)
}

func PlanClusterRollingUpgrade(cfg ClusterRollingUpgradeConfig) (ClusterRollingUpgradePlan, error) {
	return core.PlanClusterRollingUpgrade(cfg)
}

func StartClusterBackup(ctx context.Context, cfg ClusterBackupConfig) (*ClusterBackup, error) {
	return core.StartClusterBackup(ctx, cfg)
}

func StartClusterControlServer(node *ClusterNode, cfg ClusterControlConfig) (*ClusterControlServer, error) {
	return core.StartClusterControlServer(node, cfg)
}

func StartClusterNode(ctx context.Context, cfg ClusterConfig) (*ClusterNode, error) {
	return core.StartClusterNode(ctx, cfg)
}
