package bunshin

import (
	"context"
	"errors"
	"sync"
)

const defaultClusterControlBuffer = 64

var ErrClusterControlClosed = errors.New("bunshin cluster control: closed")

type ClusterControlConfig struct {
	CommandBuffer int
	Authorizer    ClusterControlAuthorizer
}

type ClusterControlServer struct {
	node       *ClusterNode
	commands   chan clusterControlCommand
	done       chan struct{}
	authorizer ClusterControlAuthorizer
	once       sync.Once
}

type ClusterControlClient struct {
	server *ClusterControlServer
}

type ClusterControlAction string

const (
	ClusterControlActionDescribe ClusterControlAction = "describe"
	ClusterControlActionSnapshot ClusterControlAction = "snapshot"
	ClusterControlActionSuspend  ClusterControlAction = "suspend"
	ClusterControlActionResume   ClusterControlAction = "resume"
	ClusterControlActionShutdown ClusterControlAction = "shutdown"
	ClusterControlActionValidate ClusterControlAction = "validate"
)

type ClusterControlAuthorizer func(context.Context, ClusterControlAction) error

type ClusterDescription struct {
	NodeID           ClusterNodeID            `json:"node_id"`
	Mode             ClusterMode              `json:"mode"`
	Role             ClusterRole              `json:"role"`
	LastPosition     int64                    `json:"last_position"`
	SnapshotPosition int64                    `json:"snapshot_position"`
	Closed           bool                     `json:"closed"`
	Suspended        bool                     `json:"suspended"`
	Learner          ClusterLearnerStatus     `json:"learner"`
	Replication      ClusterReplicationStatus `json:"replication"`
	Election         ClusterElectionStatus    `json:"election"`
	Quorum           ClusterQuorumStatus      `json:"quorum"`
}

type ClusterValidationReport struct {
	Description ClusterDescription `json:"description"`
	Healthy     bool               `json:"healthy"`
	Errors      []string           `json:"errors,omitempty"`
}

type clusterControlCommand struct {
	ctx    context.Context
	action ClusterControlAction
	apply  func() (any, error)
	reply  chan clusterControlResult
}

type clusterControlResult struct {
	value any
	err   error
}

func StartClusterControlServer(node *ClusterNode, cfg ClusterControlConfig) (*ClusterControlServer, error) {
	if node == nil {
		return nil, invalidConfigf("cluster node is required")
	}
	if cfg.CommandBuffer < 0 {
		return nil, invalidConfigf("invalid cluster control command buffer: %d", cfg.CommandBuffer)
	}
	if cfg.CommandBuffer == 0 {
		cfg.CommandBuffer = defaultClusterControlBuffer
	}
	server := &ClusterControlServer{
		node:       node,
		commands:   make(chan clusterControlCommand, cfg.CommandBuffer),
		done:       make(chan struct{}),
		authorizer: cfg.Authorizer,
	}
	go server.run()
	return server, nil
}

func (s *ClusterControlServer) Client() *ClusterControlClient {
	return &ClusterControlClient{server: s}
}

func (s *ClusterControlServer) Close() error {
	s.once.Do(func() {
		close(s.done)
	})
	return nil
}

func (s *ClusterControlServer) run() {
	for {
		select {
		case <-s.done:
			return
		case command := <-s.commands:
			if s.authorizer != nil {
				if err := s.authorizer(command.ctx, command.action); err != nil {
					command.reply <- clusterControlResult{err: err}
					continue
				}
			}
			value, err := command.apply()
			command.reply <- clusterControlResult{value: value, err: err}
		}
	}
}

func (c *ClusterControlClient) Describe(ctx context.Context) (ClusterDescription, error) {
	value, err := c.dispatch(ctx, ClusterControlActionDescribe, func() (any, error) {
		return c.server.node.Describe(ctx)
	})
	if err != nil {
		return ClusterDescription{}, err
	}
	return value.(ClusterDescription), nil
}

func (c *ClusterControlClient) Snapshot(ctx context.Context) (ClusterStateSnapshot, error) {
	value, err := c.dispatch(ctx, ClusterControlActionSnapshot, func() (any, error) {
		return c.server.node.TakeSnapshot(ctx)
	})
	if err != nil {
		return ClusterStateSnapshot{}, err
	}
	return value.(ClusterStateSnapshot), nil
}

func (c *ClusterControlClient) Suspend(ctx context.Context) error {
	_, err := c.dispatch(ctx, ClusterControlActionSuspend, func() (any, error) {
		return nil, c.server.node.Suspend(ctx)
	})
	return err
}

func (c *ClusterControlClient) Resume(ctx context.Context) error {
	_, err := c.dispatch(ctx, ClusterControlActionResume, func() (any, error) {
		return nil, c.server.node.Resume(ctx)
	})
	return err
}

func (c *ClusterControlClient) Shutdown(ctx context.Context) error {
	_, err := c.dispatch(ctx, ClusterControlActionShutdown, func() (any, error) {
		return nil, c.server.node.Close(ctx)
	})
	return err
}

func (c *ClusterControlClient) Validate(ctx context.Context) (ClusterValidationReport, error) {
	value, err := c.dispatch(ctx, ClusterControlActionValidate, func() (any, error) {
		return c.server.node.Validate(ctx)
	})
	if err != nil {
		return ClusterValidationReport{}, err
	}
	return value.(ClusterValidationReport), nil
}

func (c *ClusterControlClient) dispatch(ctx context.Context, action ClusterControlAction, apply func() (any, error)) (any, error) {
	if c == nil || c.server == nil {
		return nil, ErrClusterControlClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	reply := make(chan clusterControlResult, 1)
	command := clusterControlCommand{
		ctx:    ctx,
		action: action,
		apply:  apply,
		reply:  reply,
	}
	select {
	case c.server.commands <- command:
	case <-c.server.done:
		return nil, ErrClusterControlClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case result := <-reply:
		return result.value, result.err
	case <-c.server.done:
		return nil, ErrClusterControlClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *ClusterNode) Describe(ctx context.Context) (ClusterDescription, error) {
	snapshot, err := n.Snapshot(ctx)
	if err != nil {
		return ClusterDescription{}, err
	}
	return ClusterDescription{
		NodeID:           snapshot.NodeID,
		Mode:             snapshot.Mode,
		Role:             snapshot.Role,
		LastPosition:     snapshot.LastPosition,
		SnapshotPosition: snapshot.SnapshotPosition,
		Closed:           snapshot.Closed,
		Suspended:        snapshot.Suspended,
		Learner:          snapshot.Learner,
		Replication:      snapshot.Replication,
		Election:         snapshot.Election,
		Quorum:           snapshot.Quorum,
	}, nil
}

func (n *ClusterNode) Validate(ctx context.Context) (ClusterValidationReport, error) {
	description, err := n.Describe(ctx)
	if err != nil {
		return ClusterValidationReport{}, err
	}
	report := ClusterValidationReport{
		Description: description,
		Healthy:     true,
	}
	if description.Closed {
		report.Errors = append(report.Errors, ErrClusterClosed.Error())
	}
	if description.SnapshotPosition > description.LastPosition {
		report.Errors = append(report.Errors, "bunshin cluster: snapshot position is ahead of log")
	}
	if description.Learner.Enabled && description.Learner.SyncedPosition > description.Learner.MasterPosition {
		report.Errors = append(report.Errors, "bunshin cluster learner: synced position is ahead of master")
	}
	if description.Replication.Enabled && description.Replication.SourcePosition > 0 &&
		description.Replication.SyncedPosition > description.Replication.SourcePosition {
		report.Errors = append(report.Errors, "bunshin cluster replication: synced position is ahead of source")
	}
	if description.Election.Enabled {
		if description.Election.LeaderID == 0 {
			report.Errors = append(report.Errors, "bunshin cluster election: leader is unavailable")
		}
		if description.Election.Role != description.Role {
			report.Errors = append(report.Errors, "bunshin cluster election: role does not match node role")
		}
	}
	if description.Quorum.Enabled && description.Quorum.LastError != "" {
		report.Errors = append(report.Errors, "bunshin cluster quorum: "+description.Quorum.LastError)
	}
	report.Healthy = len(report.Errors) == 0
	return report, nil
}
