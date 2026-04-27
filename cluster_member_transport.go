package bunshin

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

const (
	ClusterMemberProtocolVersion         = 1
	clusterMemberTransportQUIC           = "quic"
	defaultClusterMemberTransportNetwork = clusterMemberTransportQUIC
	defaultClusterMemberTransportAddr    = "127.0.0.1:0"
)

type ClusterMemberProtocolMessageType string

const (
	ClusterMemberProtocolRequest  ClusterMemberProtocolMessageType = "request"
	ClusterMemberProtocolResponse ClusterMemberProtocolMessageType = "response"
)

type ClusterMemberProtocolAction string

const (
	ClusterMemberActionAppendLog          ClusterMemberProtocolAction = "append_log"
	ClusterMemberActionAppendLogAck       ClusterMemberProtocolAction = "append_log_ack"
	ClusterMemberActionAppendLogBatchAck  ClusterMemberProtocolAction = "append_log_batch_ack"
	ClusterMemberActionSnapshotLog        ClusterMemberProtocolAction = "snapshot_log"
	ClusterMemberActionLastPosition       ClusterMemberProtocolAction = "last_position"
	ClusterMemberActionSaveSnapshot       ClusterMemberProtocolAction = "save_snapshot"
	ClusterMemberActionLoadSnapshot       ClusterMemberProtocolAction = "load_snapshot"
	ClusterMemberActionTakeSnapshot       ClusterMemberProtocolAction = "take_snapshot"
	ClusterMemberActionSubmitIngress      ClusterMemberProtocolAction = "submit_ingress"
	ClusterMemberActionSubmitIngressBatch ClusterMemberProtocolAction = "submit_ingress_batch"
	ClusterMemberActionDescribe           ClusterMemberProtocolAction = "describe"
)

type ClusterMemberTransportConfig struct {
	Network    string
	Addr       string
	Listener   net.Listener
	TLSConfig  *tls.Config
	QUICConfig *quic.Config
	PacketConn net.PacketConn
}

type ClusterMemberClientConfig struct {
	Network    string
	Addr       string
	Conn       net.Conn
	TLSConfig  *tls.Config
	QUICConfig *quic.Config
	PacketConn net.PacketConn
}

type ClusterMemberTransportServer struct {
	node         *ClusterNode
	listener     net.Listener
	quicListener *quic.Listener
	transport    *quic.Transport

	done      chan struct{}
	once      sync.Once
	wg        sync.WaitGroup
	conns     map[net.Conn]struct{}
	quicConns map[*quic.Conn]struct{}
	mu        sync.Mutex
}

type ClusterMemberClient struct {
	stream    clusterMemberProtocolStream
	conn      *quic.Conn
	transport *quic.Transport
	reader    *bufio.Reader

	mu                sync.Mutex
	nextCorrelationID uint64
	closed            bool
}

type clusterMemberProtocolStream interface {
	io.Reader
	io.Writer
	io.Closer
	SetDeadline(time.Time) error
}

type ClusterMemberProtocolMessage struct {
	Version       int                              `json:"version"`
	Type          ClusterMemberProtocolMessageType `json:"type"`
	CorrelationID uint64                           `json:"correlation_id,omitempty"`
	Action        ClusterMemberProtocolAction      `json:"action,omitempty"`
	Error         string                           `json:"error,omitempty"`
	ErrorCode     string                           `json:"error_code,omitempty"`

	Entry       ClusterLogEntry      `json:"entry,omitempty"`
	Entries     []ClusterLogEntry    `json:"entries,omitempty"`
	Position    int64                `json:"position,omitempty"`
	Snapshot    ClusterStateSnapshot `json:"snapshot,omitempty"`
	HasSnapshot bool                 `json:"has_snapshot,omitempty"`
	Ingress     ClusterIngress       `json:"ingress,omitempty"`
	Ingresses   []ClusterIngress     `json:"ingresses,omitempty"`
	Egress      ClusterEgress        `json:"egress,omitempty"`
	Egresses    []ClusterEgress      `json:"egresses,omitempty"`
	Description ClusterDescription   `json:"description,omitempty"`
}

func ListenClusterMemberTransport(ctx context.Context, node *ClusterNode, cfg ClusterMemberTransportConfig) (*ClusterMemberTransportServer, error) {
	if node == nil {
		return nil, invalidConfigf("cluster node is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	network := cfg.Network
	if network == "" {
		if cfg.Listener != nil {
			network = "tcp"
		} else {
			network = defaultClusterMemberTransportNetwork
		}
	}
	addr := cfg.Addr
	if addr == "" {
		addr = defaultClusterMemberTransportAddr
	}

	server := &ClusterMemberTransportServer{
		node:      node,
		done:      make(chan struct{}),
		conns:     make(map[net.Conn]struct{}),
		quicConns: make(map[*quic.Conn]struct{}),
	}
	if network == clusterMemberTransportQUIC {
		if cfg.Listener != nil {
			return nil, invalidConfigf("cluster member TCP listener cannot be used with QUIC transport")
		}
		tlsConf, err := clusterMemberServerTLSConfig(cfg.TLSConfig)
		if err != nil {
			return nil, err
		}
		listener, transport, err := listenQUIC(addr, tlsConf, cfg.QUICConfig, cfg.PacketConn)
		if err != nil {
			return nil, fmt.Errorf("listen cluster member quic transport: %w", err)
		}
		server.quicListener = listener
		server.transport = transport
		server.wg.Add(1)
		go server.acceptQUICLoop(ctx)
	} else {
		listener := cfg.Listener
		if listener == nil {
			var err error
			listener, err = net.Listen(network, addr)
			if err != nil {
				return nil, fmt.Errorf("listen cluster member transport: %w", err)
			}
		}
		server.listener = listener
		server.wg.Add(1)
		go server.acceptLoop(ctx)
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = server.Close()
		case <-server.done:
		}
	}()
	return server, nil
}

func (s *ClusterMemberTransportServer) Addr() net.Addr {
	if s == nil {
		return nil
	}
	if s.quicListener != nil {
		return s.quicListener.Addr()
	}
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

func (s *ClusterMemberTransportServer) Close() error {
	if s == nil {
		return nil
	}
	var err error
	s.once.Do(func() {
		close(s.done)
		if s.listener != nil {
			err = errors.Join(err, s.listener.Close())
		}
		if s.quicListener != nil {
			err = errors.Join(err, s.quicListener.Close())
		}
		s.mu.Lock()
		for conn := range s.conns {
			err = errors.Join(err, conn.Close())
		}
		for conn := range s.quicConns {
			err = errors.Join(err, conn.CloseWithError(0, "closed"))
		}
		s.mu.Unlock()
		s.wg.Wait()
		if s.transport != nil {
			err = errors.Join(err, s.transport.Close())
		}
	})
	return err
}

func (s *ClusterMemberTransportServer) acceptLoop(ctx context.Context) {
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

func (s *ClusterMemberTransportServer) serveConn(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		_ = conn.Close()
	}()

	s.serveProtocolStream(ctx, conn)
}

func (s *ClusterMemberTransportServer) acceptQUICLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		conn, err := s.quicListener.Accept(ctx)
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
		s.quicConns[conn] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.serveQUICConn(ctx, conn)
	}
}

func (s *ClusterMemberTransportServer) serveQUICConn(ctx context.Context, conn *quic.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.quicConns, conn)
		s.mu.Unlock()
		_ = conn.CloseWithError(0, "closed")
	}()

	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		s.serveProtocolStream(ctx, stream)
	}
}

func (s *ClusterMemberTransportServer) serveProtocolStream(ctx context.Context, stream io.ReadWriteCloser) {
	defer stream.Close()

	reader := bufio.NewReader(stream)
	for {
		request, err := readClusterMemberProtocolMessage(reader)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(ctx.Err(), context.Canceled) {
				return
			}
			_ = writeClusterMemberProtocolMessage(stream, clusterMemberProtocolError(0, err))
			return
		}
		response := s.handleRequest(ctx, request)
		if err := writeClusterMemberProtocolMessage(stream, response); err != nil {
			return
		}
	}
}

func (s *ClusterMemberTransportServer) handleRequest(ctx context.Context, request ClusterMemberProtocolMessage) ClusterMemberProtocolMessage {
	if request.Version != ClusterMemberProtocolVersion {
		return clusterMemberProtocolError(request.CorrelationID, fmt.Errorf("%w: unsupported cluster member protocol version %d", ErrInvalidConfig, request.Version))
	}
	if request.Type != ClusterMemberProtocolRequest {
		return clusterMemberProtocolError(request.CorrelationID, fmt.Errorf("%w: expected cluster member request", ErrInvalidConfig))
	}
	if request.CorrelationID == 0 {
		return clusterMemberProtocolError(0, invalidConfigf("cluster member protocol correlation id is required"))
	}

	response := clusterMemberProtocolResponse(request.CorrelationID)
	switch request.Action {
	case ClusterMemberActionAppendLog:
		entry, err := s.node.log.Append(ctx, request.Entry)
		if err != nil {
			return clusterMemberProtocolError(request.CorrelationID, err)
		}
		response.Entry = entry
	case ClusterMemberActionAppendLogAck:
		if err := appendClusterQuorumMember(ctx, s.node.log, request.Entry); err != nil {
			return clusterMemberProtocolError(request.CorrelationID, err)
		}
		response.Position = request.Entry.Position
	case ClusterMemberActionAppendLogBatchAck:
		if err := appendClusterQuorumMemberBatch(ctx, s.node.log, request.Entries); err != nil {
			return clusterMemberProtocolError(request.CorrelationID, err)
		}
		if len(request.Entries) > 0 {
			response.Position = request.Entries[len(request.Entries)-1].Position
		}
	case ClusterMemberActionSnapshotLog:
		entries, err := s.node.log.Snapshot(ctx)
		if err != nil {
			return clusterMemberProtocolError(request.CorrelationID, err)
		}
		response.Entries = entries
	case ClusterMemberActionLastPosition:
		position, err := s.node.log.LastPosition(ctx)
		if err != nil {
			return clusterMemberProtocolError(request.CorrelationID, err)
		}
		response.Position = position
	case ClusterMemberActionSaveSnapshot:
		if s.node.snapshotStore == nil {
			return clusterMemberProtocolError(request.CorrelationID, ErrClusterSnapshotStoreUnavailable)
		}
		if err := s.node.snapshotStore.Save(ctx, request.Snapshot); err != nil {
			return clusterMemberProtocolError(request.CorrelationID, err)
		}
	case ClusterMemberActionLoadSnapshot:
		if s.node.snapshotStore == nil {
			return clusterMemberProtocolError(request.CorrelationID, ErrClusterSnapshotStoreUnavailable)
		}
		snapshot, ok, err := s.node.snapshotStore.Load(ctx)
		if err != nil {
			return clusterMemberProtocolError(request.CorrelationID, err)
		}
		response.Snapshot = snapshot
		response.HasSnapshot = ok
	case ClusterMemberActionTakeSnapshot:
		snapshot, err := s.node.TakeSnapshot(ctx)
		if err != nil {
			return clusterMemberProtocolError(request.CorrelationID, err)
		}
		response.Snapshot = snapshot
		response.HasSnapshot = true
	case ClusterMemberActionSubmitIngress:
		egress, err := s.node.Submit(ctx, request.Ingress)
		if err != nil {
			return clusterMemberProtocolError(request.CorrelationID, err)
		}
		response.Egress = egress
	case ClusterMemberActionSubmitIngressBatch:
		egresses, err := s.node.SubmitBatch(ctx, request.Ingresses)
		if err != nil {
			return clusterMemberProtocolError(request.CorrelationID, err)
		}
		response.Egresses = egresses
	case ClusterMemberActionDescribe:
		description, err := s.node.Describe(ctx)
		if err != nil {
			return clusterMemberProtocolError(request.CorrelationID, err)
		}
		response.Description = description
	default:
		return clusterMemberProtocolError(request.CorrelationID, invalidConfigf("unsupported cluster member protocol action: %s", request.Action))
	}
	return response
}

func DialClusterMember(ctx context.Context, cfg ClusterMemberClientConfig) (*ClusterMemberClient, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cfg.Conn != nil {
		return newClusterMemberClient(cfg.Conn, nil, nil), nil
	}
	network := cfg.Network
	if network == "" {
		network = defaultClusterMemberTransportNetwork
	}
	if cfg.Addr == "" {
		return nil, invalidConfigf("cluster member address is required")
	}
	if network == clusterMemberTransportQUIC {
		tlsConf := clusterMemberClientTLSConfig(cfg.TLSConfig)
		conn, transport, err := dialQUIC(ctx, cfg.Addr, tlsConf, cfg.QUICConfig, cfg.PacketConn)
		if err != nil {
			return nil, fmt.Errorf("dial cluster member quic transport: %w", err)
		}
		stream, err := conn.OpenStreamSync(ctx)
		if err != nil {
			_ = conn.CloseWithError(0, "open stream failed")
			if transport != nil {
				_ = transport.Close()
			}
			return nil, fmt.Errorf("open cluster member quic stream: %w", err)
		}
		return newClusterMemberClient(stream, conn, transport), nil
	}

	var d net.Dialer
	conn, err := d.DialContext(ctx, network, cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("dial cluster member transport: %w", err)
	}
	return newClusterMemberClient(conn, nil, nil), nil
}

func newClusterMemberClient(stream clusterMemberProtocolStream, conn *quic.Conn, transport *quic.Transport) *ClusterMemberClient {
	return &ClusterMemberClient{
		stream:    stream,
		conn:      conn,
		transport: transport,
		reader:    bufio.NewReader(stream),
	}
}

func (c *ClusterMemberClient) Append(ctx context.Context, entry ClusterLogEntry) (ClusterLogEntry, error) {
	response, err := c.request(ctx, ClusterMemberActionAppendLog, func(request *ClusterMemberProtocolMessage) {
		request.Entry = cloneClusterLogEntry(entry)
	})
	if err != nil {
		return ClusterLogEntry{}, err
	}
	return cloneClusterLogEntry(response.Entry), nil
}

func (c *ClusterMemberClient) AppendQuorumMember(ctx context.Context, entry ClusterLogEntry) error {
	response, err := c.request(ctx, ClusterMemberActionAppendLogAck, func(request *ClusterMemberProtocolMessage) {
		request.Entry = cloneClusterLogEntry(entry)
	})
	if err != nil {
		return err
	}
	if response.Position != entry.Position {
		return fmt.Errorf("%w: quorum member appended mismatched entry at position %d", ErrClusterLogPosition, response.Position)
	}
	return nil
}

func (c *ClusterMemberClient) AppendQuorumBatch(ctx context.Context, entries []ClusterLogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	response, err := c.request(ctx, ClusterMemberActionAppendLogBatchAck, func(request *ClusterMemberProtocolMessage) {
		request.Entries = cloneClusterLogEntries(entries)
	})
	if err != nil {
		return err
	}
	lastPosition := entries[len(entries)-1].Position
	if response.Position != lastPosition {
		return fmt.Errorf("%w: quorum member appended mismatched batch position %d", ErrClusterLogPosition, response.Position)
	}
	return nil
}

func (c *ClusterMemberClient) Snapshot(ctx context.Context) ([]ClusterLogEntry, error) {
	response, err := c.request(ctx, ClusterMemberActionSnapshotLog, nil)
	if err != nil {
		return nil, err
	}
	entries := make([]ClusterLogEntry, len(response.Entries))
	for i, entry := range response.Entries {
		entries[i] = cloneClusterLogEntry(entry)
	}
	return entries, nil
}

func (c *ClusterMemberClient) LastPosition(ctx context.Context) (int64, error) {
	response, err := c.request(ctx, ClusterMemberActionLastPosition, nil)
	if err != nil {
		return 0, err
	}
	return response.Position, nil
}

func (c *ClusterMemberClient) Save(ctx context.Context, snapshot ClusterStateSnapshot) error {
	_, err := c.request(ctx, ClusterMemberActionSaveSnapshot, func(request *ClusterMemberProtocolMessage) {
		request.Snapshot = cloneClusterStateSnapshot(snapshot)
	})
	return err
}

func (c *ClusterMemberClient) Load(ctx context.Context) (ClusterStateSnapshot, bool, error) {
	response, err := c.request(ctx, ClusterMemberActionLoadSnapshot, nil)
	if err != nil {
		return ClusterStateSnapshot{}, false, err
	}
	return cloneClusterStateSnapshot(response.Snapshot), response.HasSnapshot, nil
}

func (c *ClusterMemberClient) TakeSnapshot(ctx context.Context) (ClusterStateSnapshot, error) {
	response, err := c.request(ctx, ClusterMemberActionTakeSnapshot, nil)
	if err != nil {
		return ClusterStateSnapshot{}, err
	}
	return cloneClusterStateSnapshot(response.Snapshot), nil
}

func (c *ClusterMemberClient) Submit(ctx context.Context, ingress ClusterIngress) (ClusterEgress, error) {
	response, err := c.request(ctx, ClusterMemberActionSubmitIngress, func(request *ClusterMemberProtocolMessage) {
		request.Ingress = ClusterIngress{
			SessionID:     ingress.SessionID,
			CorrelationID: ingress.CorrelationID,
			Payload:       cloneBytes(ingress.Payload),
		}
	})
	if err != nil {
		return ClusterEgress{}, err
	}
	response.Egress.Payload = cloneBytes(response.Egress.Payload)
	return response.Egress, nil
}

func (c *ClusterMemberClient) SubmitBatch(ctx context.Context, ingresses []ClusterIngress) ([]ClusterEgress, error) {
	if len(ingresses) == 0 {
		return nil, nil
	}
	response, err := c.request(ctx, ClusterMemberActionSubmitIngressBatch, func(request *ClusterMemberProtocolMessage) {
		request.Ingresses = cloneClusterIngresses(ingresses)
	})
	if err != nil {
		return nil, err
	}
	for i := range response.Egresses {
		response.Egresses[i].Payload = cloneBytes(response.Egresses[i].Payload)
	}
	return response.Egresses, nil
}

func (c *ClusterMemberClient) Describe(ctx context.Context) (ClusterDescription, error) {
	response, err := c.request(ctx, ClusterMemberActionDescribe, nil)
	if err != nil {
		return ClusterDescription{}, err
	}
	return response.Description, nil
}

func (c *ClusterMemberClient) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	var err error
	if c.stream != nil {
		err = errors.Join(err, c.stream.Close())
	}
	if c.conn != nil {
		err = errors.Join(err, c.conn.CloseWithError(0, "closed"))
	}
	if c.transport != nil {
		err = errors.Join(err, c.transport.Close())
	}
	return err
}

func (c *ClusterMemberClient) request(ctx context.Context, action ClusterMemberProtocolAction, fill func(*ClusterMemberProtocolMessage)) (ClusterMemberProtocolMessage, error) {
	if c == nil {
		return ClusterMemberProtocolMessage{}, ErrClusterClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ClusterMemberProtocolMessage{}, ctx.Err()
	default:
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ClusterMemberProtocolMessage{}, ErrClusterClosed
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.stream.SetDeadline(deadline); err != nil {
			return ClusterMemberProtocolMessage{}, err
		}
		defer c.stream.SetDeadline(time.Time{})
	}

	c.nextCorrelationID++
	request := ClusterMemberProtocolMessage{
		Version:       ClusterMemberProtocolVersion,
		Type:          ClusterMemberProtocolRequest,
		CorrelationID: c.nextCorrelationID,
		Action:        action,
	}
	if fill != nil {
		fill(&request)
	}
	if err := writeClusterMemberProtocolMessage(c.stream, request); err != nil {
		return ClusterMemberProtocolMessage{}, err
	}

	response, err := readClusterMemberProtocolMessage(c.reader)
	if err != nil {
		return ClusterMemberProtocolMessage{}, err
	}
	if response.Version != ClusterMemberProtocolVersion {
		return ClusterMemberProtocolMessage{}, fmt.Errorf("%w: unsupported cluster member response version %d", ErrInvalidConfig, response.Version)
	}
	if response.Type != ClusterMemberProtocolResponse {
		return ClusterMemberProtocolMessage{}, fmt.Errorf("%w: expected cluster member response", ErrInvalidConfig)
	}
	if response.CorrelationID != request.CorrelationID {
		return ClusterMemberProtocolMessage{}, fmt.Errorf("%w: cluster member correlation mismatch: got %d want %d", ErrInvalidConfig, response.CorrelationID, request.CorrelationID)
	}
	if response.Error != "" {
		return ClusterMemberProtocolMessage{}, clusterMemberRemoteError(response.ErrorCode, response.Error)
	}
	return response, nil
}

func clusterMemberServerTLSConfig(cfg *tls.Config) (*tls.Config, error) {
	if cfg == nil {
		return defaultServerTLSConfig()
	}
	clone := cfg.Clone()
	if len(clone.NextProtos) == 0 {
		clone.NextProtos = []string{quicALPN}
	}
	return clone, nil
}

func clusterMemberClientTLSConfig(cfg *tls.Config) *tls.Config {
	if cfg == nil {
		return defaultClientTLSConfig()
	}
	clone := cfg.Clone()
	if len(clone.NextProtos) == 0 {
		clone.NextProtos = []string{quicALPN}
	}
	return clone
}

func clusterMemberProtocolResponse(correlationID uint64) ClusterMemberProtocolMessage {
	return ClusterMemberProtocolMessage{
		Version:       ClusterMemberProtocolVersion,
		Type:          ClusterMemberProtocolResponse,
		CorrelationID: correlationID,
	}
}

func clusterMemberProtocolError(correlationID uint64, err error) ClusterMemberProtocolMessage {
	response := clusterMemberProtocolResponse(correlationID)
	if err != nil {
		response.Error = err.Error()
		response.ErrorCode = clusterMemberErrorCode(err)
	}
	return response
}

type clusterMemberError struct {
	message string
	target  error
}

func (e clusterMemberError) Error() string {
	return e.message
}

func (e clusterMemberError) Unwrap() error {
	return e.target
}

func clusterMemberRemoteError(code, message string) error {
	target := clusterMemberErrorTarget(code)
	if target == nil {
		return errors.New(message)
	}
	return clusterMemberError{
		message: message,
		target:  target,
	}
}

func clusterMemberErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrInvalidConfig):
		return "invalid_config"
	case errors.Is(err, ErrClusterClosed):
		return "cluster_closed"
	case errors.Is(err, ErrClusterSuspended):
		return "cluster_suspended"
	case errors.Is(err, ErrClusterNotLeader):
		return "cluster_not_leader"
	case errors.Is(err, ErrClusterServiceUnavailable):
		return "cluster_service_unavailable"
	case errors.Is(err, ErrClusterLogClosed):
		return "cluster_log_closed"
	case errors.Is(err, ErrClusterLogPosition):
		return "cluster_log_position"
	case errors.Is(err, ErrClusterLogEntryType):
		return "cluster_log_entry_type"
	case errors.Is(err, ErrClusterLearnerUnavailable):
		return "cluster_learner_unavailable"
	case errors.Is(err, ErrClusterReplicationUnavailable):
		return "cluster_replication_unavailable"
	case errors.Is(err, ErrClusterElectionUnavailable):
		return "cluster_election_unavailable"
	case errors.Is(err, ErrClusterTimerNotFound):
		return "cluster_timer_not_found"
	case errors.Is(err, ErrClusterSnapshotClosed):
		return "cluster_snapshot_closed"
	case errors.Is(err, ErrClusterSnapshotStoreUnavailable):
		return "cluster_snapshot_store_unavailable"
	case errors.Is(err, ErrClusterSnapshotUnsupported):
		return "cluster_snapshot_unsupported"
	case errors.Is(err, ErrClusterBackupClosed):
		return "cluster_backup_closed"
	case errors.Is(err, ErrClusterBackupUnsupported):
		return "cluster_backup_unsupported"
	case errors.Is(err, ErrClusterQuorumUnavailable):
		return "cluster_quorum_unavailable"
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "context_deadline_exceeded"
	default:
		return "unknown"
	}
}

func clusterMemberErrorTarget(code string) error {
	switch code {
	case "invalid_config":
		return ErrInvalidConfig
	case "cluster_closed":
		return ErrClusterClosed
	case "cluster_suspended":
		return ErrClusterSuspended
	case "cluster_not_leader":
		return ErrClusterNotLeader
	case "cluster_service_unavailable":
		return ErrClusterServiceUnavailable
	case "cluster_log_closed":
		return ErrClusterLogClosed
	case "cluster_log_position":
		return ErrClusterLogPosition
	case "cluster_log_entry_type":
		return ErrClusterLogEntryType
	case "cluster_learner_unavailable":
		return ErrClusterLearnerUnavailable
	case "cluster_replication_unavailable":
		return ErrClusterReplicationUnavailable
	case "cluster_election_unavailable":
		return ErrClusterElectionUnavailable
	case "cluster_timer_not_found":
		return ErrClusterTimerNotFound
	case "cluster_snapshot_closed":
		return ErrClusterSnapshotClosed
	case "cluster_snapshot_store_unavailable":
		return ErrClusterSnapshotStoreUnavailable
	case "cluster_snapshot_unsupported":
		return ErrClusterSnapshotUnsupported
	case "cluster_backup_closed":
		return ErrClusterBackupClosed
	case "cluster_backup_unsupported":
		return ErrClusterBackupUnsupported
	case "cluster_quorum_unavailable":
		return ErrClusterQuorumUnavailable
	case "context_canceled":
		return context.Canceled
	case "context_deadline_exceeded":
		return context.DeadlineExceeded
	default:
		return nil
	}
}
