package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DriverIPCProtocolVersion               = 1
	defaultDriverIPCSubscriptionPollWindow = 10 * time.Millisecond
	defaultDriverIPCSubscriptionPumpWindow = defaultDriverIPCSubscriptionPollWindow
	defaultDriverIPCSubscriptionDataRing   = 1024 * 1024
)

var (
	ErrDriverIPCClosed   = errors.New("bunshin driver ipc: closed")
	ErrDriverIPCProtocol = errors.New("bunshin driver ipc: protocol error")
)

type DriverIPCCommandType string

const (
	DriverIPCCommandOpenClient        DriverIPCCommandType = "open_client"
	DriverIPCCommandHeartbeatClient   DriverIPCCommandType = "heartbeat_client"
	DriverIPCCommandCloseClient       DriverIPCCommandType = "close_client"
	DriverIPCCommandAddPublication    DriverIPCCommandType = "add_publication"
	DriverIPCCommandSendPublication   DriverIPCCommandType = "send_publication"
	DriverIPCCommandClosePublication  DriverIPCCommandType = "close_publication"
	DriverIPCCommandAddSubscription   DriverIPCCommandType = "add_subscription"
	DriverIPCCommandPollSubscription  DriverIPCCommandType = "poll_subscription"
	DriverIPCCommandCloseSubscription DriverIPCCommandType = "close_subscription"
	DriverIPCCommandSnapshot          DriverIPCCommandType = "snapshot"
	DriverIPCCommandFlushReports      DriverIPCCommandType = "flush_reports"
	DriverIPCCommandTerminate         DriverIPCCommandType = "terminate"
)

type DriverIPCEventType string

const (
	DriverIPCEventCommandError       DriverIPCEventType = "command_error"
	DriverIPCEventClientOpened       DriverIPCEventType = "client_opened"
	DriverIPCEventClientHeartbeat    DriverIPCEventType = "client_heartbeat"
	DriverIPCEventClientClosed       DriverIPCEventType = "client_closed"
	DriverIPCEventPublicationAdded   DriverIPCEventType = "publication_added"
	DriverIPCEventPublicationSent    DriverIPCEventType = "publication_sent"
	DriverIPCEventPublicationClosed  DriverIPCEventType = "publication_closed"
	DriverIPCEventSubscriptionAdded  DriverIPCEventType = "subscription_added"
	DriverIPCEventSubscriptionPolled DriverIPCEventType = "subscription_polled"
	DriverIPCEventSubscriptionClosed DriverIPCEventType = "subscription_closed"
	DriverIPCEventSnapshot           DriverIPCEventType = "snapshot"
	DriverIPCEventReportsFlushed     DriverIPCEventType = "reports_flushed"
	DriverIPCEventTerminated         DriverIPCEventType = "terminated"
)

type DriverIPCConfig struct {
	Directory           string
	CommandRingPath     string
	EventRingPath       string
	CommandRingCapacity int
	EventRingCapacity   int
	Reset               bool
}

type DriverIPC struct {
	commandRing       *IPCRing
	eventRing         *IPCRing
	nextCorrelationID atomic.Uint64
	pendingMu         sync.Mutex
	pendingEvents     []DriverIPCEvent
	closeOnce         sync.Once
	closed            atomic.Bool
}

type DriverIPCCommand struct {
	Version          int                         `json:"version"`
	CorrelationID    uint64                      `json:"correlation_id"`
	Type             DriverIPCCommandType        `json:"type"`
	ClientID         DriverClientID              `json:"client_id,omitempty"`
	ResourceID       DriverResourceID            `json:"resource_id,omitempty"`
	ResponseRingPath string                      `json:"response_ring_path,omitempty"`
	ClientName       string                      `json:"client_name,omitempty"`
	Publication      DriverIPCPublicationConfig  `json:"publication,omitempty"`
	Subscription     DriverIPCSubscriptionConfig `json:"subscription,omitempty"`
	Payload          []byte                      `json:"payload,omitempty"`
	MessageLimit     int                         `json:"message_limit,omitempty"`
	FragmentLimit    int                         `json:"fragment_limit,omitempty"`
}

type DriverIPCPublicationConfig struct {
	Transport                 TransportMode `json:"transport,omitempty"`
	StreamID                  uint32        `json:"stream_id,omitempty"`
	SessionID                 uint32        `json:"session_id,omitempty"`
	RemoteAddr                string        `json:"remote_addr,omitempty"`
	UDPDestinations           []string      `json:"udp_destinations,omitempty"`
	UDPMulticastInterface     string        `json:"udp_multicast_interface,omitempty"`
	UDPNameResolutionInterval time.Duration `json:"udp_name_resolution_interval,omitempty"`
	UDPReceiverTimeout        time.Duration `json:"udp_receiver_timeout,omitempty"`
	MaxPayloadBytes           int           `json:"max_payload_bytes,omitempty"`
	MTUBytes                  int           `json:"mtu_bytes,omitempty"`
	UDPRetransmitBufferBytes  int           `json:"udp_retransmit_buffer_bytes,omitempty"`
	TermBufferLength          int           `json:"term_buffer_length,omitempty"`
	InitialTermID             int32         `json:"initial_term_id,omitempty"`
	PublicationWindowBytes    int           `json:"publication_window_bytes,omitempty"`
}

type DriverIPCSubscriptionConfig struct {
	Transport             TransportMode `json:"transport,omitempty"`
	StreamID              uint32        `json:"stream_id,omitempty"`
	LocalAddr             string        `json:"local_addr,omitempty"`
	UDPMulticastInterface string        `json:"udp_multicast_interface,omitempty"`
	UDPNakRetryInterval   time.Duration `json:"udp_nak_retry_interval,omitempty"`
	ReceiverWindowBytes   int           `json:"receiver_window_bytes,omitempty"`
	TermBufferLength      int           `json:"term_buffer_length,omitempty"`
	DataRingCapacity      int           `json:"data_ring_capacity,omitempty"`
}

type DriverIPCEvent struct {
	Version                int                    `json:"version"`
	CorrelationID          uint64                 `json:"correlation_id,omitempty"`
	Type                   DriverIPCEventType     `json:"type"`
	CommandType            DriverIPCCommandType   `json:"command_type,omitempty"`
	ClientID               DriverClientID         `json:"client_id,omitempty"`
	ResourceID             DriverResourceID       `json:"resource_id,omitempty"`
	DataImagePath          string                 `json:"data_image_path,omitempty"`
	DataImageCapacity      int                    `json:"data_image_capacity,omitempty"`
	DataImageUsed          int                    `json:"data_image_used,omitempty"`
	DataImageFree          int                    `json:"data_image_free,omitempty"`
	DataImageReadPosition  uint64                 `json:"data_image_read_position,omitempty"`
	DataImageWritePosition uint64                 `json:"data_image_write_position,omitempty"`
	DataRingPath           string                 `json:"data_ring_path,omitempty"`
	DataRingCapacity       int                    `json:"data_ring_capacity,omitempty"`
	DataRingUsed           int                    `json:"data_ring_used,omitempty"`
	DataRingFree           int                    `json:"data_ring_free,omitempty"`
	DataRingReadPosition   uint64                 `json:"data_ring_read_position,omitempty"`
	DataRingWritePosition  uint64                 `json:"data_ring_write_position,omitempty"`
	BackPressured          bool                   `json:"back_pressured,omitempty"`
	MessageCount           int                    `json:"message_count,omitempty"`
	Message                string                 `json:"message,omitempty"`
	Error                  string                 `json:"error,omitempty"`
	Messages               []DriverIPCMessage     `json:"messages,omitempty"`
	Snapshot               *DriverSnapshot        `json:"snapshot,omitempty"`
	Directory              *DriverDirectoryReport `json:"directory,omitempty"`
	At                     time.Time              `json:"at"`
}

type DriverIPCMessage struct {
	StreamID      uint32 `json:"stream_id,omitempty"`
	SessionID     uint32 `json:"session_id,omitempty"`
	TermID        int32  `json:"term_id,omitempty"`
	TermOffset    int32  `json:"term_offset,omitempty"`
	Position      int64  `json:"position,omitempty"`
	Sequence      uint64 `json:"sequence,omitempty"`
	ReservedValue uint64 `json:"reserved_value,omitempty"`
	Payload       []byte `json:"payload,omitempty"`
	Remote        string `json:"remote,omitempty"`
}

type DriverIPCCommandHandler func(DriverIPCCommand) error

type DriverIPCEventHandler func(DriverIPCEvent) error

type DriverIPCServer struct {
	driver               *MediaDriver
	ipc                  *DriverIPC
	terminated           bool
	clients              map[DriverClientID]*DriverClient
	publications         map[DriverResourceID]*DriverPublication
	subscriptions        map[DriverResourceID]*DriverSubscription
	publicationOwners    map[DriverResourceID]DriverClientID
	subscriptionOwners   map[DriverResourceID]DriverClientID
	responseRings        map[string]*IPCRing
	dataImages           map[DriverResourceID]*DriverSubscriptionImage
	dataImagePaths       map[DriverResourceID]string
	subscriptionFallback map[DriverResourceID][]DriverIPCMessage
}

func OpenDriverIPC(cfg DriverIPCConfig) (*DriverIPC, error) {
	if cfg.Directory != "" {
		layout, err := ResolveDriverDirectoryLayout(cfg.Directory)
		if err != nil {
			return nil, err
		}
		if cfg.CommandRingPath == "" {
			cfg.CommandRingPath = layout.CommandRingFile
		}
		if cfg.EventRingPath == "" {
			cfg.EventRingPath = layout.EventRingFile
		}
	}
	if cfg.CommandRingPath == "" {
		return nil, invalidConfigf("driver ipc command ring path is required")
	}
	if cfg.EventRingPath == "" {
		return nil, invalidConfigf("driver ipc event ring path is required")
	}
	commandRing, err := OpenIPCRing(IPCRingConfig{
		Path:     cfg.CommandRingPath,
		Capacity: cfg.CommandRingCapacity,
		Reset:    cfg.Reset,
	})
	if err != nil {
		return nil, err
	}
	eventRing, err := OpenIPCRing(IPCRingConfig{
		Path:     cfg.EventRingPath,
		Capacity: cfg.EventRingCapacity,
		Reset:    cfg.Reset,
	})
	if err != nil {
		_ = commandRing.Close()
		return nil, err
	}
	return &DriverIPC{
		commandRing: commandRing,
		eventRing:   eventRing,
	}, nil
}

func NewDriverIPCServer(driver *MediaDriver, ipc *DriverIPC) (*DriverIPCServer, error) {
	if driver == nil {
		return nil, invalidConfigf("media driver is required")
	}
	if ipc == nil {
		return nil, invalidConfigf("driver ipc is required")
	}
	return &DriverIPCServer{
		driver:               driver,
		ipc:                  ipc,
		clients:              make(map[DriverClientID]*DriverClient),
		publications:         make(map[DriverResourceID]*DriverPublication),
		subscriptions:        make(map[DriverResourceID]*DriverSubscription),
		publicationOwners:    make(map[DriverResourceID]DriverClientID),
		subscriptionOwners:   make(map[DriverResourceID]DriverClientID),
		responseRings:        make(map[string]*IPCRing),
		dataImages:           make(map[DriverResourceID]*DriverSubscriptionImage),
		dataImagePaths:       make(map[DriverResourceID]string),
		subscriptionFallback: make(map[DriverResourceID][]DriverIPCMessage),
	}, nil
}

func (p *DriverIPC) Close() error {
	if p == nil {
		return nil
	}
	var err error
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		if p.commandRing != nil {
			err = errors.Join(err, p.commandRing.Close())
		}
		if p.eventRing != nil {
			err = errors.Join(err, p.eventRing.Close())
		}
	})
	return err
}

func (p *DriverIPC) NextCorrelationID() uint64 {
	if p == nil {
		return 0
	}
	return p.nextCorrelationID.Add(1)
}

func (p *DriverIPC) SendCommand(ctx context.Context, command DriverIPCCommand, idle IdleStrategy) (uint64, error) {
	if p == nil || p.closed.Load() {
		return 0, ErrDriverIPCClosed
	}
	if command.CorrelationID == 0 {
		command.CorrelationID = p.NextCorrelationID()
	}
	command.Version = DriverIPCProtocolVersion
	if command.ResponseRingPath == "" && p.eventRing != nil {
		command.ResponseRingPath = p.eventRing.path
	}
	if err := validateDriverIPCCommand(command); err != nil {
		return 0, err
	}
	payload, err := json.Marshal(command)
	if err != nil {
		return 0, fmt.Errorf("%w: encode command: %w", ErrDriverIPCProtocol, err)
	}
	if err := p.commandRing.OfferContext(ctx, payload, idle); err != nil {
		return 0, err
	}
	return command.CorrelationID, nil
}

func (p *DriverIPC) PollCommands(limit int, handler DriverIPCCommandHandler) (int, error) {
	if p == nil || p.closed.Load() {
		return 0, ErrDriverIPCClosed
	}
	if handler == nil {
		return 0, invalidConfigf("driver ipc command handler is required")
	}
	return p.commandRing.PollN(limit, func(payload []byte) error {
		command, err := decodeDriverIPCCommand(payload)
		if err != nil {
			return err
		}
		return handler(command)
	})
}

func (p *DriverIPC) SendEvent(ctx context.Context, event DriverIPCEvent, idle IdleStrategy) error {
	if p == nil || p.closed.Load() {
		return ErrDriverIPCClosed
	}
	return sendDriverIPCEvent(ctx, p.eventRing, event, idle)
}

func sendDriverIPCEvent(ctx context.Context, ring *IPCRing, event DriverIPCEvent, idle IdleStrategy) error {
	if ring == nil {
		return ErrDriverIPCClosed
	}
	event.Version = DriverIPCProtocolVersion
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	} else {
		event.At = event.At.UTC()
	}
	if err := validateDriverIPCEvent(event); err != nil {
		return err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("%w: encode event: %w", ErrDriverIPCProtocol, err)
	}
	return ring.OfferContext(ctx, payload, idle)
}

func (p *DriverIPC) PollEvents(limit int, handler DriverIPCEventHandler) (int, error) {
	if p == nil || p.closed.Load() {
		return 0, ErrDriverIPCClosed
	}
	if handler == nil {
		return 0, invalidConfigf("driver ipc event handler is required")
	}
	return p.eventRing.PollN(limit, func(payload []byte) error {
		event, err := decodeDriverIPCEvent(payload)
		if err != nil {
			return err
		}
		return handler(event)
	})
}

func (p *DriverIPC) takePendingEvent(correlationID uint64) (DriverIPCEvent, bool) {
	if p == nil || correlationID == 0 {
		return DriverIPCEvent{}, false
	}
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()
	for i, event := range p.pendingEvents {
		if event.CorrelationID == correlationID {
			copy(p.pendingEvents[i:], p.pendingEvents[i+1:])
			p.pendingEvents[len(p.pendingEvents)-1] = DriverIPCEvent{}
			p.pendingEvents = p.pendingEvents[:len(p.pendingEvents)-1]
			return event, true
		}
	}
	return DriverIPCEvent{}, false
}

func (p *DriverIPC) storePendingEvent(event DriverIPCEvent) {
	if p == nil || event.Type == "" {
		return
	}
	p.pendingMu.Lock()
	p.pendingEvents = append(p.pendingEvents, event)
	p.pendingMu.Unlock()
}

func (s *DriverIPCServer) Poll(ctx context.Context, limit int) (int, error) {
	if s == nil || s.ipc == nil {
		return 0, ErrDriverIPCClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return s.ipc.PollCommands(limit, func(command DriverIPCCommand) error {
		event := s.handleCommand(ctx, command)
		return s.sendEvent(ctx, command, event)
	})
}

func (s *DriverIPCServer) Close() error {
	if s == nil {
		return nil
	}
	var err error
	for path, ring := range s.responseRings {
		if ring != nil {
			err = errors.Join(err, ring.Close())
		}
		delete(s.responseRings, path)
	}
	for id := range s.dataImages {
		err = errors.Join(err, s.closeSubscriptionDataRing(id, true))
	}
	return err
}

func (s *DriverIPCServer) PumpSubscriptions(ctx context.Context, limit int) (int, error) {
	if s == nil || s.driver == nil {
		return 0, ErrDriverIPCClosed
	}
	if limit <= 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ids := make([]DriverResourceID, 0, len(s.subscriptions))
	for id := range s.subscriptions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	work := 0
	for _, id := range ids {
		if work >= limit {
			return work, nil
		}
		if s.subscriptionFallbackCount(id) > 0 {
			continue
		}
		subscription := s.subscriptions[id]
		dataImage := s.dataImages[id]
		if subscription == nil || dataImage == nil {
			continue
		}
		n, err := s.pumpSubscription(ctx, id, subscription, dataImage, limit-work)
		work += n
		if err != nil {
			return work, err
		}
	}
	return work, nil
}

func (s *DriverIPCServer) pumpSubscription(ctx context.Context, id DriverResourceID, subscription *DriverSubscription, dataImage *DriverSubscriptionImage, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	snapshot, err := dataImage.Snapshot()
	if err != nil {
		return 0, err
	}
	if snapshot.Free < minDriverSubscriptionImageRecordBytes {
		return 0, nil
	}
	pollCtx, cancel := context.WithTimeout(ctx, defaultDriverIPCSubscriptionPumpWindow)
	defer cancel()

	var written atomic.Int64
	var fallback atomic.Int64
	var writeErrMu sync.Mutex
	var writeErr error
	_, err = subscription.pollDriverOwned(pollCtx, 1, limit, func(_ context.Context, msg Message) error {
		ipcMessage := driverIPCMessageFromMessage(msg)
		if err := writeDriverSubscriptionImageMessage(dataImage, msg); err != nil {
			if errors.Is(err, ErrBackPressure) {
				if subscription.subscription != nil {
					subscription.subscription.metrics.incBackPressureEvents()
				}
				s.enqueueSubscriptionFallback(id, []DriverIPCMessage{ipcMessage})
				fallback.Add(1)
				return nil
			}
			writeErrMu.Lock()
			writeErr = err
			writeErrMu.Unlock()
			return err
		}
		written.Add(1)
		return nil
	})
	work := int(written.Load() + fallback.Load())
	writeErrMu.Lock()
	recordedWriteErr := writeErr
	writeErrMu.Unlock()
	if recordedWriteErr != nil {
		return work, recordedWriteErr
	}
	if err != nil {
		return work, nil
	}
	return work, nil
}

func (s *DriverIPCServer) Reconcile(ctx context.Context) error {
	if s == nil || s.driver == nil {
		return nil
	}
	snapshot, err := s.driver.Snapshot(ctx)
	if err != nil {
		return err
	}
	s.reconcileSnapshot(snapshot)
	return nil
}

func (s *DriverIPCServer) FlushReports(ctx context.Context) (DriverDirectoryReport, error) {
	if s == nil || s.driver == nil {
		return DriverDirectoryReport{}, ErrDriverDirectoryUnavailable
	}
	if err := s.Reconcile(ctx); err != nil {
		return DriverDirectoryReport{}, err
	}
	report, err := s.driver.FlushReports(ctx)
	if err != nil {
		return DriverDirectoryReport{}, err
	}
	snapshot, err := s.snapshot(ctx)
	if err != nil {
		return DriverDirectoryReport{}, err
	}
	now := time.Now()
	rings := BuildDriverRingsReport(snapshot, now)
	report.Rings = rings
	report.Streams = DriverStreamsReportFile{
		UpdatedAt: now.UTC(),
		Snapshot:  snapshot,
	}
	if report.Layout.RingsReportFile != "" {
		if err := writeDriverJSONFile(report.Layout.RingsReportFile, rings); err != nil {
			return DriverDirectoryReport{}, err
		}
	}
	if report.Layout.StreamsReportFile != "" {
		if err := writeDriverJSONFile(report.Layout.StreamsReportFile, report.Streams); err != nil {
			return DriverDirectoryReport{}, err
		}
	}
	return report, nil
}

func (s *DriverIPCServer) reconcileSnapshot(snapshot DriverSnapshot) {
	activeClients := make(map[DriverClientID]struct{}, len(snapshot.Clients))
	for _, client := range snapshot.Clients {
		activeClients[client.ID] = struct{}{}
	}
	activePublications := make(map[DriverResourceID]struct{}, len(snapshot.Publications))
	for _, publication := range snapshot.Publications {
		activePublications[publication.ID] = struct{}{}
	}
	activeSubscriptions := make(map[DriverResourceID]struct{}, len(snapshot.Subscriptions))
	for _, subscription := range snapshot.Subscriptions {
		activeSubscriptions[subscription.ID] = struct{}{}
	}
	for id := range s.clients {
		if _, ok := activeClients[id]; !ok {
			delete(s.clients, id)
		}
	}
	for id := range s.publications {
		if _, ok := activePublications[id]; !ok {
			delete(s.publications, id)
			delete(s.publicationOwners, id)
		}
	}
	for id := range s.subscriptions {
		if _, ok := activeSubscriptions[id]; !ok {
			_ = s.closeSubscriptionDataRing(id, true)
			delete(s.subscriptions, id)
			delete(s.subscriptionOwners, id)
			delete(s.subscriptionFallback, id)
		}
	}
}

func (s *DriverIPCServer) sendEvent(ctx context.Context, command DriverIPCCommand, event DriverIPCEvent) error {
	if command.ResponseRingPath == "" || s.ipc == nil || s.ipc.eventRing == nil || command.ResponseRingPath == s.ipc.eventRing.path {
		return s.ipc.SendEvent(ctx, event, nil)
	}
	ring, err := s.responseRing(command.ResponseRingPath)
	if err != nil {
		return err
	}
	return sendDriverIPCEvent(ctx, ring, event, nil)
}

func (s *DriverIPCServer) responseRing(path string) (*IPCRing, error) {
	if path == "" {
		return nil, invalidConfigf("driver ipc response ring path is required")
	}
	if ring := s.responseRings[path]; ring != nil {
		return ring, nil
	}
	ring, err := OpenIPCRing(IPCRingConfig{Path: path})
	if err != nil {
		return nil, err
	}
	s.responseRings[path] = ring
	return ring, nil
}

func (s *DriverIPCServer) openSubscriptionDataRing(id DriverResourceID, capacity int) (*DriverSubscriptionImage, string, error) {
	if capacity == 0 {
		capacity = defaultDriverIPCSubscriptionDataRing
	}
	path := s.subscriptionDataRingPath(id)
	image, err := OpenDriverSubscriptionImage(DriverSubscriptionImageConfig{
		Path:     path,
		Capacity: capacity,
		Reset:    true,
	})
	if err != nil {
		return nil, "", err
	}
	s.dataImages[id] = image
	s.dataImagePaths[id] = path
	return image, path, nil
}

func (s *DriverIPCServer) subscriptionDataRingPath(id DriverResourceID) string {
	if s != nil && s.driver != nil {
		if layout, ok := s.driver.Directory(); ok {
			return filepath.Join(layout.BuffersDirectory, "subscriptions", fmt.Sprintf("subscription-%d", id), "image.dat")
		}
	}
	if s != nil && s.ipc != nil && s.ipc.eventRing != nil && s.ipc.eventRing.path != "" {
		return filepath.Join(filepath.Dir(s.ipc.eventRing.path), "subscriptions", fmt.Sprintf("subscription-%d", id), "image.dat")
	}
	return filepath.Join(os.TempDir(), "bunshin-driver", "subscriptions", fmt.Sprintf("subscription-%d", id), "image.dat")
}

func (s *DriverIPCServer) closeSubscriptionDataRing(id DriverResourceID, remove bool) error {
	if s == nil {
		return nil
	}
	var err error
	if image := s.dataImages[id]; image != nil {
		err = errors.Join(err, image.Close())
	}
	path := s.dataImagePaths[id]
	delete(s.dataImages, id)
	delete(s.dataImagePaths, id)
	delete(s.subscriptionFallback, id)
	if remove && path != "" {
		err = errors.Join(err, os.Remove(path))
	}
	return err
}

func (s *DriverIPCServer) Terminated() bool {
	if s == nil {
		return false
	}
	return s.terminated
}

func (s *DriverIPCServer) handleCommand(ctx context.Context, command DriverIPCCommand) DriverIPCEvent {
	event := DriverIPCEvent{
		CorrelationID: command.CorrelationID,
		CommandType:   command.Type,
		ClientID:      command.ClientID,
		ResourceID:    command.ResourceID,
	}
	switch command.Type {
	case DriverIPCCommandOpenClient:
		client, err := s.driver.NewClient(ctx, command.ClientName)
		if err != nil {
			return driverIPCErrorEvent(command, err)
		}
		s.clients[client.ID()] = client
		event.Type = DriverIPCEventClientOpened
		event.ClientID = client.ID()
		event.Message = client.Name()
		return event
	case DriverIPCCommandHeartbeatClient:
		client, err := s.driverIPCClient(command.ClientID)
		if err != nil {
			return driverIPCErrorEvent(command, err)
		}
		if err := client.Heartbeat(ctx); err != nil {
			return driverIPCErrorEvent(command, err)
		}
		event.Type = DriverIPCEventClientHeartbeat
		return event
	case DriverIPCCommandCloseClient:
		client, err := s.driverIPCClient(command.ClientID)
		if err != nil {
			return driverIPCErrorEvent(command, err)
		}
		if err := client.Close(ctx); err != nil {
			return driverIPCErrorEvent(command, err)
		}
		s.removeClient(command.ClientID)
		event.Type = DriverIPCEventClientClosed
		return event
	case DriverIPCCommandAddPublication:
		client, err := s.driverIPCClient(command.ClientID)
		if err != nil {
			return driverIPCErrorEvent(command, err)
		}
		publication, err := client.AddPublication(ctx, command.Publication.publicationConfig())
		if err != nil {
			return driverIPCErrorEvent(command, err)
		}
		s.publications[publication.ID()] = publication
		s.publicationOwners[publication.ID()] = command.ClientID
		event.Type = DriverIPCEventPublicationAdded
		event.ResourceID = publication.ID()
		event.Message = publication.LocalAddr().String()
		return event
	case DriverIPCCommandSendPublication:
		publication, ok := s.publications[command.ResourceID]
		if !ok || s.publicationOwners[command.ResourceID] != command.ClientID {
			return driverIPCErrorEvent(command, ErrDriverResourceNotFound)
		}
		if err := publication.Send(ctx, command.Payload); err != nil {
			return driverIPCErrorEvent(command, err)
		}
		event.Type = DriverIPCEventPublicationSent
		return event
	case DriverIPCCommandClosePublication:
		publication, ok := s.publications[command.ResourceID]
		if !ok || s.publicationOwners[command.ResourceID] != command.ClientID {
			return driverIPCErrorEvent(command, ErrDriverResourceNotFound)
		}
		if err := publication.Close(ctx); err != nil {
			return driverIPCErrorEvent(command, err)
		}
		delete(s.publications, command.ResourceID)
		delete(s.publicationOwners, command.ResourceID)
		event.Type = DriverIPCEventPublicationClosed
		return event
	case DriverIPCCommandAddSubscription:
		client, err := s.driverIPCClient(command.ClientID)
		if err != nil {
			return driverIPCErrorEvent(command, err)
		}
		subscription, err := client.AddSubscription(ctx, command.Subscription.subscriptionConfig())
		if err != nil {
			return driverIPCErrorEvent(command, err)
		}
		_, dataImagePath, err := s.openSubscriptionDataRing(subscription.ID(), command.Subscription.DataRingCapacity)
		if err != nil {
			_ = subscription.Close(ctx)
			return driverIPCErrorEvent(command, err)
		}
		s.subscriptions[subscription.ID()] = subscription
		s.subscriptionOwners[subscription.ID()] = command.ClientID
		event.Type = DriverIPCEventSubscriptionAdded
		event.ResourceID = subscription.ID()
		event.DataImagePath = dataImagePath
		event.DataRingPath = dataImagePath
		event.Message = subscription.LocalAddr().String()
		return event
	case DriverIPCCommandPollSubscription:
		subscription, ok := s.subscriptions[command.ResourceID]
		if !ok || s.subscriptionOwners[command.ResourceID] != command.ClientID {
			return driverIPCErrorEvent(command, ErrDriverResourceNotFound)
		}
		messageLimit := command.MessageLimit
		fragmentLimit := command.FragmentLimit
		if messageLimit <= 0 {
			messageLimit = 1
		}
		if fragmentLimit <= 0 {
			fragmentLimit = messageLimit
		}
		dataImage := s.dataImages[command.ResourceID]
		event.Type = DriverIPCEventSubscriptionPolled
		if fallback := s.takeSubscriptionFallback(command.ResourceID, messageLimit); len(fallback) > 0 {
			event.Messages = fallback
			event.BackPressured = true
			event.Message = ErrBackPressure.Error()
			if dataImage != nil {
				if snapshotErr := addDriverIPCEventDataImageSnapshot(&event, dataImage); snapshotErr != nil {
					return driverIPCErrorEvent(command, snapshotErr)
				}
			}
			return event
		}
		messages := make([]DriverIPCMessage, 0, messageLimit)
		dataImageMessageCount := 0
		dataImageBackPressured := false
		if dataImage != nil {
			messageLimit = 1
			if snapshotErr := addDriverIPCEventDataImageSnapshot(&event, dataImage); snapshotErr != nil {
				return driverIPCErrorEvent(command, snapshotErr)
			}
			if event.DataImageFree < minDriverSubscriptionImageRecordBytes {
				if subscription.subscription != nil {
					subscription.subscription.metrics.incBackPressureEvents()
				}
				event.BackPressured = true
				event.Message = ErrBackPressure.Error()
				return event
			}
		}
		pollCtx, cancel := context.WithTimeout(ctx, defaultDriverIPCSubscriptionPollWindow)
		n, err := subscription.pollDriverOwned(pollCtx, messageLimit, fragmentLimit, func(_ context.Context, msg Message) error {
			ipcMessage := driverIPCMessageFromMessage(msg)
			if dataImage != nil {
				if err := writeDriverSubscriptionImageMessage(dataImage, msg); err != nil {
					if errors.Is(err, ErrBackPressure) {
						if subscription.subscription != nil {
							subscription.subscription.metrics.incBackPressureEvents()
						}
						dataImageBackPressured = true
						messages = append(messages, ipcMessage)
						return nil
					}
					return err
				}
				dataImageMessageCount++
				return nil
			}
			messages = append(messages, ipcMessage)
			return nil
		})
		cancel()
		event.MessageCount = n
		if dataImage != nil {
			event.MessageCount = dataImageMessageCount
			event.BackPressured = dataImageBackPressured
			if dataImageBackPressured {
				event.Message = ErrBackPressure.Error()
			}
		}
		event.Messages = messages
		if dataImage != nil {
			if snapshotErr := addDriverIPCEventDataImageSnapshot(&event, dataImage); snapshotErr != nil {
				return driverIPCErrorEvent(command, snapshotErr)
			}
		}
		if dataImage != nil && errors.Is(err, ErrBackPressure) {
			event.BackPressured = true
			event.Message = ErrBackPressure.Error()
			return event
		}
		if err != nil && !(n == 0 && errors.Is(err, context.DeadlineExceeded)) {
			return driverIPCErrorEvent(command, err)
		}
		return event
	case DriverIPCCommandCloseSubscription:
		subscription, ok := s.subscriptions[command.ResourceID]
		if !ok || s.subscriptionOwners[command.ResourceID] != command.ClientID {
			return driverIPCErrorEvent(command, ErrDriverResourceNotFound)
		}
		if err := subscription.Close(ctx); err != nil {
			return driverIPCErrorEvent(command, err)
		}
		if err := s.closeSubscriptionDataRing(command.ResourceID, true); err != nil {
			return driverIPCErrorEvent(command, err)
		}
		delete(s.subscriptions, command.ResourceID)
		delete(s.subscriptionOwners, command.ResourceID)
		event.Type = DriverIPCEventSubscriptionClosed
		return event
	case DriverIPCCommandSnapshot:
		snapshot, err := s.snapshot(ctx)
		if err != nil {
			return driverIPCErrorEvent(command, err)
		}
		event.Type = DriverIPCEventSnapshot
		event.Snapshot = &snapshot
		return event
	case DriverIPCCommandFlushReports:
		report, err := s.FlushReports(ctx)
		if err != nil {
			return driverIPCErrorEvent(command, err)
		}
		event.Type = DriverIPCEventReportsFlushed
		event.Directory = &report
		return event
	case DriverIPCCommandTerminate:
		if err := s.driver.Close(); err != nil {
			return driverIPCErrorEvent(command, err)
		}
		s.terminated = true
		s.clients = make(map[DriverClientID]*DriverClient)
		s.publications = make(map[DriverResourceID]*DriverPublication)
		s.subscriptions = make(map[DriverResourceID]*DriverSubscription)
		s.publicationOwners = make(map[DriverResourceID]DriverClientID)
		s.subscriptionOwners = make(map[DriverResourceID]DriverClientID)
		s.subscriptionFallback = make(map[DriverResourceID][]DriverIPCMessage)
		for id := range s.dataImages {
			_ = s.closeSubscriptionDataRing(id, true)
		}
		event.Type = DriverIPCEventTerminated
		return event
	default:
		return driverIPCErrorEvent(command, fmt.Errorf("%w: unknown command type %q", ErrDriverIPCProtocol, command.Type))
	}
}

func (s *DriverIPCServer) snapshot(ctx context.Context) (DriverSnapshot, error) {
	snapshot, err := s.driver.Snapshot(ctx)
	if err != nil {
		return DriverSnapshot{}, err
	}
	if err := s.addSubscriptionDataRingSnapshots(&snapshot); err != nil {
		return DriverSnapshot{}, err
	}
	return snapshot, nil
}

func (s *DriverIPCServer) addSubscriptionDataRingSnapshots(snapshot *DriverSnapshot) error {
	if s == nil || snapshot == nil {
		return nil
	}
	for i := range snapshot.Subscriptions {
		subscription := &snapshot.Subscriptions[i]
		path := s.dataImagePaths[subscription.ID]
		if path == "" {
			continue
		}
		subscription.DataImagePath = path
		subscription.DataImageMapped = true
		subscription.DataRingPath = path
		subscription.DataRingMapped = true
		image := s.dataImages[subscription.ID]
		if image == nil {
			continue
		}
		imageSnapshot, err := image.Snapshot()
		if err != nil {
			return err
		}
		pending := s.subscriptionFallbackCount(subscription.ID)
		subscription.DataImageCapacity = imageSnapshot.Capacity
		subscription.DataImageUsed = imageSnapshot.Used
		subscription.DataImageFree = imageSnapshot.Free
		subscription.DataImageReadPosition = imageSnapshot.ReadPosition
		subscription.DataImageWritePosition = imageSnapshot.WritePosition
		subscription.DataImagePendingMessages = pending
		subscription.DataRingCapacity = imageSnapshot.Capacity
		subscription.DataRingUsed = imageSnapshot.Used
		subscription.DataRingFree = imageSnapshot.Free
		subscription.DataRingReadPosition = imageSnapshot.ReadPosition
		subscription.DataRingWritePosition = imageSnapshot.WritePosition
		subscription.DataRingPendingMessages = pending
	}
	return nil
}

func (s *DriverIPCServer) enqueueSubscriptionFallback(id DriverResourceID, messages []DriverIPCMessage) {
	if s == nil || len(messages) == 0 {
		return
	}
	s.subscriptionFallback[id] = append(s.subscriptionFallback[id], cloneDriverIPCMessages(messages)...)
}

func (s *DriverIPCServer) takeSubscriptionFallback(id DriverResourceID, limit int) []DriverIPCMessage {
	if s == nil || limit <= 0 {
		return nil
	}
	pending := s.subscriptionFallback[id]
	if len(pending) == 0 {
		return nil
	}
	if limit > len(pending) {
		limit = len(pending)
	}
	out := cloneDriverIPCMessages(pending[:limit])
	if limit == len(pending) {
		delete(s.subscriptionFallback, id)
		return out
	}
	copy(pending, pending[limit:])
	clear(pending[len(pending)-limit:])
	s.subscriptionFallback[id] = pending[:len(pending)-limit]
	return out
}

func (s *DriverIPCServer) subscriptionFallbackCount(id DriverResourceID) int {
	if s == nil {
		return 0
	}
	return len(s.subscriptionFallback[id])
}

func addDriverIPCEventDataImageSnapshot(event *DriverIPCEvent, image *DriverSubscriptionImage) error {
	if event == nil || image == nil {
		return nil
	}
	imageSnapshot, err := image.Snapshot()
	if err != nil {
		return err
	}
	event.DataImagePath = imageSnapshot.Path
	event.DataImageCapacity = imageSnapshot.Capacity
	event.DataImageUsed = imageSnapshot.Used
	event.DataImageFree = imageSnapshot.Free
	event.DataImageReadPosition = imageSnapshot.ReadPosition
	event.DataImageWritePosition = imageSnapshot.WritePosition
	event.DataRingPath = imageSnapshot.Path
	event.DataRingCapacity = imageSnapshot.Capacity
	event.DataRingUsed = imageSnapshot.Used
	event.DataRingFree = imageSnapshot.Free
	event.DataRingReadPosition = imageSnapshot.ReadPosition
	event.DataRingWritePosition = imageSnapshot.WritePosition
	return nil
}

func (s *DriverIPCServer) driverIPCClient(id DriverClientID) (*DriverClient, error) {
	client := s.clients[id]
	if client == nil {
		return nil, ErrDriverClientClosed
	}
	return client, nil
}

func (s *DriverIPCServer) removeClient(id DriverClientID) {
	delete(s.clients, id)
	for resourceID, owner := range s.publicationOwners {
		if owner == id {
			delete(s.publications, resourceID)
			delete(s.publicationOwners, resourceID)
		}
	}
	for resourceID, owner := range s.subscriptionOwners {
		if owner == id {
			_ = s.closeSubscriptionDataRing(resourceID, true)
			delete(s.subscriptions, resourceID)
			delete(s.subscriptionOwners, resourceID)
		}
	}
}

func (cfg DriverIPCPublicationConfig) publicationConfig() PublicationConfig {
	return PublicationConfig{
		Transport:                 cfg.Transport,
		StreamID:                  cfg.StreamID,
		SessionID:                 cfg.SessionID,
		RemoteAddr:                cfg.RemoteAddr,
		UDPDestinations:           append([]string(nil), cfg.UDPDestinations...),
		UDPMulticastInterface:     cfg.UDPMulticastInterface,
		UDPNameResolutionInterval: cfg.UDPNameResolutionInterval,
		UDPReceiverTimeout:        cfg.UDPReceiverTimeout,
		MaxPayloadBytes:           cfg.MaxPayloadBytes,
		MTUBytes:                  cfg.MTUBytes,
		UDPRetransmitBufferBytes:  cfg.UDPRetransmitBufferBytes,
		TermBufferLength:          cfg.TermBufferLength,
		InitialTermID:             cfg.InitialTermID,
		PublicationWindowBytes:    cfg.PublicationWindowBytes,
	}
}

func (cfg DriverIPCSubscriptionConfig) subscriptionConfig() SubscriptionConfig {
	return SubscriptionConfig{
		Transport:              cfg.Transport,
		StreamID:               cfg.StreamID,
		LocalAddr:              cfg.LocalAddr,
		UDPMulticastInterface:  cfg.UDPMulticastInterface,
		UDPNakRetryInterval:    cfg.UDPNakRetryInterval,
		ReceiverWindowBytes:    cfg.ReceiverWindowBytes,
		TermBufferLength:       cfg.TermBufferLength,
		DriverDataRingCapacity: cfg.DataRingCapacity,
	}
}

func driverIPCMessageFromMessage(msg Message) DriverIPCMessage {
	ipcMessage := DriverIPCMessage{
		StreamID:      msg.StreamID,
		SessionID:     msg.SessionID,
		TermID:        msg.TermID,
		TermOffset:    msg.TermOffset,
		Position:      msg.Position,
		Sequence:      msg.Sequence,
		ReservedValue: msg.ReservedValue,
		Payload:       cloneBytes(msg.Payload),
	}
	if msg.Remote != nil {
		ipcMessage.Remote = msg.Remote.String()
	}
	return ipcMessage
}

func cloneDriverIPCMessages(messages []DriverIPCMessage) []DriverIPCMessage {
	if len(messages) == 0 {
		return nil
	}
	out := make([]DriverIPCMessage, len(messages))
	for i, msg := range messages {
		out[i] = msg
		out[i].Payload = cloneBytes(msg.Payload)
	}
	return out
}

func (msg DriverIPCMessage) message() Message {
	out := Message{
		StreamID:      msg.StreamID,
		SessionID:     msg.SessionID,
		TermID:        msg.TermID,
		TermOffset:    msg.TermOffset,
		Position:      msg.Position,
		Sequence:      msg.Sequence,
		ReservedValue: msg.ReservedValue,
		Payload:       cloneBytes(msg.Payload),
	}
	if msg.Remote != "" {
		out.Remote = driverNetAddr(msg.Remote)
	}
	return out
}

func writeDriverSubscriptionImageMessage(image *DriverSubscriptionImage, msg Message) error {
	if image == nil {
		return ErrDriverIPCClosed
	}
	if err := image.OfferMessage(msg); err != nil {
		if errors.Is(err, ErrIPCRingFull) || errors.Is(err, ErrIPCRingMessageTooLarge) {
			return fmt.Errorf("%w: subscription image cannot accept message: %w", ErrBackPressure, err)
		}
		return err
	}
	return nil
}

func decodeDriverIPCCommand(payload []byte) (DriverIPCCommand, error) {
	var command DriverIPCCommand
	if err := json.Unmarshal(payload, &command); err != nil {
		return DriverIPCCommand{}, fmt.Errorf("%w: decode command: %w", ErrDriverIPCProtocol, err)
	}
	if err := validateDriverIPCCommand(command); err != nil {
		return DriverIPCCommand{}, err
	}
	return command, nil
}

func decodeDriverIPCEvent(payload []byte) (DriverIPCEvent, error) {
	var event DriverIPCEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return DriverIPCEvent{}, fmt.Errorf("%w: decode event: %w", ErrDriverIPCProtocol, err)
	}
	if err := validateDriverIPCEvent(event); err != nil {
		return DriverIPCEvent{}, err
	}
	return event, nil
}

func validateDriverIPCCommand(command DriverIPCCommand) error {
	if command.Version != DriverIPCProtocolVersion {
		return fmt.Errorf("%w: unsupported command version %d", ErrDriverIPCProtocol, command.Version)
	}
	if command.CorrelationID == 0 {
		return fmt.Errorf("%w: command correlation id is required", ErrDriverIPCProtocol)
	}
	if command.Type == "" {
		return fmt.Errorf("%w: command type is required", ErrDriverIPCProtocol)
	}
	return nil
}

func validateDriverIPCEvent(event DriverIPCEvent) error {
	if event.Version != DriverIPCProtocolVersion {
		return fmt.Errorf("%w: unsupported event version %d", ErrDriverIPCProtocol, event.Version)
	}
	if event.Type == "" {
		return fmt.Errorf("%w: event type is required", ErrDriverIPCProtocol)
	}
	return nil
}

func driverIPCErrorEvent(command DriverIPCCommand, err error) DriverIPCEvent {
	message := ""
	if err != nil {
		message = err.Error()
	}
	return DriverIPCEvent{
		CorrelationID: command.CorrelationID,
		Type:          DriverIPCEventCommandError,
		CommandType:   command.Type,
		ClientID:      command.ClientID,
		ResourceID:    command.ResourceID,
		Error:         message,
	}
}
