package bunshin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const DriverIPCProtocolVersion = 1

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
	closeOnce         sync.Once
	closed            atomic.Bool
}

type DriverIPCCommand struct {
	Version       int                         `json:"version"`
	CorrelationID uint64                      `json:"correlation_id"`
	Type          DriverIPCCommandType        `json:"type"`
	ClientID      DriverClientID              `json:"client_id,omitempty"`
	ResourceID    DriverResourceID            `json:"resource_id,omitempty"`
	ClientName    string                      `json:"client_name,omitempty"`
	Publication   DriverIPCPublicationConfig  `json:"publication,omitempty"`
	Subscription  DriverIPCSubscriptionConfig `json:"subscription,omitempty"`
	Payload       []byte                      `json:"payload,omitempty"`
}

type DriverIPCPublicationConfig struct {
	StreamID               uint32 `json:"stream_id,omitempty"`
	SessionID              uint32 `json:"session_id,omitempty"`
	RemoteAddr             string `json:"remote_addr,omitempty"`
	MaxPayloadBytes        int    `json:"max_payload_bytes,omitempty"`
	MTUBytes               int    `json:"mtu_bytes,omitempty"`
	TermBufferLength       int    `json:"term_buffer_length,omitempty"`
	InitialTermID          int32  `json:"initial_term_id,omitempty"`
	PublicationWindowBytes int    `json:"publication_window_bytes,omitempty"`
}

type DriverIPCSubscriptionConfig struct {
	StreamID  uint32 `json:"stream_id,omitempty"`
	LocalAddr string `json:"local_addr,omitempty"`
}

type DriverIPCEvent struct {
	Version       int                    `json:"version"`
	CorrelationID uint64                 `json:"correlation_id,omitempty"`
	Type          DriverIPCEventType     `json:"type"`
	CommandType   DriverIPCCommandType   `json:"command_type,omitempty"`
	ClientID      DriverClientID         `json:"client_id,omitempty"`
	ResourceID    DriverResourceID       `json:"resource_id,omitempty"`
	Message       string                 `json:"message,omitempty"`
	Error         string                 `json:"error,omitempty"`
	Snapshot      *DriverSnapshot        `json:"snapshot,omitempty"`
	Directory     *DriverDirectoryReport `json:"directory,omitempty"`
	At            time.Time              `json:"at"`
}

type DriverIPCCommandHandler func(DriverIPCCommand) error

type DriverIPCEventHandler func(DriverIPCEvent) error

type DriverIPCServer struct {
	driver             *MediaDriver
	ipc                *DriverIPC
	terminated         bool
	clients            map[DriverClientID]*DriverClient
	publications       map[DriverResourceID]*DriverPublication
	subscriptions      map[DriverResourceID]*DriverSubscription
	publicationOwners  map[DriverResourceID]DriverClientID
	subscriptionOwners map[DriverResourceID]DriverClientID
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
		driver:             driver,
		ipc:                ipc,
		clients:            make(map[DriverClientID]*DriverClient),
		publications:       make(map[DriverResourceID]*DriverPublication),
		subscriptions:      make(map[DriverResourceID]*DriverSubscription),
		publicationOwners:  make(map[DriverResourceID]DriverClientID),
		subscriptionOwners: make(map[DriverResourceID]DriverClientID),
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
	return p.eventRing.OfferContext(ctx, payload, idle)
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

func (s *DriverIPCServer) Poll(ctx context.Context, limit int) (int, error) {
	if s == nil || s.ipc == nil {
		return 0, ErrDriverIPCClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return s.ipc.PollCommands(limit, func(command DriverIPCCommand) error {
		event := s.handleCommand(ctx, command)
		return s.ipc.SendEvent(ctx, event, nil)
	})
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
		s.subscriptions[subscription.ID()] = subscription
		s.subscriptionOwners[subscription.ID()] = command.ClientID
		event.Type = DriverIPCEventSubscriptionAdded
		event.ResourceID = subscription.ID()
		event.Message = subscription.LocalAddr().String()
		return event
	case DriverIPCCommandCloseSubscription:
		subscription, ok := s.subscriptions[command.ResourceID]
		if !ok || s.subscriptionOwners[command.ResourceID] != command.ClientID {
			return driverIPCErrorEvent(command, ErrDriverResourceNotFound)
		}
		if err := subscription.Close(ctx); err != nil {
			return driverIPCErrorEvent(command, err)
		}
		delete(s.subscriptions, command.ResourceID)
		delete(s.subscriptionOwners, command.ResourceID)
		event.Type = DriverIPCEventSubscriptionClosed
		return event
	case DriverIPCCommandSnapshot:
		snapshot, err := s.driver.Snapshot(ctx)
		if err != nil {
			return driverIPCErrorEvent(command, err)
		}
		event.Type = DriverIPCEventSnapshot
		event.Snapshot = &snapshot
		return event
	case DriverIPCCommandFlushReports:
		report, err := s.driver.FlushReports(ctx)
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
		event.Type = DriverIPCEventTerminated
		return event
	default:
		return driverIPCErrorEvent(command, fmt.Errorf("%w: unknown command type %q", ErrDriverIPCProtocol, command.Type))
	}
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
			delete(s.subscriptions, resourceID)
			delete(s.subscriptionOwners, resourceID)
		}
	}
}

func (cfg DriverIPCPublicationConfig) publicationConfig() PublicationConfig {
	return PublicationConfig{
		StreamID:               cfg.StreamID,
		SessionID:              cfg.SessionID,
		RemoteAddr:             cfg.RemoteAddr,
		MaxPayloadBytes:        cfg.MaxPayloadBytes,
		MTUBytes:               cfg.MTUBytes,
		TermBufferLength:       cfg.TermBufferLength,
		InitialTermID:          cfg.InitialTermID,
		PublicationWindowBytes: cfg.PublicationWindowBytes,
	}
}

func (cfg DriverIPCSubscriptionConfig) subscriptionConfig() SubscriptionConfig {
	return SubscriptionConfig{
		StreamID:  cfg.StreamID,
		LocalAddr: cfg.LocalAddr,
	}
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
