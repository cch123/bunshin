package bunshin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultDriverCommandBuffer  = 64
	defaultDriverClientTimeout  = 30 * time.Second
	defaultDriverCleanupPeriod  = time.Second
	defaultDriverStallThreshold = 50 * time.Millisecond
)

var driverResponseRingSequence atomic.Uint64

var (
	ErrDriverClosed              = errors.New("bunshin driver: closed")
	ErrDriverClientClosed        = errors.New("bunshin driver: client closed")
	ErrDriverResourceNotFound    = errors.New("bunshin driver: resource not found")
	ErrDriverExternalUnsupported = errors.New("bunshin driver: external resource unsupported")
	ErrDriverDirectoryActive     = errors.New("bunshin driver directory: active")
)

type DriverConfig struct {
	Directory             string
	CommandBuffer         int
	ClientTimeout         time.Duration
	CleanupInterval       time.Duration
	StallThreshold        time.Duration
	DirectoryStaleTimeout time.Duration
	TermBufferDirectory   string
	ThreadingMode         DriverThreadingMode
	SenderIdleStrategy    IdleStrategy
	ReceiverIdleStrategy  IdleStrategy
	Metrics               *Metrics
	Logger                Logger
}

type MediaDriver struct {
	commands  chan driverCommand
	done      chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
	closing   atomic.Bool
	directory *driverDirectory
	agentWG   sync.WaitGroup
}

type DriverClient struct {
	driver              *MediaDriver
	ipc                 *DriverIPC
	id                  DriverClientID
	name                string
	timeout             time.Duration
	responseRingPath    string
	cleanupResponseRing bool
	heartbeatCancel     context.CancelFunc
	heartbeatDone       chan struct{}
	heartbeatMu         sync.Mutex
	mu                  sync.Mutex
	closed              atomic.Bool
}

type DriverPublication struct {
	id          DriverResourceID
	client      *DriverClient
	publication *Publication
	localAddr   string
}

type DriverSubscription struct {
	id              DriverResourceID
	client          *DriverClient
	subscription    *Subscription
	localAddr       string
	dataRing        *IPCRing
	dataRingPath    string
	pendingMu       sync.Mutex
	pendingMessages []Message
}

type DriverClientID uint64

type DriverResourceID uint64

type DriverConnectionConfig struct {
	Directory            string
	CommandRingPath      string
	EventRingPath        string
	ClientName           string
	CommandRingCapacity  int
	EventRingCapacity    int
	Timeout              time.Duration
	HeartbeatInterval    time.Duration
	DisableAutoHeartbeat bool
}

type DriverCounters struct {
	CommandsProcessed       uint64
	CommandsFailed          uint64
	ClientsRegistered       uint64
	ClientsClosed           uint64
	PublicationsRegistered  uint64
	PublicationsClosed      uint64
	SubscriptionsRegistered uint64
	SubscriptionsClosed     uint64
	CleanupRuns             uint64
	StaleClientsClosed      uint64
	DutyCycles              uint64
	ConductorDutyCycles     uint64
	SenderDutyCycles        uint64
	ReceiverDutyCycles      uint64
	DutyCycleNanos          uint64
	DutyCycleMaxNanos       uint64
	Stalls                  uint64
	StallNanos              uint64
	StallMaxNanos           uint64
}

type DriverStatusCounters struct {
	ActiveClients         uint64
	ChannelEndpoints      uint64
	PublicationEndpoints  uint64
	SubscriptionEndpoints uint64
	ActivePublications    uint64
	ActiveSubscriptions   uint64
	Images                uint64
	AvailableImages       uint64
	UnavailableImages     uint64
	LaggingImages         uint64
}

type DriverSnapshot struct {
	Counters         DriverCounters
	StatusCounters   DriverStatusCounters
	CounterSnapshots []CounterSnapshot
	Clients          []DriverClientSnapshot
	Publications     []DriverPublicationSnapshot
	Subscriptions    []DriverSubscriptionSnapshot
	Images           []DriverImageSnapshot
	LossReports      []DriverLossReportSnapshot
}

type DriverClientSnapshot struct {
	ID            DriverClientID
	Name          string
	CreatedAt     time.Time
	LastHeartbeat time.Time
	Publications  int
	Subscriptions int
}

type DriverPublicationSnapshot struct {
	ID               DriverResourceID
	ClientID         DriverClientID
	StreamID         uint32
	SessionID        uint32
	RemoteAddr       string
	CreatedAt        time.Time
	TermBufferFiles  []string
	TermBufferMapped bool
}

type DriverSubscriptionSnapshot struct {
	ID                      DriverResourceID
	ClientID                DriverClientID
	StreamID                uint32
	LocalAddr               string
	CreatedAt               time.Time
	DataRingPath            string
	DataRingCapacity        int
	DataRingUsed            int
	DataRingFree            int
	DataRingReadPosition    uint64
	DataRingWritePosition   uint64
	DataRingMapped          bool
	DataRingPendingMessages int
}

type DriverSubscriptionDataRingStatus struct {
	Path                  string
	Capacity              int
	Used                  int
	Free                  int
	ReadPosition          uint64
	WritePosition         uint64
	Mapped                bool
	LocalPendingMessages  int
	ServerPendingMessages int
}

type DriverImageSnapshot struct {
	ResourceID           DriverResourceID
	ClientID             DriverClientID
	StreamID             uint32
	SessionID            uint32
	Source               string
	InitialTermID        int32
	TermBufferLength     int
	JoinPosition         int64
	CurrentPosition      int64
	ObservedPosition     int64
	LagBytes             int64
	LastSequence         uint64
	LastObservedSequence uint64
	AvailableAt          time.Time
	UnavailableAt        time.Time
}

type DriverLossReportSnapshot struct {
	ResourceID       DriverResourceID
	ClientID         DriverClientID
	StreamID         uint32
	SessionID        uint32
	Source           string
	ObservationCount uint64
	MissingMessages  uint64
	FirstObservation time.Time
	LastObservation  time.Time
}

type driverCommand struct {
	apply    func(*driverState) (any, error)
	reply    chan driverResult
	agent    driverAgentRole
	internal bool
}

type driverResult struct {
	value any
	err   error
}

type driverState struct {
	metrics             *Metrics
	logger              Logger
	clientTimeout       time.Duration
	cleanupInterval     time.Duration
	stallThreshold      time.Duration
	directory           *driverDirectory
	termBufferDirectory string

	nextClientID   DriverClientID
	nextResourceID DriverResourceID
	clients        map[DriverClientID]*driverClientState
	publications   map[DriverResourceID]*driverPublicationState
	subscriptions  map[DriverResourceID]*driverSubscriptionState
	counters       DriverCounters
	errorReports   []DriverErrorReport
}

type DriverThreadingMode string

const (
	DriverThreadingShared    DriverThreadingMode = "shared"
	DriverThreadingDedicated DriverThreadingMode = "dedicated"
)

type driverAgentRole string

const (
	driverAgentConductor driverAgentRole = "conductor"
	driverAgentSender    driverAgentRole = "sender"
	driverAgentReceiver  driverAgentRole = "receiver"
)

type driverClientState struct {
	id            DriverClientID
	name          string
	createdAt     time.Time
	lastHeartbeat time.Time
	publications  map[DriverResourceID]struct{}
	subscriptions map[DriverResourceID]struct{}
}

type driverPublicationState struct {
	id               DriverResourceID
	clientID         DriverClientID
	streamID         uint32
	sessionID        uint32
	remoteAddr       string
	createdAt        time.Time
	publication      *Publication
	termBufferFiles  []string
	termBufferMapped bool
}

type driverSubscriptionState struct {
	id           DriverResourceID
	clientID     DriverClientID
	streamID     uint32
	localAddr    string
	createdAt    time.Time
	subscription *Subscription
}

func StartMediaDriver(cfg DriverConfig) (*MediaDriver, error) {
	if cfg.CommandBuffer < 0 {
		return nil, invalidConfigf("invalid driver command buffer: %d", cfg.CommandBuffer)
	}
	if cfg.ClientTimeout < 0 {
		return nil, invalidConfigf("invalid driver client timeout: %s", cfg.ClientTimeout)
	}
	if cfg.CleanupInterval < 0 {
		return nil, invalidConfigf("invalid driver cleanup interval: %s", cfg.CleanupInterval)
	}
	if cfg.StallThreshold < 0 {
		return nil, invalidConfigf("invalid driver stall threshold: %s", cfg.StallThreshold)
	}
	if cfg.DirectoryStaleTimeout < 0 {
		return nil, invalidConfigf("invalid driver directory stale timeout: %s", cfg.DirectoryStaleTimeout)
	}
	if cfg.ThreadingMode == "" {
		cfg.ThreadingMode = DriverThreadingShared
	}
	switch cfg.ThreadingMode {
	case DriverThreadingShared, DriverThreadingDedicated:
	default:
		return nil, invalidConfigf("invalid driver threading mode: %s", cfg.ThreadingMode)
	}
	if cfg.CommandBuffer == 0 {
		cfg.CommandBuffer = defaultDriverCommandBuffer
	}
	if cfg.ClientTimeout == 0 {
		cfg.ClientTimeout = defaultDriverClientTimeout
	}
	if cfg.CleanupInterval == 0 {
		cfg.CleanupInterval = defaultDriverCleanupPeriod
	}
	if cfg.StallThreshold == 0 {
		cfg.StallThreshold = defaultDriverStallThreshold
	}
	if cfg.DirectoryStaleTimeout == 0 {
		cfg.DirectoryStaleTimeout = defaultDriverProcessStaleTimeout
	}
	directory, err := openDriverDirectory(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.TermBufferDirectory == "" && directory != nil {
		cfg.TermBufferDirectory = filepath.Join(directory.layout.BuffersDirectory, "publications")
	}

	driver := &MediaDriver{
		commands:  make(chan driverCommand, cfg.CommandBuffer),
		done:      make(chan struct{}),
		closed:    make(chan struct{}),
		directory: directory,
	}
	go driver.run(cfg, directory)
	driver.startAgentLoops(cfg)

	logEvent(context.Background(), cfg.Logger, LogEvent{
		Level:     LogLevelInfo,
		Component: "media_driver",
		Operation: "start",
		Message:   "media driver started",
		Fields: map[string]any{
			"command_buffer":   cfg.CommandBuffer,
			"client_timeout":   cfg.ClientTimeout,
			"cleanup_interval": cfg.CleanupInterval,
			"directory":        cfg.Directory,
			"term_buffers":     cfg.TermBufferDirectory,
			"threading_mode":   cfg.ThreadingMode,
		},
	})
	return driver, nil
}

func (d *MediaDriver) NewClient(ctx context.Context, name string) (*DriverClient, error) {
	value, err := d.dispatch(ctx, func(state *driverState) (any, error) {
		now := time.Now()
		state.nextClientID++
		id := state.nextClientID
		if name == "" {
			name = fmt.Sprintf("client-%d", id)
		}
		state.clients[id] = &driverClientState{
			id:            id,
			name:          name,
			createdAt:     now,
			lastHeartbeat: now,
			publications:  make(map[DriverResourceID]struct{}),
			subscriptions: make(map[DriverResourceID]struct{}),
		}
		state.counters.ClientsRegistered++
		state.log(LogLevelInfo, "client", "client registered", map[string]any{
			"client_id": id,
			"name":      name,
		}, nil)
		return &DriverClient{
			driver: d,
			id:     id,
			name:   name,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return value.(*DriverClient), nil
}

func ConnectMediaDriver(ctx context.Context, cfg DriverConnectionConfig) (*DriverClient, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cfg.Timeout < 0 {
		return nil, invalidConfigf("invalid driver connection timeout: %s", cfg.Timeout)
	}
	if cfg.HeartbeatInterval < 0 {
		return nil, invalidConfigf("invalid driver connection heartbeat interval: %s", cfg.HeartbeatInterval)
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultDriverClientTimeout
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = defaultDriverClientTimeout / 3
	}
	cleanupResponseRing := false
	if cfg.Directory != "" && cfg.EventRingPath == "" {
		layout, err := ResolveDriverDirectoryLayout(cfg.Directory)
		if err != nil {
			return nil, err
		}
		cfg.EventRingPath = nextDriverResponseRingPath(layout.ClientsDirectory)
		cleanupResponseRing = true
	}
	ipc, err := OpenDriverIPC(DriverIPCConfig{
		Directory:           cfg.Directory,
		CommandRingPath:     cfg.CommandRingPath,
		EventRingPath:       cfg.EventRingPath,
		CommandRingCapacity: cfg.CommandRingCapacity,
		EventRingCapacity:   cfg.EventRingCapacity,
	})
	if err != nil {
		return nil, err
	}
	client := &DriverClient{
		ipc:                 ipc,
		name:                cfg.ClientName,
		timeout:             cfg.Timeout,
		responseRingPath:    cfg.EventRingPath,
		cleanupResponseRing: cleanupResponseRing,
	}
	event, err := client.sendIPCCommand(ctx, DriverIPCCommand{
		Type:       DriverIPCCommandOpenClient,
		ClientName: cfg.ClientName,
	})
	if err != nil {
		_ = ipc.Close()
		client.cleanupOwnedResponseRing()
		return nil, err
	}
	client.id = event.ClientID
	client.name = event.Message
	if !cfg.DisableAutoHeartbeat {
		client.startAutoHeartbeat(cfg.HeartbeatInterval)
	}
	return client, nil
}

func nextDriverResponseRingPath(directory string) string {
	sequence := driverResponseRingSequence.Add(1)
	name := fmt.Sprintf("client-events-%d-%d-%d.ring", os.Getpid(), time.Now().UnixNano(), sequence)
	return filepath.Join(directory, name)
}

func (d *MediaDriver) Snapshot(ctx context.Context) (DriverSnapshot, error) {
	value, err := d.dispatch(ctx, func(state *driverState) (any, error) {
		return state.snapshot(), nil
	})
	if err != nil {
		return DriverSnapshot{}, err
	}
	return value.(DriverSnapshot), nil
}

func (d *MediaDriver) Directory() (DriverDirectoryLayout, bool) {
	if d == nil || d.directory == nil {
		return DriverDirectoryLayout{}, false
	}
	return d.directory.layout, true
}

func (d *MediaDriver) FlushReports(ctx context.Context) (DriverDirectoryReport, error) {
	if d == nil || d.directory == nil {
		return DriverDirectoryReport{}, ErrDriverDirectoryUnavailable
	}
	value, err := d.dispatch(ctx, func(state *driverState) (any, error) {
		return state.flushDriverDirectory(time.Now(), DriverDirectoryStatusActive)
	})
	if err != nil {
		return DriverDirectoryReport{}, err
	}
	return value.(DriverDirectoryReport), nil
}

func (d *MediaDriver) Close() error {
	d.closeOnce.Do(func() {
		d.closing.Store(true)
		close(d.done)
		<-d.closed
		d.agentWG.Wait()
	})
	return nil
}

func (d *MediaDriver) dispatch(ctx context.Context, apply func(*driverState) (any, error)) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if d.closing.Load() {
		return nil, ErrDriverClosed
	}
	cmd := driverCommand{
		apply: apply,
		reply: make(chan driverResult, 1),
		agent: driverAgentConductor,
	}
	select {
	case d.commands <- cmd:
	case <-d.done:
		return nil, ErrDriverClosed
	case <-d.closed:
		return nil, ErrDriverClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case result := <-cmd.reply:
		return result.value, result.err
	case <-d.closed:
		return nil, ErrDriverClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (d *MediaDriver) run(cfg DriverConfig, directory *driverDirectory) {
	state := &driverState{
		metrics:             cfg.Metrics,
		logger:              cfg.Logger,
		clientTimeout:       cfg.ClientTimeout,
		cleanupInterval:     cfg.CleanupInterval,
		stallThreshold:      cfg.StallThreshold,
		directory:           directory,
		termBufferDirectory: cfg.TermBufferDirectory,
		clients:             make(map[DriverClientID]*driverClientState),
		publications:        make(map[DriverResourceID]*driverPublicationState),
		subscriptions:       make(map[DriverResourceID]*driverSubscriptionState),
	}
	if directory != nil {
		if _, err := state.flushDriverDirectory(time.Now(), DriverDirectoryStatusActive); err != nil {
			state.recordDriverError(LogLevelError, "directory", "initial driver report flush failed", nil, err)
		}
	}

	ticker := time.NewTicker(cfg.CleanupInterval)
	defer ticker.Stop()
	defer close(d.closed)

	for {
		select {
		case command := <-d.commands:
			start := time.Now()
			if command.agent == "" {
				command.agent = driverAgentConductor
			}
			if !command.internal {
				state.counters.CommandsProcessed++
			}
			value, err := command.apply(state)
			if err != nil && !command.internal {
				state.counters.CommandsFailed++
				state.recordDriverError(LogLevelError, "command", "driver command failed", nil, err)
			}
			state.recordDutyCycle(command.agent, time.Since(start))
			command.reply <- driverResult{value: value, err: err}
		case now := <-ticker.C:
			start := time.Now()
			state.cleanup(now)
			state.recordDutyCycle(driverAgentConductor, time.Since(start))
		case <-d.done:
			start := time.Now()
			state.closeAll()
			state.log(LogLevelInfo, "close", "media driver closed", nil, nil)
			state.recordDutyCycle(driverAgentConductor, time.Since(start))
			if directory != nil {
				if _, err := state.flushDriverDirectory(time.Now(), DriverDirectoryStatusClosed); err != nil {
					state.recordDriverError(LogLevelError, "directory", "final driver report flush failed", nil, err)
				}
			}
			return
		}
	}
}

func (d *MediaDriver) startAgentLoops(cfg DriverConfig) {
	if cfg.ThreadingMode != DriverThreadingDedicated {
		return
	}
	d.startAgentLoop(driverAgentSender, cfg.SenderIdleStrategy)
	d.startAgentLoop(driverAgentReceiver, cfg.ReceiverIdleStrategy)
}

func (d *MediaDriver) startAgentLoop(role driverAgentRole, idle IdleStrategy) {
	if idle == nil {
		idle = NewDefaultBackoffIdleStrategy()
	}
	d.agentWG.Add(1)
	go func() {
		defer d.agentWG.Done()
		for {
			select {
			case <-d.done:
				return
			default:
			}

			workCount, err := d.dispatchAgentDuty(role)
			if err != nil {
				if errors.Is(err, ErrDriverClosed) {
					return
				}
				workCount = 0
			}
			idle.Idle(workCount)
		}
	}()
}

func (d *MediaDriver) dispatchAgentDuty(role driverAgentRole) (int, error) {
	if d.closing.Load() {
		return 0, ErrDriverClosed
	}
	reply := make(chan driverResult, 1)
	command := driverCommand{
		agent:    role,
		internal: true,
		reply:    reply,
		apply: func(state *driverState) (any, error) {
			switch role {
			case driverAgentSender:
				return state.senderAgentDuty(), nil
			case driverAgentReceiver:
				return state.receiverAgentDuty(), nil
			default:
				return 0, invalidConfigf("invalid driver agent role: %s", role)
			}
		},
	}
	select {
	case d.commands <- command:
	case <-d.done:
		return 0, ErrDriverClosed
	case <-d.closed:
		return 0, ErrDriverClosed
	}
	select {
	case result := <-reply:
		if result.err != nil {
			return 0, result.err
		}
		workCount, _ := result.value.(int)
		return workCount, nil
	case <-d.done:
		return 0, ErrDriverClosed
	case <-d.closed:
		return 0, ErrDriverClosed
	}
}

func (c *DriverClient) ID() DriverClientID {
	return c.id
}

func (c *DriverClient) Name() string {
	return c.name
}

func (c *DriverClient) Heartbeat(ctx context.Context) error {
	if c.closed.Load() {
		return ErrDriverClientClosed
	}
	if c.ipc != nil {
		_, err := c.sendIPCCommand(ctx, DriverIPCCommand{
			Type:     DriverIPCCommandHeartbeatClient,
			ClientID: c.id,
		})
		return err
	}
	_, err := c.driver.dispatch(ctx, func(state *driverState) (any, error) {
		client := state.clients[c.id]
		if client == nil {
			return nil, ErrDriverClientClosed
		}
		client.lastHeartbeat = time.Now()
		return nil, nil
	})
	return err
}

func (c *DriverClient) startAutoHeartbeat(interval time.Duration) {
	if c == nil || c.ipc == nil || interval <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.heartbeatMu.Lock()
	if c.heartbeatCancel != nil {
		c.heartbeatMu.Unlock()
		cancel()
		return
	}
	c.heartbeatCancel = cancel
	c.heartbeatDone = make(chan struct{})
	done := c.heartbeatDone
	c.heartbeatMu.Unlock()
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		if err := c.Heartbeat(ctx); errors.Is(err, ErrDriverClientClosed) || errors.Is(err, ErrDriverIPCClosed) {
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := c.Heartbeat(ctx)
				if errors.Is(err, ErrDriverClientClosed) || errors.Is(err, ErrDriverIPCClosed) {
					return
				}
			}
		}
	}()
}

func (c *DriverClient) stopAutoHeartbeat() {
	if c == nil {
		return
	}
	c.heartbeatMu.Lock()
	cancel := c.heartbeatCancel
	done := c.heartbeatDone
	c.heartbeatCancel = nil
	c.heartbeatDone = nil
	c.heartbeatMu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	if done != nil {
		<-done
	}
}

func (c *DriverClient) AddPublication(ctx context.Context, cfg PublicationConfig) (*DriverPublication, error) {
	if c.closed.Load() {
		return nil, ErrDriverClientClosed
	}
	if c.ipc != nil {
		event, err := c.sendIPCCommand(ctx, DriverIPCCommand{
			Type:     DriverIPCCommandAddPublication,
			ClientID: c.id,
			Publication: DriverIPCPublicationConfig{
				StreamID:               cfg.StreamID,
				SessionID:              cfg.SessionID,
				RemoteAddr:             cfg.RemoteAddr,
				MaxPayloadBytes:        cfg.MaxPayloadBytes,
				MTUBytes:               cfg.MTUBytes,
				TermBufferLength:       cfg.TermBufferLength,
				InitialTermID:          cfg.InitialTermID,
				PublicationWindowBytes: cfg.PublicationWindowBytes,
			},
		})
		if err != nil {
			return nil, err
		}
		return &DriverPublication{
			id:        event.ResourceID,
			client:    c,
			localAddr: event.Message,
		}, nil
	}
	value, err := c.driver.dispatch(ctx, func(state *driverState) (any, error) {
		client := state.clients[c.id]
		if client == nil {
			return nil, ErrDriverClientClosed
		}
		client.lastHeartbeat = time.Now()
		if cfg.Metrics == nil {
			cfg.Metrics = state.metrics
		}
		if cfg.Logger == nil {
			cfg.Logger = state.logger
		}
		state.nextResourceID++
		id := state.nextResourceID
		if cfg.TermBufferDirectory == "" && state.termBufferDirectory != "" {
			cfg.TermBufferDirectory = filepath.Join(state.termBufferDirectory, fmt.Sprintf("publication-%d", id))
		}
		pub, err := DialPublication(cfg)
		if err != nil {
			return nil, err
		}

		resource := &driverPublicationState{
			id:               id,
			clientID:         c.id,
			streamID:         pub.streamID,
			sessionID:        pub.sessionID,
			remoteAddr:       cfg.RemoteAddr,
			createdAt:        time.Now(),
			publication:      pub,
			termBufferFiles:  pub.terms.mappedFiles(),
			termBufferMapped: pub.terms.isMapped(),
		}
		state.publications[id] = resource
		client.publications[id] = struct{}{}
		state.counters.PublicationsRegistered++
		state.log(LogLevelInfo, "publication", "publication registered", map[string]any{
			"client_id":    c.id,
			"resource_id":  id,
			"stream_id":    pub.streamID,
			"session_id":   pub.sessionID,
			"remote_addr":  cfg.RemoteAddr,
			"term_buffers": resource.termBufferFiles,
		}, nil)
		return &DriverPublication{
			id:          id,
			client:      c,
			publication: pub,
			localAddr:   pub.LocalAddr().String(),
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return value.(*DriverPublication), nil
}

func (c *DriverClient) AddSubscription(ctx context.Context, cfg SubscriptionConfig) (*DriverSubscription, error) {
	if c.closed.Load() {
		return nil, ErrDriverClientClosed
	}
	if c.ipc != nil {
		event, err := c.sendIPCCommand(ctx, DriverIPCCommand{
			Type:     DriverIPCCommandAddSubscription,
			ClientID: c.id,
			Subscription: DriverIPCSubscriptionConfig{
				StreamID:         cfg.StreamID,
				LocalAddr:        cfg.LocalAddr,
				DataRingCapacity: cfg.DriverDataRingCapacity,
			},
		})
		if err != nil {
			return nil, err
		}
		var dataRing *IPCRing
		if event.DataRingPath != "" {
			dataRing, err = OpenIPCRing(IPCRingConfig{Path: event.DataRingPath})
			if err != nil {
				_ = c.CloseSubscription(ctx, event.ResourceID)
				return nil, err
			}
		}
		return &DriverSubscription{
			id:           event.ResourceID,
			client:       c,
			localAddr:    event.Message,
			dataRing:     dataRing,
			dataRingPath: event.DataRingPath,
		}, nil
	}
	value, err := c.driver.dispatch(ctx, func(state *driverState) (any, error) {
		client := state.clients[c.id]
		if client == nil {
			return nil, ErrDriverClientClosed
		}
		client.lastHeartbeat = time.Now()
		if cfg.Metrics == nil {
			cfg.Metrics = state.metrics
		}
		if cfg.Logger == nil {
			cfg.Logger = state.logger
		}
		sub, err := ListenSubscription(cfg)
		if err != nil {
			return nil, err
		}

		state.nextResourceID++
		id := state.nextResourceID
		resource := &driverSubscriptionState{
			id:           id,
			clientID:     c.id,
			streamID:     sub.streamID,
			localAddr:    sub.LocalAddr().String(),
			createdAt:    time.Now(),
			subscription: sub,
		}
		state.subscriptions[id] = resource
		client.subscriptions[id] = struct{}{}
		state.counters.SubscriptionsRegistered++
		state.log(LogLevelInfo, "subscription", "subscription registered", map[string]any{
			"client_id":   c.id,
			"resource_id": id,
			"stream_id":   sub.streamID,
			"local_addr":  resource.localAddr,
		}, nil)
		return &DriverSubscription{
			id:           id,
			client:       c,
			subscription: sub,
			localAddr:    sub.LocalAddr().String(),
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return value.(*DriverSubscription), nil
}

func (c *DriverClient) ClosePublication(ctx context.Context, id DriverResourceID) error {
	if c.closed.Load() {
		return ErrDriverClientClosed
	}
	if c.ipc != nil {
		_, err := c.sendIPCCommand(ctx, DriverIPCCommand{
			Type:       DriverIPCCommandClosePublication,
			ClientID:   c.id,
			ResourceID: id,
		})
		return err
	}
	_, err := c.driver.dispatch(ctx, func(state *driverState) (any, error) {
		if state.clients[c.id] == nil {
			return nil, ErrDriverClientClosed
		}
		return nil, state.closePublication(id, c.id)
	})
	return err
}

func (c *DriverClient) CloseSubscription(ctx context.Context, id DriverResourceID) error {
	if c.closed.Load() {
		return ErrDriverClientClosed
	}
	if c.ipc != nil {
		_, err := c.sendIPCCommand(ctx, DriverIPCCommand{
			Type:       DriverIPCCommandCloseSubscription,
			ClientID:   c.id,
			ResourceID: id,
		})
		return err
	}
	_, err := c.driver.dispatch(ctx, func(state *driverState) (any, error) {
		if state.clients[c.id] == nil {
			return nil, ErrDriverClientClosed
		}
		return nil, state.closeSubscription(id, c.id)
	})
	return err
}

func (c *DriverClient) Snapshot(ctx context.Context) (DriverSnapshot, error) {
	if c.closed.Load() {
		return DriverSnapshot{}, ErrDriverClientClosed
	}
	if c.ipc != nil {
		event, err := c.sendIPCCommand(ctx, DriverIPCCommand{
			Type: DriverIPCCommandSnapshot,
		})
		if err != nil {
			return DriverSnapshot{}, err
		}
		if event.Snapshot == nil {
			return DriverSnapshot{}, ErrDriverIPCProtocol
		}
		return *event.Snapshot, nil
	}
	return c.driver.Snapshot(ctx)
}

func (c *DriverClient) Close(ctx context.Context) error {
	if c.closed.Load() {
		return nil
	}
	if c.ipc != nil {
		c.stopAutoHeartbeat()
		_, err := c.sendIPCCommand(ctx, DriverIPCCommand{
			Type:     DriverIPCCommandCloseClient,
			ClientID: c.id,
		})
		if err == nil || errors.Is(err, ErrDriverClientClosed) {
			c.closed.Store(true)
			closeErr := c.ipc.Close()
			c.cleanupOwnedResponseRing()
			if err == nil {
				return closeErr
			}
		}
		return err
	}
	_, err := c.driver.dispatch(ctx, func(state *driverState) (any, error) {
		return nil, state.closeClient(c.id, false, true)
	})
	if err == nil || errors.Is(err, ErrDriverClientClosed) {
		c.closed.Store(true)
	}
	return err
}

func (c *DriverClient) cleanupOwnedResponseRing() {
	if c == nil || !c.cleanupResponseRing || c.responseRingPath == "" {
		return
	}
	_ = os.Remove(c.responseRingPath)
	c.cleanupResponseRing = false
}

func (p *DriverPublication) ID() DriverResourceID {
	return p.id
}

func (p *DriverPublication) Send(ctx context.Context, payload []byte) error {
	if p == nil || p.client == nil {
		return ErrDriverClientClosed
	}
	if p.publication == nil {
		_, err := p.client.sendIPCCommand(ctx, DriverIPCCommand{
			Type:       DriverIPCCommandSendPublication,
			ClientID:   p.client.id,
			ResourceID: p.id,
			Payload:    cloneBytes(payload),
		})
		return err
	}
	return p.publication.Send(ctx, payload)
}

func (p *DriverPublication) LocalAddr() net.Addr {
	if p == nil {
		return nil
	}
	if p.publication == nil {
		return driverNetAddr(p.localAddr)
	}
	return p.publication.LocalAddr()
}

func (p *DriverPublication) Close(ctx context.Context) error {
	return p.client.ClosePublication(ctx, p.id)
}

func (s *DriverSubscription) ID() DriverResourceID {
	return s.id
}

func (s *DriverSubscription) Serve(ctx context.Context, handler Handler) error {
	if s == nil {
		return ErrDriverClientClosed
	}
	if s.subscription == nil {
		return ErrDriverExternalUnsupported
	}
	return s.subscription.Serve(ctx, handler)
}

func (s *DriverSubscription) Poll(ctx context.Context, handler Handler) (int, error) {
	if s == nil {
		return 0, ErrDriverClientClosed
	}
	if s.subscription == nil {
		return s.pollExternal(ctx, 1, maxFrameFragments, handler, nil)
	}
	return s.subscription.Poll(ctx, handler)
}

func (s *DriverSubscription) PollN(ctx context.Context, fragmentLimit int, handler Handler) (int, error) {
	if s == nil {
		return 0, ErrDriverClientClosed
	}
	if s.subscription == nil {
		return s.pollExternal(ctx, fragmentLimit, fragmentLimit, handler, nil)
	}
	return s.subscription.PollN(ctx, fragmentLimit, handler)
}

func (s *DriverSubscription) ControlledPoll(ctx context.Context, handler ControlledHandler) (int, error) {
	if s == nil {
		return 0, ErrDriverClientClosed
	}
	if s.subscription == nil {
		return s.controlledPollExternal(ctx, 1, handler)
	}
	return s.subscription.ControlledPoll(ctx, handler)
}

func (s *DriverSubscription) ControlledPollN(ctx context.Context, fragmentLimit int, handler ControlledHandler) (int, error) {
	if s == nil {
		return 0, ErrDriverClientClosed
	}
	if s.subscription == nil {
		return s.controlledPollExternal(ctx, fragmentLimit, handler)
	}
	return s.subscription.ControlledPollN(ctx, fragmentLimit, handler)
}

func (s *DriverSubscription) LocalAddr() net.Addr {
	if s == nil {
		return nil
	}
	if s.subscription == nil {
		return driverNetAddr(s.localAddr)
	}
	return s.subscription.LocalAddr()
}

func (s *DriverSubscription) LossReports() []LossReport {
	if s == nil {
		return nil
	}
	if s.subscription == nil {
		return s.externalLossReports()
	}
	return s.subscription.LossReports()
}

func (s *DriverSubscription) LagReports() []SubscriptionLagReport {
	if s == nil {
		return nil
	}
	if s.subscription == nil {
		return s.externalLagReports()
	}
	return s.subscription.LagReports()
}

func (s *DriverSubscription) Images() []ImageSnapshot {
	if s == nil {
		return nil
	}
	if s.subscription == nil {
		return s.externalImages()
	}
	return s.subscription.Images()
}

func (s *DriverSubscription) DataRingSnapshot() (IPCRingSnapshot, bool, error) {
	if s == nil {
		return IPCRingSnapshot{}, false, ErrDriverClientClosed
	}
	if s.dataRing == nil {
		return IPCRingSnapshot{}, false, nil
	}
	snapshot, err := s.dataRing.Snapshot()
	return snapshot, true, err
}

func (s *DriverSubscription) DataRingStatus(ctx context.Context) (DriverSubscriptionDataRingStatus, bool, error) {
	if s == nil {
		return DriverSubscriptionDataRingStatus{}, false, ErrDriverClientClosed
	}
	status := DriverSubscriptionDataRingStatus{
		LocalPendingMessages: s.PendingMessages(),
	}
	ok := false
	if s.dataRing != nil {
		snapshot, err := s.dataRing.Snapshot()
		if err != nil {
			return DriverSubscriptionDataRingStatus{}, false, err
		}
		status.Path = snapshot.Path
		status.Capacity = snapshot.Capacity
		status.Used = snapshot.Used
		status.Free = snapshot.Free
		status.ReadPosition = snapshot.ReadPosition
		status.WritePosition = snapshot.WritePosition
		status.Mapped = true
		ok = true
	}
	if s.subscription != nil {
		return status, ok, nil
	}
	if s.client == nil {
		return status, ok, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	snapshot, err := s.client.Snapshot(ctx)
	if err != nil {
		return status, ok, err
	}
	for _, subscription := range snapshot.Subscriptions {
		if subscription.ID != s.id {
			continue
		}
		if !ok {
			status.Path = subscription.DataRingPath
			status.Capacity = subscription.DataRingCapacity
			status.Used = subscription.DataRingUsed
			status.Free = subscription.DataRingFree
			status.ReadPosition = subscription.DataRingReadPosition
			status.WritePosition = subscription.DataRingWritePosition
			status.Mapped = subscription.DataRingMapped
			ok = subscription.DataRingMapped || subscription.DataRingPath != ""
		}
		status.ServerPendingMessages = subscription.DataRingPendingMessages
		return status, ok, nil
	}
	return status, ok, nil
}

// PendingMessages reports the number of locally buffered external fallback messages.
func (s *DriverSubscription) PendingMessages() int {
	if s == nil {
		return 0
	}
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	return len(s.pendingMessages)
}

func (s *DriverSubscription) Close(ctx context.Context) error {
	if s == nil || s.client == nil {
		return ErrDriverClientClosed
	}
	err := s.client.CloseSubscription(ctx, s.id)
	if s.subscription == nil && (err == nil || errors.Is(err, ErrDriverResourceNotFound)) {
		err = errors.Join(err, s.closeExternalDataRing())
	}
	return err
}

func (s *DriverSubscription) pollDriverOwned(ctx context.Context, messageLimit, fragmentLimit int, handler Handler) (int, error) {
	if s == nil || s.subscription == nil {
		return 0, ErrDriverResourceNotFound
	}
	return s.subscription.poll(ctx, messageLimit, fragmentLimit, handler, nil)
}

func (s *DriverSubscription) externalImages() []ImageSnapshot {
	snapshot, ok := s.externalSnapshot()
	if !ok {
		return nil
	}
	images := make([]ImageSnapshot, 0)
	for _, image := range snapshot.Images {
		if image.ResourceID != s.id {
			continue
		}
		images = append(images, ImageSnapshot{
			StreamID:             image.StreamID,
			SessionID:            image.SessionID,
			Source:               image.Source,
			InitialTermID:        image.InitialTermID,
			TermBufferLength:     image.TermBufferLength,
			JoinPosition:         image.JoinPosition,
			CurrentPosition:      image.CurrentPosition,
			ObservedPosition:     image.ObservedPosition,
			LagBytes:             image.LagBytes,
			LastSequence:         image.LastSequence,
			LastObservedSequence: image.LastObservedSequence,
			AvailableAt:          image.AvailableAt,
			UnavailableAt:        image.UnavailableAt,
		})
	}
	return images
}

func (s *DriverSubscription) externalLagReports() []SubscriptionLagReport {
	snapshot, ok := s.externalSnapshot()
	if !ok {
		return nil
	}
	reports := make([]SubscriptionLagReport, 0)
	for _, image := range snapshot.Images {
		if image.ResourceID != s.id {
			continue
		}
		reports = append(reports, SubscriptionLagReport{
			StreamID:             image.StreamID,
			SessionID:            image.SessionID,
			Source:               image.Source,
			CurrentPosition:      image.CurrentPosition,
			ObservedPosition:     image.ObservedPosition,
			LagBytes:             image.LagBytes,
			LastSequence:         image.LastSequence,
			LastObservedSequence: image.LastObservedSequence,
			AvailableAt:          image.AvailableAt,
			UnavailableAt:        image.UnavailableAt,
		})
	}
	return reports
}

func (s *DriverSubscription) externalLossReports() []LossReport {
	snapshot, ok := s.externalSnapshot()
	if !ok {
		return nil
	}
	reports := make([]LossReport, 0)
	for _, report := range snapshot.LossReports {
		if report.ResourceID != s.id {
			continue
		}
		reports = append(reports, LossReport{
			StreamID:         report.StreamID,
			SessionID:        report.SessionID,
			Source:           report.Source,
			ObservationCount: report.ObservationCount,
			MissingMessages:  report.MissingMessages,
			FirstObservation: report.FirstObservation,
			LastObservation:  report.LastObservation,
		})
	}
	return reports
}

func (s *DriverSubscription) externalSnapshot() (DriverSnapshot, bool) {
	if s == nil || s.client == nil {
		return DriverSnapshot{}, false
	}
	snapshot, err := s.client.Snapshot(context.Background())
	if err != nil {
		return DriverSnapshot{}, false
	}
	return snapshot, true
}

func (s *DriverSubscription) closeExternalDataRing() error {
	if s == nil || s.dataRing == nil {
		return nil
	}
	err := s.dataRing.Close()
	s.dataRing = nil
	return err
}

func (s *DriverSubscription) controlledPollExternal(ctx context.Context, fragmentLimit int, handler ControlledHandler) (int, error) {
	if s == nil || s.client == nil {
		return 0, ErrDriverClientClosed
	}
	if handler == nil {
		return 0, errors.New("controlled handler is required")
	}
	if fragmentLimit <= 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	idle := NewDefaultBackoffIdleStrategy()
	delivered := 0
	for delivered < fragmentLimit {
		select {
		case <-ctx.Done():
			if delivered > 0 {
				return delivered, nil
			}
			return 0, ctx.Err()
		default:
		}
		n, ok, stop, err := s.controlledPollExternalPending(ctx, handler)
		if err != nil {
			return delivered + n, err
		}
		if ok {
			delivered += n
			if stop {
				return delivered, nil
			}
			continue
		}
		if s.dataRing != nil {
			n, ok, stop, err := s.controlledPollExternalDataRing(ctx, handler)
			if err != nil {
				return delivered + n, err
			}
			if ok {
				delivered += n
				if stop {
					return delivered, nil
				}
				continue
			}
		}
		event, err := s.client.sendIPCCommand(ctx, DriverIPCCommand{
			Type:          DriverIPCCommandPollSubscription,
			ClientID:      s.client.id,
			ResourceID:    s.id,
			MessageLimit:  1,
			FragmentLimit: fragmentLimit - delivered,
		})
		if err != nil {
			if delivered > 0 && isContextDoneError(err) {
				return delivered, nil
			}
			return delivered, err
		}
		if event.Type != DriverIPCEventSubscriptionPolled {
			return delivered, ErrDriverIPCProtocol
		}
		if event.BackPressured && event.MessageCount == 0 && len(event.Messages) == 0 {
			idle.Idle(0)
			continue
		}
		if event.MessageCount == 0 && len(event.Messages) == 0 {
			idle.Idle(0)
			continue
		}
		if s.dataRing != nil && event.MessageCount > 0 {
			n, ok, stop, err := s.controlledPollExternalDataRing(ctx, handler)
			if err != nil {
				return delivered + n, err
			}
			if !ok {
				return delivered, ErrDriverIPCProtocol
			}
			delivered += n
			if stop {
				return delivered, nil
			}
			continue
		}
		if len(event.Messages) > 0 {
			s.enqueueExternalPendingIPCMessages(event.Messages)
			n, ok, stop, err := s.controlledPollExternalPending(ctx, handler)
			if err != nil {
				return delivered + n, err
			}
			if !ok {
				return delivered, ErrDriverIPCProtocol
			}
			delivered += n
			if stop {
				return delivered, nil
			}
			continue
		}
		msg, err := s.readExternalMessage(event)
		if err != nil {
			return delivered, err
		}
		switch action := handler(ctx, msg); action {
		case ControlledPollContinue, ControlledPollCommit:
			delivered++
		case ControlledPollBreak:
			return delivered + 1, nil
		case ControlledPollAbort:
			return delivered, ErrControlledPollAbort
		default:
			return delivered, invalidConfigf("invalid controlled poll action: %d", action)
		}
	}
	return delivered, nil
}

func (s *DriverSubscription) pollExternal(ctx context.Context, messageLimit, fragmentLimit int, handler Handler, shouldStop func() bool) (int, error) {
	if s == nil || s.client == nil {
		return 0, ErrDriverClientClosed
	}
	if handler == nil {
		return 0, errors.New("handler is required")
	}
	if messageLimit <= 0 || fragmentLimit <= 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	idle := NewDefaultBackoffIdleStrategy()
	deliveredTotal := 0
	for deliveredTotal < messageLimit && deliveredTotal < fragmentLimit && !pollShouldStop(shouldStop) {
		select {
		case <-ctx.Done():
			if deliveredTotal > 0 {
				return deliveredTotal, nil
			}
			return 0, ctx.Err()
		default:
		}
		remainingMessages := messageLimit - deliveredTotal
		remainingFragments := fragmentLimit - deliveredTotal
		delivered, err := s.drainExternalPending(ctx, min(remainingMessages, remainingFragments), handler, shouldStop)
		if err != nil {
			return deliveredTotal + delivered, err
		}
		deliveredTotal += delivered
		if deliveredTotal >= messageLimit || deliveredTotal >= fragmentLimit || pollShouldStop(shouldStop) {
			return deliveredTotal, nil
		}
		remainingMessages = messageLimit - deliveredTotal
		remainingFragments = fragmentLimit - deliveredTotal
		if s.dataRing != nil {
			delivered, err = s.drainExternalDataRing(ctx, min(remainingMessages, remainingFragments), handler, shouldStop)
			if err != nil && !errors.Is(err, ErrIPCRingEmpty) {
				return deliveredTotal + delivered, err
			}
			deliveredTotal += delivered
			if deliveredTotal >= messageLimit || deliveredTotal >= fragmentLimit || pollShouldStop(shouldStop) {
				return deliveredTotal, nil
			}
		}
		event, err := s.client.sendIPCCommand(ctx, DriverIPCCommand{
			Type:          DriverIPCCommandPollSubscription,
			ClientID:      s.client.id,
			ResourceID:    s.id,
			MessageLimit:  messageLimit - deliveredTotal,
			FragmentLimit: fragmentLimit - deliveredTotal,
		})
		if err != nil {
			if deliveredTotal > 0 && isContextDoneError(err) {
				return deliveredTotal, nil
			}
			return deliveredTotal, err
		}
		if event.Type != DriverIPCEventSubscriptionPolled {
			return deliveredTotal, ErrDriverIPCProtocol
		}
		if event.BackPressured && event.MessageCount == 0 && len(event.Messages) == 0 {
			if deliveredTotal > 0 {
				return deliveredTotal, nil
			}
			idle.Idle(0)
			continue
		}
		if event.MessageCount == 0 && len(event.Messages) == 0 {
			if deliveredTotal > 0 {
				return deliveredTotal, nil
			}
			idle.Idle(0)
			continue
		}
		delivered, err = s.drainExternalMessages(ctx, event, handler, shouldStop)
		deliveredTotal += delivered
		if err != nil {
			return deliveredTotal, err
		}
	}
	return deliveredTotal, nil
}

func (s *DriverSubscription) enqueueExternalPendingIPCMessages(messages []DriverIPCMessage) {
	if s == nil || len(messages) == 0 {
		return
	}
	pending := make([]Message, 0, len(messages))
	for _, ipcMessage := range messages {
		pending = append(pending, ipcMessage.message())
	}
	s.pendingMu.Lock()
	s.pendingMessages = append(s.pendingMessages, pending...)
	s.pendingMu.Unlock()
}

func (s *DriverSubscription) peekExternalPending() (Message, bool) {
	if s == nil {
		return Message{}, false
	}
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if len(s.pendingMessages) == 0 {
		return Message{}, false
	}
	return cloneMessage(s.pendingMessages[0]), true
}

func (s *DriverSubscription) commitExternalPending() {
	if s == nil {
		return
	}
	s.pendingMu.Lock()
	if len(s.pendingMessages) > 0 {
		copy(s.pendingMessages, s.pendingMessages[1:])
		s.pendingMessages[len(s.pendingMessages)-1] = Message{}
		s.pendingMessages = s.pendingMessages[:len(s.pendingMessages)-1]
	}
	s.pendingMu.Unlock()
}

func (s *DriverSubscription) drainExternalPending(ctx context.Context, limit int, handler Handler, shouldStop func() bool) (int, error) {
	if limit <= 0 || pollShouldStop(shouldStop) {
		return 0, nil
	}
	delivered := 0
	for delivered < limit && !pollShouldStop(shouldStop) {
		msg, ok := s.peekExternalPending()
		if !ok {
			return delivered, nil
		}
		if err := handler(ctx, msg); err != nil {
			return delivered, err
		}
		s.commitExternalPending()
		delivered++
	}
	return delivered, nil
}

func (s *DriverSubscription) controlledPollExternalPending(ctx context.Context, handler ControlledHandler) (int, bool, bool, error) {
	msg, ok := s.peekExternalPending()
	if !ok {
		return 0, false, false, nil
	}
	switch action := handler(ctx, msg); action {
	case ControlledPollContinue, ControlledPollCommit:
		s.commitExternalPending()
		return 1, true, false, nil
	case ControlledPollBreak:
		s.commitExternalPending()
		return 1, true, true, nil
	case ControlledPollAbort:
		return 0, true, true, ErrControlledPollAbort
	default:
		return 0, true, true, invalidConfigf("invalid controlled poll action: %d", action)
	}
}

func (s *DriverSubscription) controlledPollExternalDataRing(ctx context.Context, handler ControlledHandler) (int, bool, bool, error) {
	if s == nil || s.dataRing == nil {
		return 0, false, false, ErrDriverIPCClosed
	}
	delivered := 0
	handled := false
	stop := false
	_, err := pollDriverIPCMessages(s.dataRing, 1, func(ipcMessage DriverIPCMessage) error {
		handled = true
		switch action := handler(ctx, ipcMessage.message()); action {
		case ControlledPollContinue, ControlledPollCommit:
			delivered = 1
			return nil
		case ControlledPollBreak:
			delivered = 1
			stop = true
			return nil
		case ControlledPollAbort:
			stop = true
			return ErrControlledPollAbort
		default:
			stop = true
			return invalidConfigf("invalid controlled poll action: %d", action)
		}
	})
	if err != nil {
		if errors.Is(err, ErrIPCRingEmpty) {
			return 0, false, false, nil
		}
		return delivered, handled, stop, err
	}
	return delivered, handled, stop, nil
}

func isContextDoneError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (s *DriverSubscription) readExternalMessage(event DriverIPCEvent) (Message, error) {
	if s != nil && s.dataRing != nil && event.MessageCount > 0 {
		out, ok, err := s.readExternalDataRingMessage()
		if err != nil {
			return Message{}, err
		}
		if !ok {
			return Message{}, ErrDriverIPCProtocol
		}
		return out, nil
	}
	if len(event.Messages) == 0 {
		return Message{}, ErrDriverIPCProtocol
	}
	return event.Messages[0].message(), nil
}

func (s *DriverSubscription) drainExternalMessages(ctx context.Context, event DriverIPCEvent, handler Handler, shouldStop func() bool) (int, error) {
	if pollShouldStop(shouldStop) {
		return 0, nil
	}
	if s != nil && s.dataRing != nil && event.MessageCount > 0 {
		if len(event.Messages) > 0 {
			return 0, ErrDriverIPCProtocol
		}
		delivered, err := s.drainExternalDataRing(ctx, event.MessageCount, handler, shouldStop)
		if err != nil {
			return delivered, err
		}
		if delivered != event.MessageCount && !pollShouldStop(shouldStop) {
			return delivered, ErrDriverIPCProtocol
		}
		return delivered, nil
	}
	if len(event.Messages) == 0 {
		return 0, nil
	}
	s.enqueueExternalPendingIPCMessages(event.Messages)
	return s.drainExternalPending(ctx, len(event.Messages), handler, shouldStop)
}

func (s *DriverSubscription) readExternalDataRingMessage() (Message, bool, error) {
	if s == nil || s.dataRing == nil {
		return Message{}, false, ErrDriverIPCClosed
	}
	var out Message
	n, err := pollDriverIPCMessages(s.dataRing, 1, func(ipcMessage DriverIPCMessage) error {
		out = ipcMessage.message()
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrIPCRingEmpty) {
			return Message{}, false, nil
		}
		return Message{}, false, err
	}
	return out, n == 1, nil
}

func (s *DriverSubscription) drainExternalDataRing(ctx context.Context, limit int, handler Handler, shouldStop func() bool) (int, error) {
	if s == nil || s.dataRing == nil {
		return 0, ErrDriverIPCClosed
	}
	if limit <= 0 || pollShouldStop(shouldStop) {
		return 0, nil
	}
	delivered := 0
	n, err := pollDriverIPCMessages(s.dataRing, limit, func(ipcMessage DriverIPCMessage) error {
		if err := handler(ctx, ipcMessage.message()); err != nil {
			return err
		}
		delivered++
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrIPCRingEmpty) && delivered > 0 {
			return delivered, nil
		}
		return delivered, err
	}
	if n == 0 && delivered == 0 {
		return 0, ErrIPCRingEmpty
	}
	return delivered, nil
}

func (c *DriverClient) sendIPCCommand(ctx context.Context, command DriverIPCCommand) (DriverIPCEvent, error) {
	if c == nil || c.ipc == nil {
		return DriverIPCEvent{}, ErrDriverClientClosed
	}
	if c.closed.Load() {
		return DriverIPCEvent{}, ErrDriverClientClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	correlationID, err := c.ipc.SendCommand(ctx, command, nil)
	if err != nil {
		return DriverIPCEvent{}, err
	}
	event, err := waitDriverIPCEvent(ctx, c.ipc, correlationID)
	if err != nil {
		return DriverIPCEvent{}, err
	}
	if event.Type == DriverIPCEventCommandError {
		return DriverIPCEvent{}, driverIPCCommandError(event.Error)
	}
	return event, nil
}

func driverIPCCommandError(message string) error {
	switch {
	case strings.Contains(message, ErrDriverClientClosed.Error()):
		return fmt.Errorf("%w: %s", ErrDriverClientClosed, message)
	case strings.Contains(message, ErrDriverResourceNotFound.Error()):
		return fmt.Errorf("%w: %s", ErrDriverResourceNotFound, message)
	case strings.Contains(message, ErrDriverClosed.Error()):
		return fmt.Errorf("%w: %s", ErrDriverClosed, message)
	case strings.Contains(message, ErrDriverExternalUnsupported.Error()):
		return fmt.Errorf("%w: %s", ErrDriverExternalUnsupported, message)
	case strings.Contains(message, ErrBackPressure.Error()):
		return fmt.Errorf("%w: %s", ErrBackPressure, message)
	case strings.Contains(message, ErrDriverIPCProtocol.Error()):
		return fmt.Errorf("%w: %s", ErrDriverIPCProtocol, message)
	case strings.Contains(message, ErrInvalidConfig.Error()):
		return fmt.Errorf("%w: %s", ErrInvalidConfig, message)
	default:
		return errors.New(message)
	}
}

type driverNetAddr string

func (a driverNetAddr) Network() string {
	return "driver"
}

func (a driverNetAddr) String() string {
	return string(a)
}

func (state *driverState) closeAll() {
	for id := range state.publications {
		_ = state.closePublication(id, 0)
	}
	for id := range state.subscriptions {
		_ = state.closeSubscription(id, 0)
	}
	for id := range state.clients {
		delete(state.clients, id)
		state.counters.ClientsClosed++
	}
}

func (state *driverState) cleanup(now time.Time) {
	state.counters.CleanupRuns++
	for id, client := range state.clients {
		if now.Sub(client.lastHeartbeat) <= state.clientTimeout {
			continue
		}
		if err := state.closeClient(id, true, false); err == nil {
			state.counters.StaleClientsClosed++
		}
	}
}

func (state *driverState) closeClient(id DriverClientID, stale, allowMissing bool) error {
	client := state.clients[id]
	if client == nil {
		if allowMissing {
			return nil
		}
		return ErrDriverClientClosed
	}

	var closeErr error
	for resourceID := range client.publications {
		if err := state.closePublication(resourceID, id); closeErr == nil && err != nil {
			closeErr = err
		}
	}
	for resourceID := range client.subscriptions {
		if err := state.closeSubscription(resourceID, id); closeErr == nil && err != nil {
			closeErr = err
		}
	}
	delete(state.clients, id)
	state.counters.ClientsClosed++
	operation := "client"
	message := "client closed"
	if stale {
		operation = "cleanup"
		message = "stale client closed"
	}
	state.log(LogLevelInfo, operation, message, map[string]any{
		"client_id": id,
		"name":      client.name,
	}, closeErr)
	return closeErr
}

func (state *driverState) closePublication(id DriverResourceID, owner DriverClientID) error {
	resource := state.publications[id]
	if resource == nil || (owner != 0 && resource.clientID != owner) {
		return ErrDriverResourceNotFound
	}
	err := resource.publication.Close()
	delete(state.publications, id)
	if client := state.clients[resource.clientID]; client != nil {
		delete(client.publications, id)
	}
	state.counters.PublicationsClosed++
	state.log(LogLevelInfo, "publication", "publication closed", map[string]any{
		"client_id":   resource.clientID,
		"resource_id": id,
		"stream_id":   resource.streamID,
		"session_id":  resource.sessionID,
	}, err)
	return err
}

func (state *driverState) closeSubscription(id DriverResourceID, owner DriverClientID) error {
	resource := state.subscriptions[id]
	if resource == nil || (owner != 0 && resource.clientID != owner) {
		return ErrDriverResourceNotFound
	}
	err := resource.subscription.Close()
	delete(state.subscriptions, id)
	if client := state.clients[resource.clientID]; client != nil {
		delete(client.subscriptions, id)
	}
	state.counters.SubscriptionsClosed++
	state.log(LogLevelInfo, "subscription", "subscription closed", map[string]any{
		"client_id":   resource.clientID,
		"resource_id": id,
		"stream_id":   resource.streamID,
	}, err)
	return err
}

func (state *driverState) snapshot() DriverSnapshot {
	snapshot := DriverSnapshot{
		Counters: state.counters,
	}
	publicationEndpoints := make(map[string]struct{})
	subscriptionEndpoints := make(map[string]struct{})
	for _, client := range state.clients {
		snapshot.Clients = append(snapshot.Clients, DriverClientSnapshot{
			ID:            client.id,
			Name:          client.name,
			CreatedAt:     client.createdAt,
			LastHeartbeat: client.lastHeartbeat,
			Publications:  len(client.publications),
			Subscriptions: len(client.subscriptions),
		})
	}
	for _, publication := range state.publications {
		for _, endpoint := range driverPublicationEndpoints(publication) {
			publicationEndpoints[endpoint] = struct{}{}
		}
		snapshot.Publications = append(snapshot.Publications, DriverPublicationSnapshot{
			ID:               publication.id,
			ClientID:         publication.clientID,
			StreamID:         publication.streamID,
			SessionID:        publication.sessionID,
			RemoteAddr:       publication.remoteAddr,
			CreatedAt:        publication.createdAt,
			TermBufferFiles:  append([]string(nil), publication.termBufferFiles...),
			TermBufferMapped: publication.termBufferMapped,
		})
	}
	for _, subscription := range state.subscriptions {
		if endpoint := driverSubscriptionEndpoint(subscription); endpoint != "" {
			subscriptionEndpoints[endpoint] = struct{}{}
		}
		snapshot.Subscriptions = append(snapshot.Subscriptions, DriverSubscriptionSnapshot{
			ID:        subscription.id,
			ClientID:  subscription.clientID,
			StreamID:  subscription.streamID,
			LocalAddr: subscription.localAddr,
			CreatedAt: subscription.createdAt,
		})
		if subscription.subscription != nil {
			for _, image := range subscription.subscription.Images() {
				snapshot.Images = append(snapshot.Images, DriverImageSnapshot{
					ResourceID:           subscription.id,
					ClientID:             subscription.clientID,
					StreamID:             image.StreamID,
					SessionID:            image.SessionID,
					Source:               image.Source,
					InitialTermID:        image.InitialTermID,
					TermBufferLength:     image.TermBufferLength,
					JoinPosition:         image.JoinPosition,
					CurrentPosition:      image.CurrentPosition,
					ObservedPosition:     image.ObservedPosition,
					LagBytes:             image.LagBytes,
					LastSequence:         image.LastSequence,
					LastObservedSequence: image.LastObservedSequence,
					AvailableAt:          image.AvailableAt,
					UnavailableAt:        image.UnavailableAt,
				})
			}
			for _, report := range subscription.subscription.LossReports() {
				snapshot.LossReports = append(snapshot.LossReports, DriverLossReportSnapshot{
					ResourceID:       subscription.id,
					ClientID:         subscription.clientID,
					StreamID:         report.StreamID,
					SessionID:        report.SessionID,
					Source:           report.Source,
					ObservationCount: report.ObservationCount,
					MissingMessages:  report.MissingMessages,
					FirstObservation: report.FirstObservation,
					LastObservation:  report.LastObservation,
				})
			}
		}
	}
	snapshot.StatusCounters = buildDriverStatusCounters(snapshot, publicationEndpoints, subscriptionEndpoints)
	snapshot.CounterSnapshots = buildDriverCounterSnapshots(state.counters, snapshot.StatusCounters, state.metrics)
	sort.Slice(snapshot.Clients, func(i, j int) bool {
		return snapshot.Clients[i].ID < snapshot.Clients[j].ID
	})
	sort.Slice(snapshot.Publications, func(i, j int) bool {
		return snapshot.Publications[i].ID < snapshot.Publications[j].ID
	})
	sort.Slice(snapshot.Subscriptions, func(i, j int) bool {
		return snapshot.Subscriptions[i].ID < snapshot.Subscriptions[j].ID
	})
	sort.Slice(snapshot.Images, func(i, j int) bool {
		if snapshot.Images[i].ResourceID != snapshot.Images[j].ResourceID {
			return snapshot.Images[i].ResourceID < snapshot.Images[j].ResourceID
		}
		if snapshot.Images[i].StreamID != snapshot.Images[j].StreamID {
			return snapshot.Images[i].StreamID < snapshot.Images[j].StreamID
		}
		if snapshot.Images[i].SessionID != snapshot.Images[j].SessionID {
			return snapshot.Images[i].SessionID < snapshot.Images[j].SessionID
		}
		return snapshot.Images[i].Source < snapshot.Images[j].Source
	})
	sort.Slice(snapshot.LossReports, func(i, j int) bool {
		if snapshot.LossReports[i].ResourceID != snapshot.LossReports[j].ResourceID {
			return snapshot.LossReports[i].ResourceID < snapshot.LossReports[j].ResourceID
		}
		if snapshot.LossReports[i].StreamID != snapshot.LossReports[j].StreamID {
			return snapshot.LossReports[i].StreamID < snapshot.LossReports[j].StreamID
		}
		if snapshot.LossReports[i].SessionID != snapshot.LossReports[j].SessionID {
			return snapshot.LossReports[i].SessionID < snapshot.LossReports[j].SessionID
		}
		return snapshot.LossReports[i].Source < snapshot.LossReports[j].Source
	})
	return snapshot
}

func buildDriverStatusCounters(snapshot DriverSnapshot, publicationEndpoints, subscriptionEndpoints map[string]struct{}) DriverStatusCounters {
	channelEndpoints := make(map[string]struct{}, len(publicationEndpoints)+len(subscriptionEndpoints))
	for endpoint := range publicationEndpoints {
		channelEndpoints[endpoint] = struct{}{}
	}
	for endpoint := range subscriptionEndpoints {
		channelEndpoints[endpoint] = struct{}{}
	}

	status := DriverStatusCounters{
		ActiveClients:         uint64(len(snapshot.Clients)),
		ChannelEndpoints:      uint64(len(channelEndpoints)),
		PublicationEndpoints:  uint64(len(publicationEndpoints)),
		SubscriptionEndpoints: uint64(len(subscriptionEndpoints)),
		ActivePublications:    uint64(len(snapshot.Publications)),
		ActiveSubscriptions:   uint64(len(snapshot.Subscriptions)),
		Images:                uint64(len(snapshot.Images)),
	}
	for _, image := range snapshot.Images {
		if image.UnavailableAt.IsZero() {
			status.AvailableImages++
		} else {
			status.UnavailableImages++
		}
		if image.LagBytes > 0 {
			status.LaggingImages++
		}
	}
	return status
}

func driverPublicationEndpoints(publication *driverPublicationState) []string {
	if publication == nil {
		return nil
	}
	if publication.publication != nil {
		channel := publication.publication.ChannelURI()
		endpoints := make([]string, 0, 1+len(channel.Destinations))
		if channel.Endpoint != "" {
			endpoints = append(endpoints, channel.Endpoint)
		}
		endpoints = append(endpoints, channel.Destinations...)
		if len(endpoints) > 0 {
			return endpoints
		}
	}
	if publication.remoteAddr != "" {
		return []string{publication.remoteAddr}
	}
	return nil
}

func driverSubscriptionEndpoint(subscription *driverSubscriptionState) string {
	if subscription == nil {
		return ""
	}
	if subscription.subscription != nil {
		channel := subscription.subscription.ChannelURI()
		if channel.Endpoint != "" {
			return channel.Endpoint
		}
	}
	return subscription.localAddr
}

func (state *driverState) senderAgentDuty() int {
	if state == nil {
		return 0
	}
	return 0
}

func (state *driverState) receiverAgentDuty() int {
	if state == nil {
		return 0
	}
	return 0
}

func (state *driverState) recordDutyCycle(role driverAgentRole, duration time.Duration) {
	if state == nil || duration <= 0 {
		return
	}
	nanos := uint64(duration)
	state.counters.DutyCycles++
	switch role {
	case driverAgentSender:
		state.counters.SenderDutyCycles++
	case driverAgentReceiver:
		state.counters.ReceiverDutyCycles++
	default:
		state.counters.ConductorDutyCycles++
	}
	state.counters.DutyCycleNanos += nanos
	if nanos > state.counters.DutyCycleMaxNanos {
		state.counters.DutyCycleMaxNanos = nanos
	}
	if state.stallThreshold > 0 && duration >= state.stallThreshold {
		state.counters.Stalls++
		state.counters.StallNanos += nanos
		if nanos > state.counters.StallMaxNanos {
			state.counters.StallMaxNanos = nanos
		}
	}
}

func (state *driverState) log(level LogLevel, operation, message string, fields map[string]any, err error) {
	state.recordDriverError(level, operation, message, fields, err)
	logEvent(context.Background(), state.logger, LogEvent{
		Level:     level,
		Component: "media_driver",
		Operation: operation,
		Message:   message,
		Fields:    fields,
		Err:       err,
	})
}
