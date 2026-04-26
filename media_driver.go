package bunshin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sort"
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
	driver  *MediaDriver
	ipc     *DriverIPC
	id      DriverClientID
	name    string
	timeout time.Duration
	mu      sync.Mutex
	closed  atomic.Bool
}

type DriverPublication struct {
	id          DriverResourceID
	client      *DriverClient
	publication *Publication
	localAddr   string
}

type DriverSubscription struct {
	id           DriverResourceID
	client       *DriverClient
	subscription *Subscription
	localAddr    string
}

type DriverClientID uint64

type DriverResourceID uint64

type DriverConnectionConfig struct {
	Directory           string
	CommandRingPath     string
	EventRingPath       string
	ClientName          string
	CommandRingCapacity int
	EventRingCapacity   int
	Timeout             time.Duration
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
	ID        DriverResourceID
	ClientID  DriverClientID
	StreamID  uint32
	LocalAddr string
	CreatedAt time.Time
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
		ipc:     ipc,
		name:    cfg.ClientName,
		timeout: cfg.Timeout,
	}
	event, err := client.sendIPCCommand(ctx, DriverIPCCommand{
		Type:       DriverIPCCommandOpenClient,
		ClientName: cfg.ClientName,
	})
	if err != nil {
		_ = ipc.Close()
		return nil, err
	}
	client.id = event.ClientID
	client.name = event.Message
	return client, nil
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
				StreamID:  cfg.StreamID,
				LocalAddr: cfg.LocalAddr,
			},
		})
		if err != nil {
			return nil, err
		}
		return &DriverSubscription{
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
		_, err := c.sendIPCCommand(ctx, DriverIPCCommand{
			Type:     DriverIPCCommandCloseClient,
			ClientID: c.id,
		})
		if err == nil || errors.Is(err, ErrDriverClientClosed) {
			c.closed.Store(true)
			closeErr := c.ipc.Close()
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
		return 0, ErrDriverExternalUnsupported
	}
	return s.subscription.Poll(ctx, handler)
}

func (s *DriverSubscription) PollN(ctx context.Context, fragmentLimit int, handler Handler) (int, error) {
	if s == nil {
		return 0, ErrDriverClientClosed
	}
	if s.subscription == nil {
		return 0, ErrDriverExternalUnsupported
	}
	return s.subscription.PollN(ctx, fragmentLimit, handler)
}

func (s *DriverSubscription) ControlledPoll(ctx context.Context, handler ControlledHandler) (int, error) {
	if s == nil {
		return 0, ErrDriverClientClosed
	}
	if s.subscription == nil {
		return 0, ErrDriverExternalUnsupported
	}
	return s.subscription.ControlledPoll(ctx, handler)
}

func (s *DriverSubscription) ControlledPollN(ctx context.Context, fragmentLimit int, handler ControlledHandler) (int, error) {
	if s == nil {
		return 0, ErrDriverClientClosed
	}
	if s.subscription == nil {
		return 0, ErrDriverExternalUnsupported
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
	if s == nil || s.subscription == nil {
		return nil
	}
	return s.subscription.LossReports()
}

func (s *DriverSubscription) LagReports() []SubscriptionLagReport {
	if s == nil || s.subscription == nil {
		return nil
	}
	return s.subscription.LagReports()
}

func (s *DriverSubscription) Images() []ImageSnapshot {
	if s == nil || s.subscription == nil {
		return nil
	}
	return s.subscription.Images()
}

func (s *DriverSubscription) Close(ctx context.Context) error {
	return s.client.CloseSubscription(ctx, s.id)
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
		return DriverIPCEvent{}, errors.New(event.Error)
	}
	return event, nil
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
