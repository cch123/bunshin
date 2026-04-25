package bunshin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultDriverCommandBuffer = 64
	defaultDriverClientTimeout = 30 * time.Second
	defaultDriverCleanupPeriod = time.Second
)

var (
	ErrDriverClosed           = errors.New("bunshin driver: closed")
	ErrDriverClientClosed     = errors.New("bunshin driver: client closed")
	ErrDriverResourceNotFound = errors.New("bunshin driver: resource not found")
)

type DriverConfig struct {
	CommandBuffer   int
	ClientTimeout   time.Duration
	CleanupInterval time.Duration
	Metrics         *Metrics
	Logger          Logger
}

type MediaDriver struct {
	commands  chan driverCommand
	done      chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
	closing   atomic.Bool
}

type DriverClient struct {
	driver *MediaDriver
	id     DriverClientID
	name   string
	closed atomic.Bool
}

type DriverPublication struct {
	id          DriverResourceID
	client      *DriverClient
	publication *Publication
}

type DriverSubscription struct {
	id           DriverResourceID
	client       *DriverClient
	subscription *Subscription
}

type DriverClientID uint64

type DriverResourceID uint64

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
}

type DriverSnapshot struct {
	Counters      DriverCounters
	Clients       []DriverClientSnapshot
	Publications  []DriverPublicationSnapshot
	Subscriptions []DriverSubscriptionSnapshot
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
	ID         DriverResourceID
	ClientID   DriverClientID
	StreamID   uint32
	SessionID  uint32
	RemoteAddr string
	CreatedAt  time.Time
}

type DriverSubscriptionSnapshot struct {
	ID        DriverResourceID
	ClientID  DriverClientID
	StreamID  uint32
	LocalAddr string
	CreatedAt time.Time
}

type driverCommand struct {
	apply func(*driverState) (any, error)
	reply chan driverResult
}

type driverResult struct {
	value any
	err   error
}

type driverState struct {
	metrics         *Metrics
	logger          Logger
	clientTimeout   time.Duration
	cleanupInterval time.Duration

	nextClientID   DriverClientID
	nextResourceID DriverResourceID
	clients        map[DriverClientID]*driverClientState
	publications   map[DriverResourceID]*driverPublicationState
	subscriptions  map[DriverResourceID]*driverSubscriptionState
	counters       DriverCounters
}

type driverClientState struct {
	id            DriverClientID
	name          string
	createdAt     time.Time
	lastHeartbeat time.Time
	publications  map[DriverResourceID]struct{}
	subscriptions map[DriverResourceID]struct{}
}

type driverPublicationState struct {
	id          DriverResourceID
	clientID    DriverClientID
	streamID    uint32
	sessionID   uint32
	remoteAddr  string
	createdAt   time.Time
	publication *Publication
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
	if cfg.CommandBuffer == 0 {
		cfg.CommandBuffer = defaultDriverCommandBuffer
	}
	if cfg.ClientTimeout == 0 {
		cfg.ClientTimeout = defaultDriverClientTimeout
	}
	if cfg.CleanupInterval == 0 {
		cfg.CleanupInterval = defaultDriverCleanupPeriod
	}

	driver := &MediaDriver{
		commands: make(chan driverCommand, cfg.CommandBuffer),
		done:     make(chan struct{}),
		closed:   make(chan struct{}),
	}
	go driver.run(cfg)

	logEvent(context.Background(), cfg.Logger, LogEvent{
		Level:     LogLevelInfo,
		Component: "media_driver",
		Operation: "start",
		Message:   "media driver started",
		Fields: map[string]any{
			"command_buffer":   cfg.CommandBuffer,
			"client_timeout":   cfg.ClientTimeout,
			"cleanup_interval": cfg.CleanupInterval,
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

func (d *MediaDriver) Snapshot(ctx context.Context) (DriverSnapshot, error) {
	value, err := d.dispatch(ctx, func(state *driverState) (any, error) {
		return state.snapshot(), nil
	})
	if err != nil {
		return DriverSnapshot{}, err
	}
	return value.(DriverSnapshot), nil
}

func (d *MediaDriver) Close() error {
	d.closeOnce.Do(func() {
		d.closing.Store(true)
		close(d.done)
		<-d.closed
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

func (d *MediaDriver) run(cfg DriverConfig) {
	state := &driverState{
		metrics:         cfg.Metrics,
		logger:          cfg.Logger,
		clientTimeout:   cfg.ClientTimeout,
		cleanupInterval: cfg.CleanupInterval,
		clients:         make(map[DriverClientID]*driverClientState),
		publications:    make(map[DriverResourceID]*driverPublicationState),
		subscriptions:   make(map[DriverResourceID]*driverSubscriptionState),
	}

	ticker := time.NewTicker(cfg.CleanupInterval)
	defer ticker.Stop()
	defer close(d.closed)

	for {
		select {
		case command := <-d.commands:
			state.counters.CommandsProcessed++
			value, err := command.apply(state)
			if err != nil {
				state.counters.CommandsFailed++
			}
			command.reply <- driverResult{value: value, err: err}
		case now := <-ticker.C:
			state.cleanup(now)
		case <-d.done:
			state.closeAll()
			state.log(LogLevelInfo, "close", "media driver closed", nil, nil)
			return
		}
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
		pub, err := DialPublication(cfg)
		if err != nil {
			return nil, err
		}

		state.nextResourceID++
		id := state.nextResourceID
		resource := &driverPublicationState{
			id:          id,
			clientID:    c.id,
			streamID:    pub.streamID,
			sessionID:   pub.sessionID,
			remoteAddr:  cfg.RemoteAddr,
			createdAt:   time.Now(),
			publication: pub,
		}
		state.publications[id] = resource
		client.publications[id] = struct{}{}
		state.counters.PublicationsRegistered++
		state.log(LogLevelInfo, "publication", "publication registered", map[string]any{
			"client_id":   c.id,
			"resource_id": id,
			"stream_id":   pub.streamID,
			"session_id":  pub.sessionID,
			"remote_addr": cfg.RemoteAddr,
		}, nil)
		return &DriverPublication{
			id:          id,
			client:      c,
			publication: pub,
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
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return value.(*DriverSubscription), nil
}

func (c *DriverClient) ClosePublication(ctx context.Context, id DriverResourceID) error {
	_, err := c.driver.dispatch(ctx, func(state *driverState) (any, error) {
		if state.clients[c.id] == nil {
			return nil, ErrDriverClientClosed
		}
		return nil, state.closePublication(id, c.id)
	})
	return err
}

func (c *DriverClient) CloseSubscription(ctx context.Context, id DriverResourceID) error {
	_, err := c.driver.dispatch(ctx, func(state *driverState) (any, error) {
		if state.clients[c.id] == nil {
			return nil, ErrDriverClientClosed
		}
		return nil, state.closeSubscription(id, c.id)
	})
	return err
}

func (c *DriverClient) Snapshot(ctx context.Context) (DriverSnapshot, error) {
	return c.driver.Snapshot(ctx)
}

func (c *DriverClient) Close(ctx context.Context) error {
	if c.closed.Load() {
		return nil
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
	return p.publication.Send(ctx, payload)
}

func (p *DriverPublication) LocalAddr() net.Addr {
	return p.publication.LocalAddr()
}

func (p *DriverPublication) Close(ctx context.Context) error {
	return p.client.ClosePublication(ctx, p.id)
}

func (s *DriverSubscription) ID() DriverResourceID {
	return s.id
}

func (s *DriverSubscription) Serve(ctx context.Context, handler Handler) error {
	return s.subscription.Serve(ctx, handler)
}

func (s *DriverSubscription) LocalAddr() net.Addr {
	return s.subscription.LocalAddr()
}

func (s *DriverSubscription) LossReports() []LossReport {
	return s.subscription.LossReports()
}

func (s *DriverSubscription) Close(ctx context.Context) error {
	return s.client.CloseSubscription(ctx, s.id)
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
		snapshot.Publications = append(snapshot.Publications, DriverPublicationSnapshot{
			ID:         publication.id,
			ClientID:   publication.clientID,
			StreamID:   publication.streamID,
			SessionID:  publication.sessionID,
			RemoteAddr: publication.remoteAddr,
			CreatedAt:  publication.createdAt,
		})
	}
	for _, subscription := range state.subscriptions {
		snapshot.Subscriptions = append(snapshot.Subscriptions, DriverSubscriptionSnapshot{
			ID:        subscription.id,
			ClientID:  subscription.clientID,
			StreamID:  subscription.streamID,
			LocalAddr: subscription.localAddr,
			CreatedAt: subscription.createdAt,
		})
	}
	sort.Slice(snapshot.Clients, func(i, j int) bool {
		return snapshot.Clients[i].ID < snapshot.Clients[j].ID
	})
	sort.Slice(snapshot.Publications, func(i, j int) bool {
		return snapshot.Publications[i].ID < snapshot.Publications[j].ID
	})
	sort.Slice(snapshot.Subscriptions, func(i, j int) bool {
		return snapshot.Subscriptions[i].ID < snapshot.Subscriptions[j].ID
	})
	return snapshot
}

func (state *driverState) log(level LogLevel, operation, message string, fields map[string]any, err error) {
	logEvent(context.Background(), state.logger, LogEvent{
		Level:     level,
		Component: "media_driver",
		Operation: operation,
		Message:   message,
		Fields:    fields,
		Err:       err,
	})
}
