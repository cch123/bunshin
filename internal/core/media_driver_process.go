package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

const (
	defaultDriverProcessHeartbeatInterval = time.Second
	defaultDriverProcessStaleTimeout      = 5 * time.Second
	defaultDriverProcessPollLimit         = 64
	defaultDriverProcessPumpLimit         = 64
)

var ErrDriverProcessUnavailable = errors.New("bunshin driver process: unavailable")

type DriverProcessConfig struct {
	Directory               string
	Driver                  DriverConfig
	CommandRingCapacity     int
	EventRingCapacity       int
	ResetIPC                bool
	HeartbeatInterval       time.Duration
	StaleTimeout            time.Duration
	PollLimit               int
	SubscriptionPumpLimit   int
	DisableSubscriptionPump bool
	IdleStrategy            IdleStrategy
}

type DriverProcessStatus struct {
	Layout       DriverDirectoryLayout `json:"layout"`
	Mark         DriverMarkFile        `json:"mark"`
	CheckedAt    time.Time             `json:"checked_at"`
	HeartbeatAge time.Duration         `json:"heartbeat_age"`
	Active       bool                  `json:"active"`
	Stale        bool                  `json:"stale"`
}

func RunMediaDriverProcess(ctx context.Context, cfg DriverProcessConfig) error {
	normalized, err := normalizeDriverProcessConfig(cfg)
	if err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	driver, err := StartMediaDriver(normalized.Driver)
	if err != nil {
		return err
	}
	defer driver.Close()

	ipc, err := OpenDriverIPC(DriverIPCConfig{
		Directory:           normalized.Directory,
		CommandRingCapacity: normalized.CommandRingCapacity,
		EventRingCapacity:   normalized.EventRingCapacity,
		Reset:               normalized.ResetIPC,
	})
	if err != nil {
		return err
	}
	defer ipc.Close()

	server, err := NewDriverIPCServer(driver, ipc)
	if err != nil {
		return err
	}
	defer server.Close()

	idle := normalized.IdleStrategy
	if idle == nil {
		idle = NewDefaultBackoffIdleStrategy()
	}
	if _, err := server.FlushReports(ctx); err != nil {
		return err
	}
	nextHeartbeat := time.Now().Add(normalized.HeartbeatInterval)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		work, err := server.Poll(ctx, normalized.PollLimit)
		if err != nil && !errors.Is(err, ErrIPCRingEmpty) {
			return err
		}
		pumpWork := 0
		if !normalized.DisableSubscriptionPump {
			var pumpErr error
			pumpWork, pumpErr = server.PumpSubscriptions(ctx, normalized.SubscriptionPumpLimit)
			if pumpErr != nil {
				return pumpErr
			}
		}
		work += pumpWork
		if server.Terminated() {
			return nil
		}

		now := time.Now()
		if !now.Before(nextHeartbeat) {
			if _, err := server.FlushReports(ctx); err != nil {
				return err
			}
			nextHeartbeat = now.Add(normalized.HeartbeatInterval)
			idle.Reset()
		}
		if errors.Is(err, ErrIPCRingEmpty) && pumpWork == 0 {
			idle.Idle(0)
			continue
		}
		idle.Idle(work)
	}
}

func ReadDriverMarkFile(directory string) (DriverMarkFile, error) {
	layout, err := ResolveDriverDirectoryLayout(directory)
	if err != nil {
		return DriverMarkFile{}, err
	}
	data, err := os.ReadFile(layout.MarkFile)
	if err != nil {
		return DriverMarkFile{}, fmt.Errorf("%w: read driver mark file: %w", ErrDriverProcessUnavailable, err)
	}
	var mark DriverMarkFile
	if err := json.Unmarshal(data, &mark); err != nil {
		return DriverMarkFile{}, fmt.Errorf("%w: decode driver mark file: %w", ErrDriverProcessUnavailable, err)
	}
	return mark, nil
}

func CheckDriverProcess(directory string, staleTimeout time.Duration) (DriverProcessStatus, error) {
	layout, err := ResolveDriverDirectoryLayout(directory)
	if err != nil {
		return DriverProcessStatus{}, err
	}
	if staleTimeout < 0 {
		return DriverProcessStatus{}, invalidConfigf("invalid driver process stale timeout: %s", staleTimeout)
	}
	if staleTimeout == 0 {
		staleTimeout = defaultDriverProcessStaleTimeout
	}
	mark, err := ReadDriverMarkFile(directory)
	if err != nil {
		return DriverProcessStatus{}, err
	}
	now := time.Now().UTC()
	age := now.Sub(mark.UpdatedAt)
	active := mark.Status == DriverDirectoryStatusActive
	return DriverProcessStatus{
		Layout:       layout,
		Mark:         mark,
		CheckedAt:    now,
		HeartbeatAge: age,
		Active:       active,
		Stale:        active && age > staleTimeout,
	}, nil
}

func TerminateDriverProcess(ctx context.Context, directory string) (DriverIPCEvent, error) {
	ipc, err := OpenDriverIPC(DriverIPCConfig{Directory: directory})
	if err != nil {
		return DriverIPCEvent{}, err
	}
	defer ipc.Close()
	correlationID, err := ipc.SendCommand(ctx, DriverIPCCommand{
		Type: DriverIPCCommandTerminate,
	}, nil)
	if err != nil {
		return DriverIPCEvent{}, err
	}
	return waitDriverIPCEvent(ctx, ipc, correlationID)
}

func waitDriverIPCEvent(ctx context.Context, ipc *DriverIPC, correlationID uint64) (DriverIPCEvent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	idle := NewDefaultBackoffIdleStrategy()
	for {
		select {
		case <-ctx.Done():
			return DriverIPCEvent{}, ctx.Err()
		default:
		}
		if matched, ok := ipc.takePendingEvent(correlationID); ok {
			return matched, nil
		}
		var matched DriverIPCEvent
		_, err := ipc.PollEvents(1, func(event DriverIPCEvent) error {
			if event.CorrelationID == correlationID {
				matched = event
			} else {
				ipc.storePendingEvent(event)
			}
			return nil
		})
		if err == nil {
			if matched.Type != "" {
				return matched, nil
			}
			idle.Reset()
			continue
		}
		if !errors.Is(err, ErrIPCRingEmpty) {
			return DriverIPCEvent{}, err
		}
		idle.Idle(0)
	}
}

func normalizeDriverProcessConfig(cfg DriverProcessConfig) (DriverProcessConfig, error) {
	if cfg.Directory == "" {
		cfg.Directory = cfg.Driver.Directory
	}
	if cfg.Directory == "" {
		return DriverProcessConfig{}, invalidConfigf("driver process directory is required")
	}
	if cfg.Driver.Directory != "" && cfg.Driver.Directory != cfg.Directory {
		return DriverProcessConfig{}, invalidConfigf("driver process directory mismatch: %s != %s", cfg.Directory, cfg.Driver.Directory)
	}
	cfg.Driver.Directory = cfg.Directory
	if cfg.HeartbeatInterval < 0 {
		return DriverProcessConfig{}, invalidConfigf("invalid driver process heartbeat interval: %s", cfg.HeartbeatInterval)
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = defaultDriverProcessHeartbeatInterval
	}
	if cfg.StaleTimeout < 0 {
		return DriverProcessConfig{}, invalidConfigf("invalid driver process stale timeout: %s", cfg.StaleTimeout)
	}
	if cfg.StaleTimeout == 0 {
		cfg.StaleTimeout = defaultDriverProcessStaleTimeout
	}
	if cfg.Driver.DirectoryStaleTimeout == 0 {
		cfg.Driver.DirectoryStaleTimeout = cfg.StaleTimeout
	}
	if cfg.PollLimit < 0 {
		return DriverProcessConfig{}, invalidConfigf("invalid driver process poll limit: %d", cfg.PollLimit)
	}
	if cfg.PollLimit == 0 {
		cfg.PollLimit = defaultDriverProcessPollLimit
	}
	if cfg.SubscriptionPumpLimit < 0 {
		return DriverProcessConfig{}, invalidConfigf("invalid driver process subscription pump limit: %d", cfg.SubscriptionPumpLimit)
	}
	if cfg.SubscriptionPumpLimit == 0 && !cfg.DisableSubscriptionPump {
		cfg.SubscriptionPumpLimit = defaultDriverProcessPumpLimit
	}
	if cfg.CommandRingCapacity < 0 {
		return DriverProcessConfig{}, invalidConfigf("invalid driver process command ring capacity: %d", cfg.CommandRingCapacity)
	}
	if cfg.EventRingCapacity < 0 {
		return DriverProcessConfig{}, invalidConfigf("invalid driver process event ring capacity: %d", cfg.EventRingCapacity)
	}
	return cfg, nil
}
