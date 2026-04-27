package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

const defaultArchiveReplayMergeLiveBuffer = 1024

var (
	ErrArchiveReplayMergeBufferFull = errors.New("bunshin archive replay merge: live buffer full")
	ErrArchiveReplayMergeClosed     = errors.New("bunshin archive replay merge: closed")
)

type ArchiveReplayMergeConfig struct {
	Replay          ArchiveReplayConfig
	LiveBufferLimit int
}

type ArchiveReplayMergeResult struct {
	Replayed     uint64
	Live         uint64
	BufferedLive uint64
	DroppedLive  uint64
}

type ArchiveReplayMerge struct {
	archive *Archive
	cfg     ArchiveReplayMergeConfig
	handler Handler

	mu           sync.Mutex
	deliverMu    sync.Mutex
	phase        archiveReplayMergePhase
	closed       bool
	bufferedLive []Message
	lastSequence map[archiveReplayMergeKey]uint64
	result       ArchiveReplayMergeResult
}

type archiveReplayMergePhase uint8

const (
	archiveReplayMergeBuffering archiveReplayMergePhase = iota
	archiveReplayMergeFlushing
	archiveReplayMergeLive
)

type archiveReplayMergeKey struct {
	streamID  uint32
	sessionID uint32
}

func (a *Archive) NewReplayMerge(cfg ArchiveReplayMergeConfig, handler Handler) (*ArchiveReplayMerge, error) {
	if a == nil {
		return nil, invalidConfigf("archive is required")
	}
	if handler == nil {
		return nil, errors.New("handler is required")
	}
	if cfg.LiveBufferLimit < 0 {
		return nil, invalidConfigf("invalid replay merge live buffer limit: %d", cfg.LiveBufferLimit)
	}
	if cfg.LiveBufferLimit == 0 {
		cfg.LiveBufferLimit = defaultArchiveReplayMergeLiveBuffer
	}
	return &ArchiveReplayMerge{
		archive:      a,
		cfg:          cfg,
		handler:      handler,
		lastSequence: make(map[archiveReplayMergeKey]uint64),
	}, nil
}

func (m *ArchiveReplayMerge) Replay(ctx context.Context) (ArchiveReplayMergeResult, error) {
	if m == nil {
		return ArchiveReplayMergeResult{}, ErrArchiveReplayMergeClosed
	}
	if err := m.archive.Replay(ctx, m.cfg.Replay, func(ctx context.Context, msg Message) error {
		return m.deliverReplay(ctx, msg)
	}); err != nil {
		return m.Result(), err
	}
	if err := m.flushBufferedLive(ctx); err != nil {
		return m.Result(), err
	}
	return m.Result(), nil
}

func (m *ArchiveReplayMerge) LiveHandler() Handler {
	return func(ctx context.Context, msg Message) error {
		return m.OnLive(ctx, msg)
	}
}

func (m *ArchiveReplayMerge) OnLive(ctx context.Context, msg Message) error {
	if m == nil {
		return ErrArchiveReplayMergeClosed
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrArchiveReplayMergeClosed
	}
	if m.phase != archiveReplayMergeLive {
		if len(m.bufferedLive) >= m.cfg.LiveBufferLimit {
			m.mu.Unlock()
			return fmt.Errorf("%w: limit %d", ErrArchiveReplayMergeBufferFull, m.cfg.LiveBufferLimit)
		}
		m.bufferedLive = append(m.bufferedLive, cloneMessage(msg))
		m.result.BufferedLive++
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	return m.deliverLive(ctx, msg)
}

func (m *ArchiveReplayMerge) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	m.bufferedLive = nil
	return nil
}

func (m *ArchiveReplayMerge) Result() ArchiveReplayMergeResult {
	if m == nil {
		return ArchiveReplayMergeResult{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.result
}

func (m *ArchiveReplayMerge) deliverReplay(ctx context.Context, msg Message) error {
	m.deliverMu.Lock()
	defer m.deliverMu.Unlock()

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrArchiveReplayMergeClosed
	}
	m.mu.Unlock()

	if err := m.handler(ctx, msg); err != nil {
		return err
	}
	m.mu.Lock()
	m.observeSequenceLocked(msg)
	m.result.Replayed++
	m.mu.Unlock()
	return nil
}

func (m *ArchiveReplayMerge) deliverLive(ctx context.Context, msg Message) error {
	m.deliverMu.Lock()
	defer m.deliverMu.Unlock()

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrArchiveReplayMergeClosed
	}
	if m.duplicateLiveLocked(msg) {
		m.result.DroppedLive++
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	if err := m.handler(ctx, msg); err != nil {
		return err
	}
	m.mu.Lock()
	m.observeSequenceLocked(msg)
	m.result.Live++
	m.mu.Unlock()
	return nil
}

func (m *ArchiveReplayMerge) flushBufferedLive(ctx context.Context) error {
	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return ErrArchiveReplayMergeClosed
		}
		m.phase = archiveReplayMergeFlushing
		buffered := m.bufferedLive
		m.bufferedLive = nil
		if len(buffered) == 0 {
			m.phase = archiveReplayMergeLive
			m.mu.Unlock()
			return nil
		}
		m.mu.Unlock()

		for _, msg := range buffered {
			if err := m.deliverLive(ctx, msg); err != nil {
				return err
			}
		}
	}
}

func (m *ArchiveReplayMerge) duplicateLiveLocked(msg Message) bool {
	if msg.Sequence == 0 {
		return false
	}
	last, ok := m.lastSequence[archiveReplayMergeKey{
		streamID:  msg.StreamID,
		sessionID: msg.SessionID,
	}]
	return ok && msg.Sequence <= last
}

func (m *ArchiveReplayMerge) observeSequenceLocked(msg Message) {
	if msg.Sequence == 0 {
		return
	}
	key := archiveReplayMergeKey{
		streamID:  msg.StreamID,
		sessionID: msg.SessionID,
	}
	if msg.Sequence > m.lastSequence[key] {
		m.lastSequence[key] = msg.Sequence
	}
}
