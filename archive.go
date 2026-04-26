package bunshin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	archiveMagic                      = "BSAR"
	archiveVersion              uint8 = 1
	archiveHeaderLen                  = 64
	archiveMaxPayload                 = uint64(^uint32(0))
	archiveRecordFlagPadding    uint8 = 1
	archiveRecordFlagRawFrame   uint8 = 1 << 1
	archiveCatalogFile                = "catalog.json"
	archiveDetachedSegmentExt         = ".detached"
	defaultArchiveSegmentLength       = 64 * 1024 * 1024
	minArchiveSegmentLength           = archiveHeaderLen
)

var (
	ErrArchiveClosed             = errors.New("bunshin archive: closed")
	ErrArchiveCorrupt            = errors.New("bunshin archive: corrupt")
	ErrArchivePosition           = errors.New("bunshin archive: invalid position")
	ErrArchiveRecordingActive    = errors.New("bunshin archive: recording active")
	ErrArchiveRecordingNotActive = errors.New("bunshin archive: recording not active")
)

type ArchiveConfig struct {
	Path                     string
	Sync                     bool
	SegmentLength            int64
	RecordingProgressHandler ArchiveRecordingEventHandler
	RecordingSignalHandler   ArchiveRecordingEventHandler
}

type Archive struct {
	mu                        sync.Mutex
	dir                       string
	catalogPath               string
	sync                      bool
	segmentLength             int64
	recordingProgressHandler  ArchiveRecordingEventHandler
	recordingSignalHandler    ArchiveRecordingEventHandler
	recordingEventSubscribers map[uint64]chan ArchiveRecordingEvent
	nextRecordingSubscriberID uint64
	activeRecordingID         int64
	extensionSignalPending    bool
	closed                    bool
	catalog                   archiveCatalog
}

type ArchiveRecordingSignal string

const (
	ArchiveRecordingSignalStart    ArchiveRecordingSignal = "start"
	ArchiveRecordingSignalProgress ArchiveRecordingSignal = "progress"
	ArchiveRecordingSignalExtend   ArchiveRecordingSignal = "extend"
	ArchiveRecordingSignalStop     ArchiveRecordingSignal = "stop"
	ArchiveRecordingSignalTruncate ArchiveRecordingSignal = "truncate"
	ArchiveRecordingSignalPurge    ArchiveRecordingSignal = "purge"
)

type ArchiveRecordingEventHandler func(ArchiveRecordingEvent)

type ArchiveRecordingEvent struct {
	Signal        ArchiveRecordingSignal
	RecordingID   int64
	Position      int64
	NextPosition  int64
	SegmentBase   int64
	StreamID      uint32
	SessionID     uint32
	Sequence      uint64
	PayloadLength int
	Descriptor    ArchiveRecordingDescriptor
	RecordedAt    time.Time
}

type ArchiveRecordingDescriptor struct {
	RecordingID   int64      `json:"recording_id"`
	StartPosition int64      `json:"start_position"`
	StopPosition  int64      `json:"stop_position"`
	SegmentLength int64      `json:"segment_length"`
	StreamID      uint32     `json:"stream_id"`
	SessionID     uint32     `json:"session_id"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	StoppedAt     *time.Time `json:"stopped_at,omitempty"`
}

type ArchiveRecord struct {
	RecordingID   int64
	Position      int64
	NextPosition  int64
	SegmentBase   int64
	RecordedAt    time.Time
	Message       Message
	RawFrame      []byte
	PayloadLength int
}

type ArchiveReplayConfig struct {
	RecordingID  int64
	FromPosition int64
	Length       int64
	StreamID     uint32
	SessionID    uint32
}

type ArchiveIntegrityReport struct {
	Path            string
	RecordingID     int64
	Recordings      uint64
	Records         uint64
	Bytes           int64
	LastPosition    int64
	CorruptPosition int64
}

type ArchiveSegmentState string

const (
	ArchiveSegmentAttached ArchiveSegmentState = "attached"
	ArchiveSegmentDetached ArchiveSegmentState = "detached"
	ArchiveSegmentMissing  ArchiveSegmentState = "missing"
)

type ArchiveSegmentDescriptor struct {
	RecordingID   int64               `json:"recording_id"`
	SegmentBase   int64               `json:"segment_base"`
	SegmentLength int64               `json:"segment_length"`
	Path          string              `json:"path"`
	Size          int64               `json:"size"`
	State         ArchiveSegmentState `json:"state"`
}

type archiveCatalog struct {
	Version         int                          `json:"version"`
	NextRecordingID int64                        `json:"next_recording_id"`
	Recordings      []ArchiveRecordingDescriptor `json:"recordings"`
}

type archiveEntry struct {
	record   ArchiveRecord
	padding  bool
	rawFrame bool
	frame    frame
}

type archivePendingRecord struct {
	packet        []byte
	message       Message
	rawFrame      []byte
	streamID      uint32
	sessionID     uint32
	payloadLength int
	recordedAt    time.Time
}

func OpenArchive(cfg ArchiveConfig) (*Archive, error) {
	if cfg.Path == "" {
		return nil, invalidConfigf("archive path is required")
	}
	if cfg.SegmentLength == 0 {
		cfg.SegmentLength = defaultArchiveSegmentLength
	}
	if cfg.SegmentLength < minArchiveSegmentLength {
		return nil, invalidConfigf("invalid archive segment length: %d", cfg.SegmentLength)
	}
	if err := os.MkdirAll(cfg.Path, 0o755); err != nil {
		return nil, fmt.Errorf("create archive directory: %w", err)
	}
	info, err := os.Stat(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("stat archive directory: %w", err)
	}
	if !info.IsDir() {
		return nil, invalidConfigf("archive path must be a directory: %s", cfg.Path)
	}

	a := &Archive{
		dir:                      cfg.Path,
		catalogPath:              filepath.Join(cfg.Path, archiveCatalogFile),
		sync:                     cfg.Sync,
		segmentLength:            cfg.SegmentLength,
		recordingProgressHandler: cfg.RecordingProgressHandler,
		recordingSignalHandler:   cfg.RecordingSignalHandler,
		catalog: archiveCatalog{
			Version:         1,
			NextRecordingID: 1,
		},
	}
	if err := a.loadCatalog(); err != nil {
		return nil, err
	}
	a.activeRecordingID = a.activeRecordingIDFromCatalog()
	a.extensionSignalPending = a.activeRecordingID != 0
	if len(a.catalog.Recordings) == 0 {
		if err := a.saveCatalogLocked(); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (a *Archive) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil
	}
	a.closed = true
	for id, subscriber := range a.recordingEventSubscribers {
		delete(a.recordingEventSubscribers, id)
		close(subscriber)
	}
	return nil
}

func (a *Archive) SubscribeRecordingEvents(buffer int) (<-chan ArchiveRecordingEvent, func(), error) {
	if a == nil {
		return nil, nil, ErrArchiveClosed
	}
	if buffer < 0 {
		return nil, nil, invalidConfigf("invalid archive recording event buffer: %d", buffer)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil, nil, ErrArchiveClosed
	}
	if a.recordingEventSubscribers == nil {
		a.recordingEventSubscribers = make(map[uint64]chan ArchiveRecordingEvent)
	}
	a.nextRecordingSubscriberID++
	id := a.nextRecordingSubscriberID
	ch := make(chan ArchiveRecordingEvent, buffer)
	a.recordingEventSubscribers[id] = ch

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			if subscriber, ok := a.recordingEventSubscribers[id]; ok {
				delete(a.recordingEventSubscribers, id)
				close(subscriber)
			}
		})
	}
	return ch, unsubscribe, nil
}

func (a *Archive) StartRecording(streamID, sessionID uint32) (ArchiveRecordingDescriptor, error) {
	now := time.Now().UTC()

	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return ArchiveRecordingDescriptor{}, ErrArchiveClosed
	}
	if a.activeRecordingID != 0 {
		a.mu.Unlock()
		return ArchiveRecordingDescriptor{}, ErrArchiveRecordingActive
	}
	index := a.createRecordingLocked(streamID, sessionID, now)
	desc := &a.catalog.Recordings[index]
	a.activeRecordingID = desc.RecordingID
	a.extensionSignalPending = false
	if err := a.saveCatalogLocked(); err != nil {
		a.mu.Unlock()
		return ArchiveRecordingDescriptor{}, err
	}
	descriptor := *desc
	event := archiveRecordingSignalEvent(ArchiveRecordingSignalStart, descriptor, descriptor.StartPosition, now)
	a.mu.Unlock()

	a.emitRecordingEvents([]ArchiveRecordingEvent{event})
	return descriptor, nil
}

func (a *Archive) StopRecording(recordingID int64) (ArchiveRecordingDescriptor, error) {
	now := time.Now().UTC()

	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return ArchiveRecordingDescriptor{}, ErrArchiveClosed
	}
	if a.activeRecordingID == 0 {
		a.mu.Unlock()
		return ArchiveRecordingDescriptor{}, ErrArchiveRecordingNotActive
	}
	if recordingID != 0 && recordingID != a.activeRecordingID {
		a.mu.Unlock()
		return ArchiveRecordingDescriptor{}, ErrArchiveRecordingNotActive
	}
	index, err := a.recordingIndexLocked(a.activeRecordingID)
	if err != nil {
		a.mu.Unlock()
		return ArchiveRecordingDescriptor{}, err
	}
	desc := &a.catalog.Recordings[index]
	desc.UpdatedAt = now
	desc.StoppedAt = &now
	a.activeRecordingID = 0
	a.extensionSignalPending = false
	if err := a.saveCatalogLocked(); err != nil {
		a.mu.Unlock()
		return ArchiveRecordingDescriptor{}, err
	}
	descriptor := *desc
	event := archiveRecordingSignalEvent(ArchiveRecordingSignalStop, descriptor, descriptor.StopPosition, now)
	a.mu.Unlock()

	a.emitRecordingEvents([]ArchiveRecordingEvent{event})
	return descriptor, nil
}

func (a *Archive) Record(msg Message) (ArchiveRecord, error) {
	if uint64(len(msg.Payload)) > archiveMaxPayload {
		return ArchiveRecord{}, fmt.Errorf("%w: payload too large: %d bytes", ErrInvalidConfig, len(msg.Payload))
	}

	recordedAt := time.Now().UTC()
	records, err := a.recordPending([]archivePendingRecord{{
		packet:        encodeArchiveRecord(msg, recordedAt),
		message:       cloneMessage(msg),
		streamID:      msg.StreamID,
		sessionID:     msg.SessionID,
		payloadLength: len(msg.Payload),
		recordedAt:    recordedAt,
	}})
	if err != nil {
		return ArchiveRecord{}, err
	}
	return records[0], nil
}

func (a *Archive) RecordFrame(rawFrame []byte) (ArchiveRecord, error) {
	records, err := a.RecordFrames([][]byte{rawFrame})
	if err != nil {
		return ArchiveRecord{}, err
	}
	return records[0], nil
}

func (a *Archive) RecordFrames(rawFrames [][]byte) ([]ArchiveRecord, error) {
	if len(rawFrames) == 0 {
		return nil, invalidConfigf("archive raw frame batch is empty")
	}
	recordedAt := time.Now().UTC()
	pending := make([]archivePendingRecord, 0, len(rawFrames))
	for _, rawFrame := range rawFrames {
		normalized, f, err := normalizeArchiveRawFrame(rawFrame)
		if err != nil {
			return nil, err
		}
		pending = append(pending, archivePendingRecord{
			packet:        encodeArchiveRawFrame(normalized, f, recordedAt),
			message:       archiveMessageFromFrameHeader(f),
			rawFrame:      normalized,
			streamID:      f.streamID,
			sessionID:     f.sessionID,
			payloadLength: len(normalized),
			recordedAt:    recordedAt,
		})
	}
	return a.recordPending(pending)
}

func (a *Archive) recordPending(pending []archivePendingRecord) ([]ArchiveRecord, error) {
	if len(pending) == 0 {
		return nil, invalidConfigf("archive record batch is empty")
	}

	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, ErrArchiveClosed
	}
	index, created, err := a.ensureRecordingForStreamLocked(pending[0].streamID, pending[0].sessionID, pending[0].recordedAt)
	if err != nil {
		a.mu.Unlock()
		return nil, err
	}
	desc := &a.catalog.Recordings[index]
	for _, pendingRecord := range pending {
		if int64(len(pendingRecord.packet)) > desc.SegmentLength {
			a.mu.Unlock()
			return nil, fmt.Errorf("%w: record length %d exceeds segment length %d", ErrInvalidConfig, len(pendingRecord.packet), desc.SegmentLength)
		}
	}
	records := make([]ArchiveRecord, 0, len(pending))
	for _, pendingRecord := range pending {
		position := desc.StopPosition
		position, err = a.padSegmentIfNeededLocked(desc, position, int64(len(pendingRecord.packet)))
		if err != nil {
			a.mu.Unlock()
			return nil, err
		}

		segmentBase := archiveSegmentBase(position, desc.SegmentLength)
		segmentOffset := position - segmentBase
		if err := a.writeSegmentLocked(desc.RecordingID, segmentBase, segmentOffset, pendingRecord.packet); err != nil {
			a.mu.Unlock()
			return nil, err
		}

		nextPosition := position + int64(len(pendingRecord.packet))
		desc.StopPosition = nextPosition
		desc.UpdatedAt = pendingRecord.recordedAt
		a.updateDescriptorStreamSession(desc, pendingRecord.streamID, pendingRecord.sessionID)
		records = append(records, ArchiveRecord{
			RecordingID:   desc.RecordingID,
			Position:      position,
			NextPosition:  nextPosition,
			SegmentBase:   segmentBase,
			RecordedAt:    pendingRecord.recordedAt,
			Message:       cloneMessage(pendingRecord.message),
			RawFrame:      cloneBytes(pendingRecord.rawFrame),
			PayloadLength: pendingRecord.payloadLength,
		})
	}
	if err := a.saveCatalogLocked(); err != nil {
		a.mu.Unlock()
		return nil, err
	}

	descriptor := *desc
	events := make([]ArchiveRecordingEvent, 0, len(records)+1)
	if created {
		events = append(events, archiveRecordingEvent(ArchiveRecordingSignalStart, descriptor, records[0]))
	} else if a.extensionSignalPending {
		a.extensionSignalPending = false
		events = append(events, archiveRecordingEvent(ArchiveRecordingSignalExtend, descriptor, records[0]))
	}
	for _, record := range records {
		events = append(events, archiveRecordingEvent(ArchiveRecordingSignalProgress, descriptor, record))
	}
	a.mu.Unlock()

	a.emitRecordingEvents(events)
	return records, nil
}

func (a *Archive) RecordingHandler(next Handler) Handler {
	return func(ctx context.Context, msg Message) error {
		if _, err := a.Record(msg); err != nil {
			return err
		}
		if next == nil {
			return nil
		}
		return next(ctx, msg)
	}
}

func (a *Archive) Replay(ctx context.Context, cfg ArchiveReplayConfig, handler Handler) error {
	if handler == nil {
		return errors.New("handler is required")
	}
	if cfg.FromPosition < 0 {
		return fmt.Errorf("%w: %d", ErrArchivePosition, cfg.FromPosition)
	}
	if cfg.Length < 0 {
		return fmt.Errorf("%w: %d", ErrArchivePosition, cfg.Length)
	}
	if cfg.Length > 0 && cfg.FromPosition > int64(^uint64(0)>>1)-cfg.Length {
		return fmt.Errorf("%w: replay length overflows position: from=%d length=%d", ErrArchivePosition, cfg.FromPosition, cfg.Length)
	}

	recordings, err := a.recordingSnapshot(cfg.RecordingID)
	if err != nil {
		return err
	}
	for _, desc := range recordings {
		rawReplay := newArchiveRawReplay()
		if cfg.FromPosition > desc.StopPosition {
			return fmt.Errorf("%w: %d", ErrArchivePosition, cfg.FromPosition)
		}
		position := max(cfg.FromPosition, desc.StartPosition)
		replayLimit := archiveReplayLimit(cfg, desc)
		for position < replayLimit {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			entry, err := a.readArchiveEntry(desc, position)
			if err != nil {
				return err
			}
			position = entry.record.NextPosition
			if entry.record.NextPosition > replayLimit {
				break
			}
			if entry.padding || !archiveReplayMatch(cfg, entry.record.Message) {
				continue
			}
			if entry.rawFrame {
				msg, ready, err := rawReplay.observe(entry.frame)
				if err != nil {
					return err
				}
				if !ready || !archiveReplayMatch(cfg, msg) {
					continue
				}
				if err := handler(ctx, msg); err != nil {
					return err
				}
				continue
			}
			if err := handler(ctx, entry.record.Message); err != nil {
				return err
			}
		}
	}
	return nil
}

func archiveReplayLimit(cfg ArchiveReplayConfig, desc ArchiveRecordingDescriptor) int64 {
	if cfg.Length <= 0 {
		return desc.StopPosition
	}
	return min(desc.StopPosition, cfg.FromPosition+cfg.Length)
}

type archiveRawReplay struct {
	pending map[archiveRawFrameKey]*archiveRawFrameSet
}

type archiveRawFrameKey struct {
	streamID  uint32
	sessionID uint32
	seq       uint64
	reserved  uint64
}

type archiveRawFrameSet struct {
	frames   []frame
	received []bool
	count    int
}

func newArchiveRawReplay() *archiveRawReplay {
	return &archiveRawReplay{pending: make(map[archiveRawFrameKey]*archiveRawFrameSet)}
}

func (r *archiveRawReplay) observe(f frame) (Message, bool, error) {
	if f.typ != frameData {
		return Message{}, false, nil
	}
	if f.fragmentCount <= 1 {
		responseChannel, payload, err := decodeDataPayload(f)
		if err != nil {
			return Message{}, false, err
		}
		msg := archiveMessageFromFrameHeader(f)
		msg.Payload = cloneBytes(payload)
		msg.ResponseChannel = responseChannel
		return msg, true, nil
	}
	if f.flags&frameFlagFragment == 0 {
		return Message{}, false, errors.New("fragment flag missing")
	}
	fragmentCount := int(f.fragmentCount)
	if int(f.fragmentIndex) >= fragmentCount {
		return Message{}, false, errors.New("invalid fragment metadata")
	}
	key := archiveRawFrameKey{
		streamID:  f.streamID,
		sessionID: f.sessionID,
		seq:       f.seq,
		reserved:  f.reserved,
	}
	set := r.pending[key]
	if set == nil {
		set = &archiveRawFrameSet{
			frames:   make([]frame, fragmentCount),
			received: make([]bool, fragmentCount),
		}
		r.pending[key] = set
	}
	if len(set.frames) != fragmentCount {
		delete(r.pending, key)
		return Message{}, false, errors.New("fragment count changed")
	}
	index := int(f.fragmentIndex)
	if !set.received[index] {
		set.frames[index] = cloneUDPFrame(f)
		set.received[index] = true
		set.count++
	}
	if set.count < fragmentCount {
		return Message{}, false, nil
	}

	payload, responseChannel, _, err := reassembleDataFrames(set.frames)
	if err != nil {
		delete(r.pending, key)
		return Message{}, false, err
	}
	msg := archiveMessageFromFrameHeader(set.frames[0])
	msg.Payload = payload
	msg.ResponseChannel = responseChannel
	delete(r.pending, key)
	return msg, true, nil
}

func (a *Archive) IntegrityScan() (ArchiveIntegrityReport, error) {
	recordings, err := a.recordingSnapshot(0)
	if err != nil {
		return ArchiveIntegrityReport{}, err
	}
	report := ArchiveIntegrityReport{
		Path:            a.dir,
		CorruptPosition: -1,
	}
	for _, desc := range recordings {
		report.Recordings++
		report.Bytes += desc.StopPosition - desc.StartPosition
		for position := desc.StartPosition; position < desc.StopPosition; {
			entry, err := a.readArchiveEntry(desc, position)
			if err != nil {
				report.RecordingID = desc.RecordingID
				report.CorruptPosition = position
				return report, err
			}
			position = entry.record.NextPosition
			if entry.padding {
				continue
			}
			report.Records++
			report.RecordingID = desc.RecordingID
			report.LastPosition = entry.record.Position
		}
	}
	return report, nil
}

func (a *Archive) Truncate(position int64) error {
	return a.TruncateRecording(0, position)
}

func (a *Archive) TruncateRecording(recordingID, position int64) error {
	if position < 0 {
		return fmt.Errorf("%w: %d", ErrArchivePosition, position)
	}

	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return ErrArchiveClosed
	}
	index, err := a.recordingIndexLocked(recordingID)
	if err != nil {
		a.mu.Unlock()
		return err
	}
	desc := &a.catalog.Recordings[index]
	if position > desc.StopPosition {
		a.mu.Unlock()
		return fmt.Errorf("%w: %d", ErrArchivePosition, position)
	}
	if err := a.validateBoundaryLocked(*desc, position); err != nil {
		a.mu.Unlock()
		return err
	}
	if err := a.truncateSegmentsLocked(desc, position); err != nil {
		a.mu.Unlock()
		return err
	}
	now := time.Now().UTC()
	desc.StopPosition = position
	desc.UpdatedAt = now
	if err := a.saveCatalogLocked(); err != nil {
		a.mu.Unlock()
		return err
	}
	descriptor := *desc
	event := ArchiveRecordingEvent{
		Signal:       ArchiveRecordingSignalTruncate,
		RecordingID:  descriptor.RecordingID,
		Position:     position,
		NextPosition: position,
		SegmentBase:  archiveSegmentBase(position, descriptor.SegmentLength),
		Descriptor:   descriptor,
		RecordedAt:   now,
	}
	a.mu.Unlock()

	a.emitRecordingEvents([]ArchiveRecordingEvent{event})
	return nil
}

func (a *Archive) Purge() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return ErrArchiveClosed
	}
	purged := append([]ArchiveRecordingDescriptor(nil), a.catalog.Recordings...)
	for _, pattern := range []string{"*.rec", "*" + archiveDetachedSegmentExt} {
		matches, err := filepath.Glob(filepath.Join(a.dir, pattern))
		if err != nil {
			a.mu.Unlock()
			return err
		}
		for _, path := range matches {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				a.mu.Unlock()
				return err
			}
		}
	}
	a.catalog = archiveCatalog{
		Version:         1,
		NextRecordingID: 1,
	}
	a.activeRecordingID = 0
	a.extensionSignalPending = false
	if err := a.saveCatalogLocked(); err != nil {
		a.mu.Unlock()
		return err
	}
	now := time.Now().UTC()
	events := make([]ArchiveRecordingEvent, 0, len(purged))
	for _, descriptor := range purged {
		events = append(events, ArchiveRecordingEvent{
			Signal:       ArchiveRecordingSignalPurge,
			RecordingID:  descriptor.RecordingID,
			Position:     descriptor.StopPosition,
			NextPosition: descriptor.StopPosition,
			Descriptor:   descriptor,
			RecordedAt:   now,
		})
	}
	a.mu.Unlock()

	a.emitRecordingEvents(events)
	return nil
}

func (a *Archive) ListRecordings() ([]ArchiveRecordingDescriptor, error) {
	recordings, err := a.recordingSnapshot(0)
	if err != nil {
		return nil, err
	}
	return recordings, nil
}

func (a *Archive) RecordingDescriptor(recordingID int64) (ArchiveRecordingDescriptor, error) {
	recordings, err := a.recordingSnapshot(recordingID)
	if err != nil {
		return ArchiveRecordingDescriptor{}, err
	}
	if len(recordings) == 0 {
		return ArchiveRecordingDescriptor{}, fmt.Errorf("%w: recording %d", ErrArchivePosition, recordingID)
	}
	return recordings[0], nil
}

func (a *Archive) ListRecordingSegments(recordingID int64) ([]ArchiveSegmentDescriptor, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil, ErrArchiveClosed
	}
	index, err := a.recordingIndexLocked(recordingID)
	if err != nil {
		return nil, err
	}
	desc := a.catalog.Recordings[index]
	var segments []ArchiveSegmentDescriptor
	for base := desc.StartPosition; base < desc.StopPosition; base += desc.SegmentLength {
		segments = append(segments, a.segmentDescriptorLocked(desc, base))
	}
	return segments, nil
}

func (a *Archive) DetachRecordingSegment(recordingID, segmentBase int64) (ArchiveSegmentDescriptor, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return ArchiveSegmentDescriptor{}, ErrArchiveClosed
	}
	index, err := a.recordingIndexLocked(recordingID)
	if err != nil {
		return ArchiveSegmentDescriptor{}, err
	}
	desc := a.catalog.Recordings[index]
	if err := validateArchiveSegmentBase(desc, segmentBase); err != nil {
		return ArchiveSegmentDescriptor{}, err
	}
	attached := a.segmentPath(desc.RecordingID, segmentBase)
	detached := a.detachedSegmentPath(desc.RecordingID, segmentBase)
	if _, err := os.Stat(detached); err == nil {
		return a.segmentDescriptorLocked(desc, segmentBase), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ArchiveSegmentDescriptor{}, fmt.Errorf("stat detached archive segment: %w", err)
	}
	if err := os.Rename(attached, detached); err != nil {
		return ArchiveSegmentDescriptor{}, fmt.Errorf("detach archive segment: %w", err)
	}
	return a.segmentDescriptorLocked(desc, segmentBase), nil
}

func (a *Archive) AttachRecordingSegment(recordingID, segmentBase int64) (ArchiveSegmentDescriptor, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return ArchiveSegmentDescriptor{}, ErrArchiveClosed
	}
	index, err := a.recordingIndexLocked(recordingID)
	if err != nil {
		return ArchiveSegmentDescriptor{}, err
	}
	desc := a.catalog.Recordings[index]
	if err := validateArchiveSegmentBase(desc, segmentBase); err != nil {
		return ArchiveSegmentDescriptor{}, err
	}
	attached := a.segmentPath(desc.RecordingID, segmentBase)
	detached := a.detachedSegmentPath(desc.RecordingID, segmentBase)
	if _, err := os.Stat(attached); err == nil {
		return a.segmentDescriptorLocked(desc, segmentBase), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ArchiveSegmentDescriptor{}, fmt.Errorf("stat attached archive segment: %w", err)
	}
	if err := os.Rename(detached, attached); err != nil {
		return ArchiveSegmentDescriptor{}, fmt.Errorf("attach archive segment: %w", err)
	}
	return a.segmentDescriptorLocked(desc, segmentBase), nil
}

func (a *Archive) DeleteDetachedRecordingSegment(recordingID, segmentBase int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return ErrArchiveClosed
	}
	index, err := a.recordingIndexLocked(recordingID)
	if err != nil {
		return err
	}
	desc := a.catalog.Recordings[index]
	if err := validateArchiveSegmentBase(desc, segmentBase); err != nil {
		return err
	}
	if _, err := os.Stat(a.segmentPath(desc.RecordingID, segmentBase)); err == nil {
		return fmt.Errorf("%w: attached segment %d", ErrArchivePosition, segmentBase)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat attached archive segment: %w", err)
	}
	if err := os.Remove(a.detachedSegmentPath(desc.RecordingID, segmentBase)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("delete detached archive segment: %w", err)
	}
	return nil
}

func (a *Archive) MigrateDetachedRecordingSegment(recordingID, segmentBase int64, dstDir string) (ArchiveSegmentDescriptor, error) {
	if dstDir == "" {
		return ArchiveSegmentDescriptor{}, invalidConfigf("archive segment migration destination is required")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return ArchiveSegmentDescriptor{}, ErrArchiveClosed
	}
	index, err := a.recordingIndexLocked(recordingID)
	if err != nil {
		return ArchiveSegmentDescriptor{}, err
	}
	desc := a.catalog.Recordings[index]
	if err := validateArchiveSegmentBase(desc, segmentBase); err != nil {
		return ArchiveSegmentDescriptor{}, err
	}
	src := a.detachedSegmentPath(desc.RecordingID, segmentBase)
	if _, err := os.Stat(src); err != nil {
		return ArchiveSegmentDescriptor{}, fmt.Errorf("stat detached archive segment: %w", err)
	}
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return ArchiveSegmentDescriptor{}, fmt.Errorf("create archive segment migration directory: %w", err)
	}
	dst := filepath.Join(dstDir, filepath.Base(src))
	if _, err := os.Stat(dst); err == nil {
		return ArchiveSegmentDescriptor{}, fmt.Errorf("%w: destination segment exists: %s", ErrArchivePosition, dst)
	} else if !errors.Is(err, os.ErrNotExist) {
		return ArchiveSegmentDescriptor{}, fmt.Errorf("stat archive segment migration destination: %w", err)
	}
	if err := os.Rename(src, dst); err != nil {
		return ArchiveSegmentDescriptor{}, fmt.Errorf("migrate detached archive segment: %w", err)
	}
	info, err := os.Stat(dst)
	if err != nil {
		return ArchiveSegmentDescriptor{}, fmt.Errorf("stat migrated archive segment: %w", err)
	}
	return ArchiveSegmentDescriptor{
		RecordingID:   desc.RecordingID,
		SegmentBase:   segmentBase,
		SegmentLength: desc.SegmentLength,
		Path:          dst,
		Size:          info.Size(),
		State:         ArchiveSegmentDetached,
	}, nil
}

func (a *Archive) loadCatalog() error {
	data, err := os.ReadFile(a.catalogPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read archive catalog: %w", err)
	}
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, &a.catalog); err != nil {
		return fmt.Errorf("%w: decode archive catalog: %w", ErrArchiveCorrupt, err)
	}
	if a.catalog.Version != 1 {
		return fmt.Errorf("%w: unsupported archive catalog version %d", ErrArchiveCorrupt, a.catalog.Version)
	}
	if a.catalog.NextRecordingID <= 0 {
		a.catalog.NextRecordingID = 1
	}
	for _, desc := range a.catalog.Recordings {
		if desc.RecordingID <= 0 || desc.SegmentLength < minArchiveSegmentLength ||
			desc.StartPosition < 0 || desc.StopPosition < desc.StartPosition {
			return fmt.Errorf("%w: invalid recording descriptor %d", ErrArchiveCorrupt, desc.RecordingID)
		}
		if desc.RecordingID >= a.catalog.NextRecordingID {
			a.catalog.NextRecordingID = desc.RecordingID + 1
		}
	}
	sortArchiveRecordings(a.catalog.Recordings)
	return nil
}

func (a *Archive) activeRecordingIDFromCatalog() int64 {
	for i := len(a.catalog.Recordings) - 1; i >= 0; i-- {
		desc := a.catalog.Recordings[i]
		if desc.StoppedAt == nil {
			return desc.RecordingID
		}
	}
	return 0
}

func (a *Archive) saveCatalogLocked() error {
	data, err := json.MarshalIndent(a.catalog, "", "  ")
	if err != nil {
		return fmt.Errorf("encode archive catalog: %w", err)
	}
	data = append(data, '\n')
	tmp := a.catalogPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write archive catalog: %w", err)
	}
	if a.sync {
		file, err := os.OpenFile(tmp, os.O_RDWR, 0)
		if err != nil {
			return fmt.Errorf("open archive catalog temp: %w", err)
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return fmt.Errorf("sync archive catalog temp: %w", err)
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("close archive catalog temp: %w", err)
		}
	}
	if err := os.Rename(tmp, a.catalogPath); err != nil {
		return fmt.Errorf("replace archive catalog: %w", err)
	}
	return nil
}

func (a *Archive) ensureRecordingLocked(msg Message, now time.Time) (int, bool, error) {
	return a.ensureRecordingForStreamLocked(msg.StreamID, msg.SessionID, now)
}

func (a *Archive) ensureRecordingForStreamLocked(streamID, sessionID uint32, now time.Time) (int, bool, error) {
	if a.activeRecordingID != 0 {
		index, err := a.recordingIndexLocked(a.activeRecordingID)
		if err != nil {
			return 0, false, err
		}
		return index, false, nil
	}
	index := a.createRecordingLocked(streamID, sessionID, now)
	return index, true, nil
}

func (a *Archive) createRecordingLocked(streamID, sessionID uint32, now time.Time) int {
	recordingID := a.catalog.NextRecordingID
	if recordingID <= 0 {
		recordingID = 1
	}
	a.catalog.NextRecordingID = recordingID + 1
	a.catalog.Recordings = append(a.catalog.Recordings, ArchiveRecordingDescriptor{
		RecordingID:   recordingID,
		StartPosition: 0,
		StopPosition:  0,
		SegmentLength: a.segmentLength,
		StreamID:      streamID,
		SessionID:     sessionID,
		CreatedAt:     now,
		UpdatedAt:     now,
	})
	a.activeRecordingID = recordingID
	a.extensionSignalPending = false
	return len(a.catalog.Recordings) - 1
}

func (a *Archive) updateDescriptorStreamSession(desc *ArchiveRecordingDescriptor, streamID, sessionID uint32) {
	if desc.StreamID != streamID {
		desc.StreamID = 0
	}
	if desc.SessionID != sessionID {
		desc.SessionID = 0
	}
}

func (a *Archive) padSegmentIfNeededLocked(desc *ArchiveRecordingDescriptor, position, recordLen int64) (int64, error) {
	if recordLen > desc.SegmentLength {
		return 0, fmt.Errorf("%w: record length %d exceeds segment length %d", ErrInvalidConfig, recordLen, desc.SegmentLength)
	}
	segmentOffset := position - archiveSegmentBase(position, desc.SegmentLength)
	tail := desc.SegmentLength - segmentOffset
	if recordLen <= tail {
		return position, nil
	}
	if tail >= archiveHeaderLen {
		padding := encodeArchivePadding(tail)
		segmentBase := archiveSegmentBase(position, desc.SegmentLength)
		if err := a.writeSegmentLocked(desc.RecordingID, segmentBase, segmentOffset, padding); err != nil {
			return 0, err
		}
	}
	return position + tail, nil
}

func (a *Archive) writeSegmentLocked(recordingID, segmentBase, offset int64, packet []byte) error {
	path := a.segmentPath(recordingID, segmentBase)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open archive segment: %w", err)
	}
	defer file.Close()
	if _, err := file.WriteAt(packet, offset); err != nil {
		return fmt.Errorf("write archive segment: %w", err)
	}
	if a.sync {
		if err := file.Sync(); err != nil {
			return fmt.Errorf("sync archive segment: %w", err)
		}
	}
	return nil
}

func (a *Archive) recordingSnapshot(recordingID int64) ([]ArchiveRecordingDescriptor, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil, ErrArchiveClosed
	}
	if recordingID == 0 {
		recordings := append([]ArchiveRecordingDescriptor(nil), a.catalog.Recordings...)
		sortArchiveRecordings(recordings)
		return recordings, nil
	}
	for _, desc := range a.catalog.Recordings {
		if desc.RecordingID == recordingID {
			return []ArchiveRecordingDescriptor{desc}, nil
		}
	}
	return nil, fmt.Errorf("%w: recording %d", ErrArchivePosition, recordingID)
}

func sortArchiveRecordings(recordings []ArchiveRecordingDescriptor) {
	sort.Slice(recordings, func(i, j int) bool {
		return recordings[i].RecordingID < recordings[j].RecordingID
	})
}

func (a *Archive) recordingIndexLocked(recordingID int64) (int, error) {
	if len(a.catalog.Recordings) == 0 {
		return 0, fmt.Errorf("%w: no recordings", ErrArchivePosition)
	}
	if recordingID == 0 {
		return len(a.catalog.Recordings) - 1, nil
	}
	for i, desc := range a.catalog.Recordings {
		if desc.RecordingID == recordingID {
			return i, nil
		}
	}
	return 0, fmt.Errorf("%w: recording %d", ErrArchivePosition, recordingID)
}

func (a *Archive) validateBoundaryLocked(desc ArchiveRecordingDescriptor, position int64) error {
	if position == desc.StartPosition || position == desc.StopPosition {
		return nil
	}
	for offset := desc.StartPosition; offset < desc.StopPosition; {
		entry, err := a.readArchiveEntry(desc, offset)
		if err != nil {
			return err
		}
		if entry.record.NextPosition == position {
			return nil
		}
		if entry.record.NextPosition > position {
			break
		}
		offset = entry.record.NextPosition
	}
	return fmt.Errorf("%w: %d", ErrArchivePosition, position)
}

func (a *Archive) truncateSegmentsLocked(desc *ArchiveRecordingDescriptor, position int64) error {
	keepBase := archiveSegmentBase(position, desc.SegmentLength)
	if position == desc.StartPosition {
		keepBase = -1
	}
	for base := desc.StartPosition; base < desc.StopPosition; base += desc.SegmentLength {
		if keepBase >= 0 && base < keepBase {
			continue
		}
		if base == keepBase {
			offset := position - base
			if err := os.Truncate(a.segmentPath(desc.RecordingID, base), offset); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("truncate archive segment: %w", err)
			}
			continue
		}
		for _, path := range []string{
			a.segmentPath(desc.RecordingID, base),
			a.detachedSegmentPath(desc.RecordingID, base),
		} {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove archive segment: %w", err)
			}
		}
	}
	return nil
}

func (a *Archive) readArchiveEntry(desc ArchiveRecordingDescriptor, position int64) (archiveEntry, error) {
	if position < desc.StartPosition || position > desc.StopPosition {
		return archiveEntry{}, fmt.Errorf("%w: %d", ErrArchivePosition, position)
	}
	segmentBase := archiveSegmentBase(position, desc.SegmentLength)
	segmentOffset := position - segmentBase
	tail := desc.SegmentLength - segmentOffset
	if tail < archiveHeaderLen {
		return archiveEntry{
			record: ArchiveRecord{
				RecordingID:  desc.RecordingID,
				Position:     position,
				NextPosition: segmentBase + desc.SegmentLength,
				SegmentBase:  segmentBase,
			},
			padding: true,
		}, nil
	}

	file, err := os.Open(a.segmentPath(desc.RecordingID, segmentBase))
	if err != nil {
		return archiveEntry{}, fmt.Errorf("%w: open segment at position %d: %w", ErrArchiveCorrupt, position, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return archiveEntry{}, fmt.Errorf("stat archive segment: %w", err)
	}
	segmentLimit := min(info.Size(), desc.SegmentLength)
	if desc.StopPosition < segmentBase+segmentLimit {
		segmentLimit = desc.StopPosition - segmentBase
	}
	entry, err := readArchiveEntryAt(file, desc.RecordingID, segmentBase, segmentOffset, segmentLimit)
	if err != nil {
		return archiveEntry{}, err
	}
	return entry, nil
}

func (a *Archive) segmentPath(recordingID, segmentBase int64) string {
	return filepath.Join(a.dir, fmt.Sprintf("%d-%d.rec", recordingID, segmentBase))
}

func (a *Archive) detachedSegmentPath(recordingID, segmentBase int64) string {
	return filepath.Join(a.dir, fmt.Sprintf("%d-%d%s", recordingID, segmentBase, archiveDetachedSegmentExt))
}

func (a *Archive) segmentDescriptorLocked(desc ArchiveRecordingDescriptor, segmentBase int64) ArchiveSegmentDescriptor {
	attached := a.segmentPath(desc.RecordingID, segmentBase)
	if info, err := os.Stat(attached); err == nil {
		return ArchiveSegmentDescriptor{
			RecordingID:   desc.RecordingID,
			SegmentBase:   segmentBase,
			SegmentLength: desc.SegmentLength,
			Path:          attached,
			Size:          info.Size(),
			State:         ArchiveSegmentAttached,
		}
	}
	detached := a.detachedSegmentPath(desc.RecordingID, segmentBase)
	if info, err := os.Stat(detached); err == nil {
		return ArchiveSegmentDescriptor{
			RecordingID:   desc.RecordingID,
			SegmentBase:   segmentBase,
			SegmentLength: desc.SegmentLength,
			Path:          detached,
			Size:          info.Size(),
			State:         ArchiveSegmentDetached,
		}
	}
	return ArchiveSegmentDescriptor{
		RecordingID:   desc.RecordingID,
		SegmentBase:   segmentBase,
		SegmentLength: desc.SegmentLength,
		State:         ArchiveSegmentMissing,
	}
}

func (a *Archive) emitRecordingEvents(events []ArchiveRecordingEvent) {
	for _, event := range events {
		if event.Signal == ArchiveRecordingSignalProgress {
			if a.recordingProgressHandler != nil {
				a.recordingProgressHandler(event)
			}
			continue
		}
		if a.recordingSignalHandler != nil {
			a.recordingSignalHandler(event)
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, event := range events {
		for _, subscriber := range a.recordingEventSubscribers {
			select {
			case subscriber <- event:
			default:
			}
		}
	}
}

func archiveRecordingEvent(signal ArchiveRecordingSignal, descriptor ArchiveRecordingDescriptor, record ArchiveRecord) ArchiveRecordingEvent {
	payloadLength := record.PayloadLength
	if payloadLength == 0 {
		payloadLength = len(record.Message.Payload)
	}
	return ArchiveRecordingEvent{
		Signal:        signal,
		RecordingID:   descriptor.RecordingID,
		Position:      record.Position,
		NextPosition:  record.NextPosition,
		SegmentBase:   record.SegmentBase,
		StreamID:      record.Message.StreamID,
		SessionID:     record.Message.SessionID,
		Sequence:      record.Message.Sequence,
		PayloadLength: payloadLength,
		Descriptor:    descriptor,
		RecordedAt:    record.RecordedAt,
	}
}

func archiveRecordingSignalEvent(signal ArchiveRecordingSignal, descriptor ArchiveRecordingDescriptor, position int64, recordedAt time.Time) ArchiveRecordingEvent {
	return ArchiveRecordingEvent{
		Signal:       signal,
		RecordingID:  descriptor.RecordingID,
		Position:     position,
		NextPosition: position,
		SegmentBase:  archiveSegmentBase(position, descriptor.SegmentLength),
		StreamID:     descriptor.StreamID,
		SessionID:    descriptor.SessionID,
		Descriptor:   descriptor,
		RecordedAt:   recordedAt,
	}
}

func validateArchiveSegmentBase(desc ArchiveRecordingDescriptor, segmentBase int64) error {
	if segmentBase < desc.StartPosition || segmentBase >= desc.StopPosition {
		return fmt.Errorf("%w: segment %d", ErrArchivePosition, segmentBase)
	}
	if archiveSegmentBase(segmentBase, desc.SegmentLength) != segmentBase {
		return fmt.Errorf("%w: segment %d", ErrArchivePosition, segmentBase)
	}
	if segmentBase+desc.SegmentLength > desc.StopPosition {
		return fmt.Errorf("%w: segment %d is active or incomplete", ErrArchivePosition, segmentBase)
	}
	return nil
}

func normalizeArchiveRawFrame(rawFrame []byte) ([]byte, frame, error) {
	if len(rawFrame) == 0 {
		return nil, frame{}, invalidConfigf("archive raw frame is empty")
	}
	if uint64(len(rawFrame)) > archiveMaxPayload {
		return nil, frame{}, fmt.Errorf("%w: raw frame too large: %d bytes", ErrInvalidConfig, len(rawFrame))
	}
	frames, err := decodeFrames(rawFrame)
	if err != nil {
		return nil, frame{}, fmt.Errorf("%w: invalid raw frame: %w", ErrInvalidConfig, err)
	}
	if len(frames) != 1 {
		return nil, frame{}, fmt.Errorf("%w: raw frame record must contain exactly one frame: %d", ErrInvalidConfig, len(frames))
	}
	if frames[0].typ != frameData {
		return nil, frame{}, fmt.Errorf("%w: raw archive frame must be DATA", ErrInvalidConfig)
	}
	return cloneBytes(rawFrame), frames[0], nil
}

func archiveMessageFromFrameHeader(f frame) Message {
	return Message{
		StreamID:      f.streamID,
		SessionID:     f.sessionID,
		TermID:        f.termID,
		TermOffset:    f.termOffset,
		Sequence:      f.seq,
		ReservedValue: f.reserved,
	}
}

func encodeArchiveRecord(msg Message, recordedAt time.Time) []byte {
	payload := append([]byte(nil), msg.Payload...)
	recordLen := archiveHeaderLen + len(payload)
	buf := make([]byte, recordLen)

	copy(buf[0:4], archiveMagic)
	buf[4] = archiveVersion
	frameByteOrder.PutUint16(buf[6:8], uint16(archiveHeaderLen))
	frameByteOrder.PutUint64(buf[8:16], uint64(recordLen))
	frameByteOrder.PutUint32(buf[16:20], msg.StreamID)
	frameByteOrder.PutUint32(buf[20:24], msg.SessionID)
	frameByteOrder.PutUint32(buf[24:28], uint32(msg.TermID))
	frameByteOrder.PutUint32(buf[28:32], uint32(msg.TermOffset))
	frameByteOrder.PutUint64(buf[32:40], msg.Sequence)
	frameByteOrder.PutUint64(buf[40:48], msg.ReservedValue)
	frameByteOrder.PutUint64(buf[48:56], uint64(recordedAt.UnixNano()))
	frameByteOrder.PutUint32(buf[56:60], uint32(len(payload)))
	copy(buf[archiveHeaderLen:], payload)
	frameByteOrder.PutUint32(buf[60:64], archiveRecordCRC(buf))
	return buf
}

func encodeArchiveRawFrame(rawFrame []byte, f frame, recordedAt time.Time) []byte {
	payload := cloneBytes(rawFrame)
	recordLen := archiveHeaderLen + len(payload)
	buf := make([]byte, recordLen)

	copy(buf[0:4], archiveMagic)
	buf[4] = archiveVersion
	buf[5] = archiveRecordFlagRawFrame
	frameByteOrder.PutUint16(buf[6:8], uint16(archiveHeaderLen))
	frameByteOrder.PutUint64(buf[8:16], uint64(recordLen))
	frameByteOrder.PutUint32(buf[16:20], f.streamID)
	frameByteOrder.PutUint32(buf[20:24], f.sessionID)
	frameByteOrder.PutUint32(buf[24:28], uint32(f.termID))
	frameByteOrder.PutUint32(buf[28:32], uint32(f.termOffset))
	frameByteOrder.PutUint64(buf[32:40], f.seq)
	frameByteOrder.PutUint64(buf[40:48], f.reserved)
	frameByteOrder.PutUint64(buf[48:56], uint64(recordedAt.UnixNano()))
	frameByteOrder.PutUint32(buf[56:60], uint32(len(payload)))
	copy(buf[archiveHeaderLen:], payload)
	frameByteOrder.PutUint32(buf[60:64], archiveRecordCRC(buf))
	return buf
}

func encodeArchivePadding(recordLen int64) []byte {
	buf := make([]byte, recordLen)
	copy(buf[0:4], archiveMagic)
	buf[4] = archiveVersion
	buf[5] = archiveRecordFlagPadding
	frameByteOrder.PutUint16(buf[6:8], uint16(archiveHeaderLen))
	frameByteOrder.PutUint64(buf[8:16], uint64(recordLen))
	frameByteOrder.PutUint32(buf[60:64], archiveRecordCRC(buf))
	return buf
}

func readArchiveEntryAt(reader io.ReaderAt, recordingID, segmentBase, segmentOffset, segmentLimit int64) (archiveEntry, error) {
	position := segmentBase + segmentOffset
	if segmentOffset < 0 || segmentOffset > segmentLimit {
		return archiveEntry{}, fmt.Errorf("%w: %d", ErrArchivePosition, position)
	}
	if segmentLimit-segmentOffset < archiveHeaderLen {
		return archiveEntry{}, fmt.Errorf("%w at position %d: incomplete header", ErrArchiveCorrupt, position)
	}

	header := make([]byte, archiveHeaderLen)
	if _, err := reader.ReadAt(header, segmentOffset); err != nil {
		return archiveEntry{}, fmt.Errorf("read archive header: %w", err)
	}
	if string(header[0:4]) != archiveMagic {
		return archiveEntry{}, fmt.Errorf("%w at position %d: invalid magic", ErrArchiveCorrupt, position)
	}
	if header[4] != archiveVersion {
		return archiveEntry{}, fmt.Errorf("%w at position %d: unsupported version %d", ErrArchiveCorrupt, position, header[4])
	}
	if headerLen := int(frameByteOrder.Uint16(header[6:8])); headerLen != archiveHeaderLen {
		return archiveEntry{}, fmt.Errorf("%w at position %d: invalid header length %d", ErrArchiveCorrupt, position, headerLen)
	}
	flags := header[5]
	if flags&^(archiveRecordFlagPadding|archiveRecordFlagRawFrame) != 0 {
		return archiveEntry{}, fmt.Errorf("%w at position %d: unsupported flags %d", ErrArchiveCorrupt, position, flags)
	}
	if flags&archiveRecordFlagPadding != 0 && flags&archiveRecordFlagRawFrame != 0 {
		return archiveEntry{}, fmt.Errorf("%w at position %d: invalid flags %d", ErrArchiveCorrupt, position, flags)
	}
	recordLen := int64(frameByteOrder.Uint64(header[8:16]))
	payloadLen := int64(frameByteOrder.Uint32(header[56:60]))
	if recordLen < archiveHeaderLen || segmentOffset+recordLen > segmentLimit {
		return archiveEntry{}, fmt.Errorf("%w at position %d: invalid record length %d", ErrArchiveCorrupt, position, recordLen)
	}
	if flags&archiveRecordFlagPadding == 0 && recordLen != archiveHeaderLen+payloadLen {
		return archiveEntry{}, fmt.Errorf("%w at position %d: invalid record length %d", ErrArchiveCorrupt, position, recordLen)
	}
	if flags&archiveRecordFlagPadding != 0 && payloadLen != 0 {
		return archiveEntry{}, fmt.Errorf("%w at position %d: invalid padding payload length %d", ErrArchiveCorrupt, position, payloadLen)
	}

	body := make([]byte, recordLen-archiveHeaderLen)
	if len(body) > 0 {
		if _, err := reader.ReadAt(body, segmentOffset+archiveHeaderLen); err != nil {
			return archiveEntry{}, fmt.Errorf("read archive record body: %w", err)
		}
	}
	packet := make([]byte, archiveHeaderLen+len(body))
	copy(packet, header)
	copy(packet[archiveHeaderLen:], body)
	wantCRC := frameByteOrder.Uint32(header[60:64])
	if gotCRC := archiveRecordCRC(packet); gotCRC != wantCRC {
		return archiveEntry{}, fmt.Errorf("%w at position %d: checksum mismatch", ErrArchiveCorrupt, position)
	}

	record := ArchiveRecord{
		RecordingID:   recordingID,
		Position:      position,
		NextPosition:  position + recordLen,
		SegmentBase:   segmentBase,
		RecordedAt:    time.Unix(0, int64(frameByteOrder.Uint64(header[48:56]))).UTC(),
		PayloadLength: int(payloadLen),
	}
	if flags&archiveRecordFlagPadding != 0 {
		return archiveEntry{record: record, padding: true}, nil
	}
	if flags&archiveRecordFlagRawFrame != 0 {
		frames, err := decodeFrames(body)
		if err != nil {
			return archiveEntry{}, fmt.Errorf("%w at position %d: invalid raw frame: %w", ErrArchiveCorrupt, position, err)
		}
		if len(frames) != 1 {
			return archiveEntry{}, fmt.Errorf("%w at position %d: raw archive record contains %d frames", ErrArchiveCorrupt, position, len(frames))
		}
		f := frames[0]
		record.Message = archiveMessageFromFrameHeader(f)
		record.RawFrame = cloneBytes(body)
		return archiveEntry{record: record, rawFrame: true, frame: f}, nil
	}
	record.Message = Message{
		StreamID:      frameByteOrder.Uint32(header[16:20]),
		SessionID:     frameByteOrder.Uint32(header[20:24]),
		TermID:        int32(frameByteOrder.Uint32(header[24:28])),
		TermOffset:    int32(frameByteOrder.Uint32(header[28:32])),
		Sequence:      frameByteOrder.Uint64(header[32:40]),
		ReservedValue: frameByteOrder.Uint64(header[40:48]),
		Payload:       append([]byte(nil), body[:payloadLen]...),
	}
	return archiveEntry{record: record}, nil
}

func archiveRecordCRC(packet []byte) uint32 {
	crc := crc32.NewIEEE()
	_, _ = crc.Write(packet[:60])
	if len(packet) > archiveHeaderLen {
		_, _ = crc.Write(packet[archiveHeaderLen:])
	}
	return crc.Sum32()
}

func archiveReplayMatch(cfg ArchiveReplayConfig, msg Message) bool {
	if cfg.StreamID != 0 && cfg.StreamID != msg.StreamID {
		return false
	}
	if cfg.SessionID != 0 && cfg.SessionID != msg.SessionID {
		return false
	}
	return true
}

func archiveSegmentBase(position, segmentLength int64) int64 {
	return position / segmentLength * segmentLength
}

func cloneMessage(msg Message) Message {
	msg.Payload = append([]byte(nil), msg.Payload...)
	return msg
}
