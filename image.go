package bunshin

import (
	"context"
	"sync"
	"time"
)

type ImageHandler func(context.Context, *Image)

type Image struct {
	StreamID         uint32
	SessionID        uint32
	Source           string
	InitialTermID    int32
	TermBufferLength int
	JoinPosition     int64
	AvailableAt      time.Time

	mu                   sync.RWMutex
	currentPosition      int64
	observedPosition     int64
	lastSequence         uint64
	lastObservedSequence uint64
	unavailableAt        time.Time
}

type ImageSnapshot struct {
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

type SubscriptionLagReport struct {
	StreamID             uint32
	SessionID            uint32
	Source               string
	CurrentPosition      int64
	ObservedPosition     int64
	LagBytes             int64
	LastSequence         uint64
	LastObservedSequence uint64
	AvailableAt          time.Time
	UnavailableAt        time.Time
}

func (i *Image) CurrentPosition() int64 {
	if i == nil {
		return 0
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.currentPosition
}

func (i *Image) LastSequence() uint64 {
	if i == nil {
		return 0
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.lastSequence
}

func (i *Image) ObservedPosition() int64 {
	if i == nil {
		return 0
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.observedPosition
}

func (i *Image) LagBytes() int64 {
	if i == nil {
		return 0
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	return imageLag(i.currentPosition, i.observedPosition)
}

func (i *Image) UnavailableAt() time.Time {
	if i == nil {
		return time.Time{}
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.unavailableAt
}

func (i *Image) Snapshot() ImageSnapshot {
	if i == nil {
		return ImageSnapshot{}
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	return ImageSnapshot{
		StreamID:             i.StreamID,
		SessionID:            i.SessionID,
		Source:               i.Source,
		InitialTermID:        i.InitialTermID,
		TermBufferLength:     i.TermBufferLength,
		JoinPosition:         i.JoinPosition,
		CurrentPosition:      i.currentPosition,
		ObservedPosition:     i.observedPosition,
		LagBytes:             imageLag(i.currentPosition, i.observedPosition),
		LastSequence:         i.lastSequence,
		LastObservedSequence: i.lastObservedSequence,
		AvailableAt:          i.AvailableAt,
		UnavailableAt:        i.unavailableAt,
	}
}

func (i *Image) observe(position int64, sequence uint64) {
	if i == nil {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if position > i.observedPosition {
		i.observedPosition = position
	}
	if sequence > i.lastObservedSequence {
		i.lastObservedSequence = sequence
	}
}

func (i *Image) update(position int64, sequence uint64) {
	if i == nil {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if position > i.currentPosition {
		i.currentPosition = position
	}
	if sequence > i.lastSequence {
		i.lastSequence = sequence
	}
}

func (i *Image) markUnavailable(now time.Time) bool {
	if i == nil {
		return false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.unavailableAt.IsZero() {
		return false
	}
	i.unavailableAt = now
	return true
}

func (s *Subscription) imageForMessage(ctx context.Context, msg Message) *Image {
	if s == nil {
		return nil
	}
	key := imageKeyForMessage(msg)
	joinPosition := s.imageJoinPosition(msg)
	now := time.Now()

	s.imagesMu.Lock()
	if s.images == nil {
		s.images = make(map[lossKey]*Image)
	}
	created := false
	image := s.images[key]
	if image == nil {
		image = &Image{
			StreamID:         msg.StreamID,
			SessionID:        msg.SessionID,
			Source:           remoteAddrString(msg.Remote),
			InitialTermID:    msg.TermID,
			TermBufferLength: s.imageTermLength,
			JoinPosition:     joinPosition,
			AvailableAt:      now,
			currentPosition:  joinPosition,
			observedPosition: joinPosition,
		}
		s.images[key] = image
		created = true
	}
	available := s.availableImage
	s.imagesMu.Unlock()

	if available != nil && created {
		available(ctx, image)
	}
	return image
}

func (s *Subscription) Images() []ImageSnapshot {
	if s == nil {
		return nil
	}
	s.imagesMu.Lock()
	defer s.imagesMu.Unlock()
	images := make([]ImageSnapshot, 0, len(s.images))
	for _, image := range s.images {
		images = append(images, image.Snapshot())
	}
	return images
}

func (s *Subscription) LagReports() []SubscriptionLagReport {
	if s == nil {
		return nil
	}
	s.imagesMu.Lock()
	defer s.imagesMu.Unlock()
	reports := make([]SubscriptionLagReport, 0, len(s.images))
	for _, image := range s.images {
		snapshot := image.Snapshot()
		reports = append(reports, SubscriptionLagReport{
			StreamID:             snapshot.StreamID,
			SessionID:            snapshot.SessionID,
			Source:               snapshot.Source,
			CurrentPosition:      snapshot.CurrentPosition,
			ObservedPosition:     snapshot.ObservedPosition,
			LagBytes:             snapshot.LagBytes,
			LastSequence:         snapshot.LastSequence,
			LastObservedSequence: snapshot.LastObservedSequence,
			AvailableAt:          snapshot.AvailableAt,
			UnavailableAt:        snapshot.UnavailableAt,
		})
	}
	return reports
}

func (s *Subscription) closeImages(ctx context.Context) {
	if s == nil {
		return
	}
	now := time.Now()
	s.imagesMu.Lock()
	images := make([]*Image, 0, len(s.images))
	for _, image := range s.images {
		images = append(images, image)
	}
	unavailable := s.unavailableImage
	s.imagesMu.Unlock()

	for _, image := range images {
		if image.markUnavailable(now) && unavailable != nil {
			unavailable(ctx, image)
		}
	}
}

func imageKeyForMessage(msg Message) lossKey {
	return lossKey{
		streamID:  msg.StreamID,
		sessionID: msg.SessionID,
		source:    remoteAddrString(msg.Remote),
	}
}

func (s *Subscription) imageJoinPosition(msg Message) int64 {
	if s == nil || msg.TermOffset < 0 {
		return 0
	}
	return computeTermPosition(msg.TermID, int(msg.TermOffset), s.imagePositionBitsToShift, msg.TermID)
}

func framePosition(f frame) (int32, int, bool) {
	if f.termOffset < 0 {
		return 0, 0, false
	}
	termOffset := int(f.termOffset) + align(headerLen+len(f.payload), termFrameAlignment)
	return f.termID, termOffset, true
}

func imageLag(currentPosition, observedPosition int64) int64 {
	if observedPosition <= currentPosition {
		return 0
	}
	return observedPosition - currentPosition
}
