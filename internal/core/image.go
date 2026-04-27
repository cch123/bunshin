package core

import (
	"context"
	"fmt"
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
	rebuild              receiverImageRebuild
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
	RebuildMessages      int
	RebuildFrames        int
	RebuildBytes         int
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
	RebuildMessages      int
	RebuildFrames        int
	RebuildBytes         int
	AvailableAt          time.Time
	UnavailableAt        time.Time
}

type receiverRebuildFrameKey struct {
	termID     int32
	termOffset int32
}

type receiverRebuildMessage struct {
	frames []receiverRebuildFrameKey
	bytes  int
}

type receiverRebuildEncodedFrame struct {
	key           receiverRebuildFrameKey
	encoded       []byte
	alignedLength int
}

type receiverImageRebuild struct {
	terms            map[int32][]byte
	termFrameCounts  map[int32]int
	frames           map[receiverRebuildFrameKey]uint64
	messages         map[uint64]receiverRebuildMessage
	bufferedMessages int
	bufferedFrames   int
	bufferedBytes    int
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
		RebuildMessages:      i.rebuild.bufferedMessages,
		RebuildFrames:        i.rebuild.bufferedFrames,
		RebuildBytes:         i.rebuild.bufferedBytes,
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

func (i *Image) bufferRebuildFrames(frames []frame, sequence uint64) error {
	if i == nil || len(frames) == 0 || sequence == 0 {
		return nil
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	if sequence <= i.lastSequence+1 {
		return nil
	}
	if i.rebuild.messages != nil {
		if _, ok := i.rebuild.messages[sequence]; ok {
			return nil
		}
	}

	message := receiverRebuildMessage{
		frames: make([]receiverRebuildFrameKey, 0, len(frames)),
	}
	encodedFrames := make([]receiverRebuildEncodedFrame, 0, len(frames))
	for _, f := range frames {
		if f.typ != frameData {
			return fmt.Errorf("non-data frame in receiver rebuild: %d", f.typ)
		}
		if f.seq != sequence {
			return fmt.Errorf("receiver rebuild sequence mismatch: got %d, want %d", f.seq, sequence)
		}
		if f.termOffset < 0 {
			return fmt.Errorf("invalid receiver rebuild term offset: %d", f.termOffset)
		}
		encoded, err := encodeFrame(f)
		if err != nil {
			return err
		}
		alignedLength := align(len(encoded), termFrameAlignment)
		termOffset := int(f.termOffset)
		if termOffset+alignedLength > i.TermBufferLength {
			return fmt.Errorf("receiver rebuild frame exceeds term: offset=%d length=%d term_length=%d", termOffset, alignedLength, i.TermBufferLength)
		}
		key := receiverRebuildFrameKey{termID: f.termID, termOffset: f.termOffset}
		if existing, ok := i.rebuild.frames[key]; ok {
			if existing == sequence {
				continue
			}
			return fmt.Errorf("receiver rebuild term slot already occupied: term_id=%d term_offset=%d", f.termID, f.termOffset)
		}
		encodedFrames = append(encodedFrames, receiverRebuildEncodedFrame{
			key:           key,
			encoded:       encoded,
			alignedLength: alignedLength,
		})
		message.frames = append(message.frames, key)
		message.bytes += alignedLength
	}
	if len(message.frames) == 0 {
		return nil
	}
	i.initRebuildLocked()
	for _, encodedFrame := range encodedFrames {
		term := i.rebuild.terms[encodedFrame.key.termID]
		if term == nil {
			term = make([]byte, i.TermBufferLength)
			i.rebuild.terms[encodedFrame.key.termID] = term
		}
		termOffset := int(encodedFrame.key.termOffset)
		copy(term[termOffset:termOffset+len(encodedFrame.encoded)], encodedFrame.encoded)
		clear(term[termOffset+len(encodedFrame.encoded) : termOffset+encodedFrame.alignedLength])
		i.rebuild.frames[encodedFrame.key] = sequence
		i.rebuild.termFrameCounts[encodedFrame.key.termID]++
	}
	i.rebuild.messages[sequence] = message
	i.rebuild.bufferedMessages++
	i.rebuild.bufferedFrames += len(message.frames)
	i.rebuild.bufferedBytes += message.bytes
	return nil
}

func (i *Image) initRebuildLocked() {
	if i.rebuild.terms == nil {
		i.rebuild.terms = make(map[int32][]byte)
	}
	if i.rebuild.termFrameCounts == nil {
		i.rebuild.termFrameCounts = make(map[int32]int)
	}
	if i.rebuild.frames == nil {
		i.rebuild.frames = make(map[receiverRebuildFrameKey]uint64)
	}
	if i.rebuild.messages == nil {
		i.rebuild.messages = make(map[uint64]receiverRebuildMessage)
	}
}

func (i *Image) completeRebuild(sequence uint64) {
	if i == nil || sequence == 0 {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	message, ok := i.rebuild.messages[sequence]
	if !ok {
		return
	}
	delete(i.rebuild.messages, sequence)
	i.rebuild.bufferedMessages--
	i.rebuild.bufferedFrames -= len(message.frames)
	i.rebuild.bufferedBytes -= message.bytes
	for _, key := range message.frames {
		delete(i.rebuild.frames, key)
		i.rebuild.termFrameCounts[key.termID]--
		if i.rebuild.termFrameCounts[key.termID] <= 0 {
			delete(i.rebuild.termFrameCounts, key.termID)
			delete(i.rebuild.terms, key.termID)
		}
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
			RebuildMessages:      snapshot.RebuildMessages,
			RebuildFrames:        snapshot.RebuildFrames,
			RebuildBytes:         snapshot.RebuildBytes,
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
