package bunshin

import (
	"net"
	"sort"
	"sync"
	"time"
)

type LossObservation struct {
	StreamID        uint32
	SessionID       uint32
	Source          string
	FromSequence    uint64
	ToSequence      uint64
	MissingMessages uint64
	ObservedAt      time.Time
}

type LossReport struct {
	StreamID         uint32
	SessionID        uint32
	Source           string
	ObservationCount uint64
	MissingMessages  uint64
	FirstObservation time.Time
	LastObservation  time.Time
}

type LossHandler func(LossObservation)

type lossDetector struct {
	mu      sync.Mutex
	metrics *Metrics
	handler LossHandler
	states  map[lossKey]*lossState
	reports map[lossKey]*LossReport
}

type lossKey struct {
	streamID  uint32
	sessionID uint32
	source    string
}

type lossState struct {
	next          uint64
	reportedUntil uint64
	receivedAhead map[uint64]struct{}
}

func newLossDetector(metrics *Metrics, handler LossHandler) *lossDetector {
	return &lossDetector{
		metrics: metrics,
		handler: handler,
		states:  make(map[lossKey]*lossState),
		reports: make(map[lossKey]*LossReport),
	}
}

func (d *lossDetector) observe(f frame, remote net.Addr) []LossObservation {
	if d == nil || f.seq == 0 {
		return nil
	}

	key := lossKey{
		streamID:  f.streamID,
		sessionID: f.sessionID,
		source:    remoteAddrString(remote),
	}
	now := time.Now()

	observations := d.observeSequence(key, f.seq, now)
	for _, observation := range observations {
		if d.handler != nil {
			d.handler(observation)
		}
	}
	return observations
}

func (d *lossDetector) observeSequence(key lossKey, seq uint64, observedAt time.Time) []LossObservation {
	d.mu.Lock()
	defer d.mu.Unlock()

	state := d.states[key]
	if state == nil {
		state = &lossState{
			next:          1,
			receivedAhead: make(map[uint64]struct{}),
		}
		d.states[key] = state
	}

	switch {
	case seq < state.next:
		return nil
	case seq == state.next:
		state.next++
		for {
			if _, ok := state.receivedAhead[state.next]; !ok {
				break
			}
			delete(state.receivedAhead, state.next)
			state.next++
		}
		return nil
	default:
		if _, ok := state.receivedAhead[seq]; ok {
			return nil
		}
		observations := d.recordMissingRanges(key, state, seq, observedAt)
		state.receivedAhead[seq] = struct{}{}
		if seq-1 > state.reportedUntil {
			state.reportedUntil = seq - 1
		}
		return observations
	}
}

func (d *lossDetector) recordMissingRanges(key lossKey, state *lossState, seq uint64, observedAt time.Time) []LossObservation {
	start := state.reportedUntil + 1
	if start < state.next {
		start = state.next
	}
	end := seq - 1
	if start > end {
		return nil
	}

	ahead := make([]uint64, 0, len(state.receivedAhead))
	for received := range state.receivedAhead {
		if received >= start && received <= end {
			ahead = append(ahead, received)
		}
	}
	sort.Slice(ahead, func(i, j int) bool {
		return ahead[i] < ahead[j]
	})

	var observations []LossObservation
	rangeStart := start
	for _, received := range ahead {
		if received < rangeStart {
			continue
		}
		if received > rangeStart {
			observations = append(observations, d.recordMissingRange(key, rangeStart, received-1, observedAt))
		}
		rangeStart = received + 1
	}
	if rangeStart <= end {
		observations = append(observations, d.recordMissingRange(key, rangeStart, end, observedAt))
	}
	return observations
}

func (d *lossDetector) recordMissingRange(key lossKey, from, to uint64, observedAt time.Time) LossObservation {
	missingMessages := to - from + 1
	report := d.reports[key]
	if report == nil {
		report = &LossReport{
			StreamID:         key.streamID,
			SessionID:        key.sessionID,
			Source:           key.source,
			FirstObservation: observedAt,
		}
		d.reports[key] = report
	}
	report.ObservationCount++
	report.MissingMessages += missingMessages
	report.LastObservation = observedAt
	d.metrics.incLossGap(missingMessages)

	return LossObservation{
		StreamID:        key.streamID,
		SessionID:       key.sessionID,
		Source:          key.source,
		FromSequence:    from,
		ToSequence:      to,
		MissingMessages: missingMessages,
		ObservedAt:      observedAt,
	}
}

func (d *lossDetector) snapshot() []LossReport {
	if d == nil {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	reports := make([]LossReport, 0, len(d.reports))
	for _, report := range d.reports {
		reports = append(reports, *report)
	}
	sort.Slice(reports, func(i, j int) bool {
		if reports[i].StreamID != reports[j].StreamID {
			return reports[i].StreamID < reports[j].StreamID
		}
		if reports[i].SessionID != reports[j].SessionID {
			return reports[i].SessionID < reports[j].SessionID
		}
		return reports[i].Source < reports[j].Source
	})
	return reports
}

func remoteAddrString(remote net.Addr) string {
	if remote == nil {
		return ""
	}
	return remote.String()
}
