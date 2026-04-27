package core

import (
	"context"
	"errors"
	"sort"
	"time"
)

const defaultClusterTimerCheckInterval = 10 * time.Millisecond

var ErrClusterTimerNotFound = errors.New("bunshin cluster timer: not found")

type ClusterTimerID uint64

type ClusterTimer struct {
	TimerID  ClusterTimerID `json:"timer_id"`
	Deadline time.Time      `json:"deadline"`
	Payload  []byte         `json:"payload,omitempty"`
}

type ClusterServiceMessage struct {
	CorrelationID ClusterCorrelationID
	SourceService string
	TargetService string
	Payload       []byte
}

func (n *ClusterNode) ScheduleTimer(ctx context.Context, timer ClusterTimer) (ClusterTimer, error) {
	if n == nil {
		return ClusterTimer{}, ErrClusterClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if timer.Deadline.IsZero() {
		return ClusterTimer{}, invalidConfigf("cluster timer deadline is required")
	}
	timer.Deadline = timer.Deadline.UTC()

	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return ClusterTimer{}, ErrClusterClosed
	}
	if n.suspended {
		n.mu.Unlock()
		return ClusterTimer{}, ErrClusterSuspended
	}
	if n.role != ClusterRoleLeader {
		n.mu.Unlock()
		return ClusterTimer{}, ErrClusterNotLeader
	}
	if timer.TimerID == 0 {
		timer.TimerID = n.nextTimerID
		n.nextTimerID++
	} else if timer.TimerID >= n.nextTimerID {
		n.nextTimerID = timer.TimerID + 1
	}
	nodeID := n.nodeID
	role := n.role
	n.mu.Unlock()

	if err := n.authorizeClusterAction(ctx, ClusterAuthorizationRequest{
		NodeID:    nodeID,
		Role:      role,
		Action:    ClusterAuthorizationActionScheduleTimer,
		TimerID:   timer.TimerID,
		Deadline:  timer.Deadline,
		Payload:   timer.Payload,
		Principal: clusterPrincipalFromContext(ctx),
	}); err != nil {
		return ClusterTimer{}, err
	}

	entry, err := n.appendClusterEntry(ctx, ClusterLogEntry{
		Type:     ClusterLogEntryTimerSchedule,
		TimerID:  timer.TimerID,
		Deadline: timer.Deadline,
		Payload:  cloneBytes(timer.Payload),
	})
	if err != nil {
		return ClusterTimer{}, err
	}
	if _, err := n.applyEntry(ctx, nodeID, role, entry, false); err != nil {
		return ClusterTimer{}, err
	}
	return cloneClusterTimer(timer), nil
}

func (n *ClusterNode) CancelTimer(ctx context.Context, timerID ClusterTimerID) error {
	if n == nil {
		return ErrClusterClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return ErrClusterClosed
	}
	if n.suspended {
		n.mu.Unlock()
		return ErrClusterSuspended
	}
	if n.role != ClusterRoleLeader {
		n.mu.Unlock()
		return ErrClusterNotLeader
	}
	if _, ok := n.timers[timerID]; !ok {
		n.mu.Unlock()
		return ErrClusterTimerNotFound
	}
	nodeID := n.nodeID
	role := n.role
	timer := cloneClusterTimer(n.timers[timerID])
	n.mu.Unlock()

	if err := n.authorizeClusterAction(ctx, ClusterAuthorizationRequest{
		NodeID:    nodeID,
		Role:      role,
		Action:    ClusterAuthorizationActionCancelTimer,
		TimerID:   timerID,
		Deadline:  timer.Deadline,
		Payload:   timer.Payload,
		Principal: clusterPrincipalFromContext(ctx),
	}); err != nil {
		return err
	}

	entry, err := n.appendClusterEntry(ctx, ClusterLogEntry{
		Type:    ClusterLogEntryTimerCancel,
		TimerID: timerID,
	})
	if err != nil {
		return err
	}
	_, err = n.applyEntry(ctx, nodeID, role, entry, false)
	return err
}

func (n *ClusterNode) FireDueTimers(ctx context.Context, now time.Time) ([]ClusterEgress, error) {
	if n == nil {
		return nil, ErrClusterClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}

	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil, ErrClusterClosed
	}
	if n.suspended {
		n.mu.Unlock()
		return nil, ErrClusterSuspended
	}
	if n.role != ClusterRoleLeader {
		n.mu.Unlock()
		return nil, ErrClusterNotLeader
	}
	nodeID := n.nodeID
	role := n.role
	due := n.dueTimersLocked(now)
	n.mu.Unlock()

	if len(due) == 0 {
		return nil, nil
	}
	egress := make([]ClusterEgress, 0, len(due))
	for _, timer := range due {
		entry, err := n.appendClusterEntry(ctx, ClusterLogEntry{
			Type:     ClusterLogEntryTimerFire,
			TimerID:  timer.TimerID,
			Deadline: timer.Deadline,
			Payload:  cloneBytes(timer.Payload),
		})
		if err != nil {
			return egress, err
		}
		delivered, err := n.applyEntry(ctx, nodeID, role, entry, false)
		if err != nil {
			return egress, err
		}
		egress = append(egress, delivered)
	}
	return egress, nil
}

func (n *ClusterNode) SendServiceMessage(ctx context.Context, msg ClusterServiceMessage) (ClusterEgress, error) {
	if n == nil {
		return ClusterEgress{}, ErrClusterClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return ClusterEgress{}, ErrClusterClosed
	}
	if n.suspended {
		n.mu.Unlock()
		return ClusterEgress{}, ErrClusterSuspended
	}
	if n.role != ClusterRoleLeader {
		n.mu.Unlock()
		return ClusterEgress{}, ErrClusterNotLeader
	}
	if msg.CorrelationID == 0 {
		msg.CorrelationID = n.nextCorrelationID
		n.nextCorrelationID++
	}
	nodeID := n.nodeID
	role := n.role
	n.mu.Unlock()

	if err := n.authorizeClusterAction(ctx, ClusterAuthorizationRequest{
		NodeID:        nodeID,
		Role:          role,
		Action:        ClusterAuthorizationActionServiceMessage,
		CorrelationID: msg.CorrelationID,
		SourceService: msg.SourceService,
		TargetService: msg.TargetService,
		Payload:       msg.Payload,
		Principal:     clusterPrincipalFromContext(ctx),
	}); err != nil {
		return ClusterEgress{}, err
	}

	entry, err := n.appendClusterEntry(ctx, ClusterLogEntry{
		Type:          ClusterLogEntryServiceMessage,
		CorrelationID: msg.CorrelationID,
		SourceService: msg.SourceService,
		TargetService: msg.TargetService,
		Payload:       cloneBytes(msg.Payload),
	})
	if err != nil {
		return ClusterEgress{}, err
	}
	return n.applyEntry(ctx, nodeID, role, entry, false)
}

func (n *ClusterNode) startTimerLoop() {
	n.mu.Lock()
	if n.disableTimerLoop || n.role != ClusterRoleLeader || n.timerCancel != nil {
		n.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	interval := n.timerCheckInterval
	n.timerCancel = cancel
	n.timerDone = done
	n.mu.Unlock()

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				if _, err := n.FireDueTimers(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
					continue
				}
			}
		}
	}()
}

func (n *ClusterNode) stopTimerLoop() {
	n.mu.Lock()
	cancel := n.timerCancel
	done := n.timerDone
	n.timerCancel = nil
	n.timerDone = nil
	n.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (n *ClusterNode) applyTimerSchedule(entry ClusterLogEntry) {
	timer := ClusterTimer{
		TimerID:  entry.TimerID,
		Deadline: entry.Deadline.UTC(),
		Payload:  cloneBytes(entry.Payload),
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.ensureTimerMapLocked()
	n.timers[timer.TimerID] = timer
	if timer.TimerID >= n.nextTimerID {
		n.nextTimerID = timer.TimerID + 1
	}
}

func (n *ClusterNode) applyTimerCancel(timerID ClusterTimerID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.timers, timerID)
}

func (n *ClusterNode) applyTimerFire(timerID ClusterTimerID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.timers, timerID)
}

func (n *ClusterNode) dueTimersLocked(now time.Time) []ClusterTimer {
	if len(n.timers) == 0 {
		return nil
	}
	var due []ClusterTimer
	for _, timer := range n.timers {
		if !timer.Deadline.After(now) {
			due = append(due, cloneClusterTimer(timer))
		}
	}
	sortClusterTimers(due)
	return due
}

func (n *ClusterNode) snapshotTimers() []ClusterTimer {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.timers) == 0 {
		return nil
	}
	timers := make([]ClusterTimer, 0, len(n.timers))
	for _, timer := range n.timers {
		timers = append(timers, cloneClusterTimer(timer))
	}
	sortClusterTimers(timers)
	return timers
}

func (n *ClusterNode) restoreTimers(timers []ClusterTimer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.timers = make(map[ClusterTimerID]ClusterTimer, len(timers))
	for _, timer := range timers {
		timer = cloneClusterTimer(timer)
		timer.Deadline = timer.Deadline.UTC()
		n.timers[timer.TimerID] = timer
		if timer.TimerID >= n.nextTimerID {
			n.nextTimerID = timer.TimerID + 1
		}
	}
}

func (n *ClusterNode) ensureTimerMapLocked() {
	if n.timers == nil {
		n.timers = make(map[ClusterTimerID]ClusterTimer)
	}
}

func cloneClusterTimer(timer ClusterTimer) ClusterTimer {
	timer.Payload = cloneBytes(timer.Payload)
	return timer
}

func cloneClusterTimers(timers []ClusterTimer) []ClusterTimer {
	if len(timers) == 0 {
		return nil
	}
	cloned := make([]ClusterTimer, len(timers))
	for i, timer := range timers {
		cloned[i] = cloneClusterTimer(timer)
	}
	return cloned
}

func sortClusterTimers(timers []ClusterTimer) {
	sort.Slice(timers, func(i, j int) bool {
		if timers[i].Deadline.Equal(timers[j].Deadline) {
			return timers[i].TimerID < timers[j].TimerID
		}
		return timers[i].Deadline.Before(timers[j].Deadline)
	})
}
