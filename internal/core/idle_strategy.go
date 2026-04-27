package core

import (
	"runtime"
	"time"
)

const (
	defaultBackoffMaxSpins  = 10
	defaultBackoffMaxYields = 20
	defaultBackoffMinSleep  = time.Microsecond
	defaultBackoffMaxSleep  = time.Millisecond
)

// IdleStrategy controls how a polling loop waits after a unit of work.
type IdleStrategy interface {
	Idle(workCount int)
	Reset()
}

// NoOpIdleStrategy never waits. It is useful when the caller owns blocking.
type NoOpIdleStrategy struct{}

func (NoOpIdleStrategy) Idle(int) {}

func (NoOpIdleStrategy) Reset() {}

// BusySpinIdleStrategy keeps the loop hot by returning immediately.
type BusySpinIdleStrategy struct{}

func (BusySpinIdleStrategy) Idle(int) {}

func (BusySpinIdleStrategy) Reset() {}

// YieldingIdleStrategy yields the processor when no work was done.
type YieldingIdleStrategy struct{}

func (YieldingIdleStrategy) Idle(workCount int) {
	if workCount > 0 {
		return
	}
	runtime.Gosched()
}

func (YieldingIdleStrategy) Reset() {}

// SleepingIdleStrategy sleeps for Duration when no work was done.
type SleepingIdleStrategy struct {
	Duration time.Duration
}

func (s SleepingIdleStrategy) Idle(workCount int) {
	if workCount > 0 || s.Duration <= 0 {
		return
	}
	time.Sleep(s.Duration)
}

func (SleepingIdleStrategy) Reset() {}

// BackoffIdleStrategy progresses from spin, to yield, to capped sleep.
type BackoffIdleStrategy struct {
	MaxSpins  int
	MaxYields int
	MinSleep  time.Duration
	MaxSleep  time.Duration

	spins  int
	yields int
	sleep  time.Duration
}

func NewDefaultBackoffIdleStrategy() *BackoffIdleStrategy {
	return NewBackoffIdleStrategy(
		defaultBackoffMaxSpins,
		defaultBackoffMaxYields,
		defaultBackoffMinSleep,
		defaultBackoffMaxSleep,
	)
}

func NewBackoffIdleStrategy(maxSpins, maxYields int, minSleep, maxSleep time.Duration) *BackoffIdleStrategy {
	strategy := &BackoffIdleStrategy{
		MaxSpins:  maxSpins,
		MaxYields: maxYields,
		MinSleep:  minSleep,
		MaxSleep:  maxSleep,
	}
	strategy.Reset()
	return strategy
}

func (s *BackoffIdleStrategy) Idle(workCount int) {
	if workCount > 0 {
		s.Reset()
		return
	}

	s.normalize()
	if s.spins < s.MaxSpins {
		s.spins++
		return
	}
	if s.yields < s.MaxYields {
		s.yields++
		runtime.Gosched()
		return
	}
	if s.sleep <= 0 {
		return
	}

	time.Sleep(s.sleep)
	s.sleep = nextBackoffSleep(s.sleep, s.MaxSleep)
}

func (s *BackoffIdleStrategy) Reset() {
	s.normalize()
	s.spins = 0
	s.yields = 0
	s.sleep = s.MinSleep
}

func (s *BackoffIdleStrategy) normalize() {
	if s.MaxSpins < 0 {
		s.MaxSpins = 0
	}
	if s.MaxYields < 0 {
		s.MaxYields = 0
	}
	if s.MinSleep < 0 {
		s.MinSleep = 0
	}
	if s.MaxSleep < s.MinSleep {
		s.MaxSleep = s.MinSleep
	}
	if s.sleep < s.MinSleep {
		s.sleep = s.MinSleep
	}
	if s.MaxSleep > 0 && s.sleep > s.MaxSleep {
		s.sleep = s.MaxSleep
	}
}

func nextBackoffSleep(current, maxSleep time.Duration) time.Duration {
	next := current * 2
	if next <= 0 || next > maxSleep {
		return maxSleep
	}
	return next
}
