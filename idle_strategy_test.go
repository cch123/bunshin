package bunshin

import (
	"testing"
	"time"
)

func TestBackoffIdleStrategyAdvancesThroughPhases(t *testing.T) {
	strategy := NewBackoffIdleStrategy(2, 1, time.Nanosecond, 2*time.Nanosecond)

	strategy.Idle(0)
	if strategy.spins != 1 || strategy.yields != 0 || strategy.sleep != time.Nanosecond {
		t.Fatalf("after first idle: spins=%d yields=%d sleep=%s", strategy.spins, strategy.yields, strategy.sleep)
	}

	strategy.Idle(0)
	if strategy.spins != 2 || strategy.yields != 0 || strategy.sleep != time.Nanosecond {
		t.Fatalf("after spin phase: spins=%d yields=%d sleep=%s", strategy.spins, strategy.yields, strategy.sleep)
	}

	strategy.Idle(0)
	if strategy.spins != 2 || strategy.yields != 1 || strategy.sleep != time.Nanosecond {
		t.Fatalf("after yield phase: spins=%d yields=%d sleep=%s", strategy.spins, strategy.yields, strategy.sleep)
	}

	strategy.Idle(0)
	if strategy.spins != 2 || strategy.yields != 1 || strategy.sleep != 2*time.Nanosecond {
		t.Fatalf("after sleep phase: spins=%d yields=%d sleep=%s", strategy.spins, strategy.yields, strategy.sleep)
	}

	strategy.Idle(0)
	if strategy.sleep != 2*time.Nanosecond {
		t.Fatalf("sleep exceeded max: %s", strategy.sleep)
	}
}

func TestBackoffIdleStrategyResetsAfterWork(t *testing.T) {
	strategy := NewBackoffIdleStrategy(1, 1, time.Nanosecond, 2*time.Nanosecond)
	strategy.Idle(0)
	strategy.Idle(0)
	strategy.Idle(0)

	strategy.Idle(1)
	if strategy.spins != 0 || strategy.yields != 0 || strategy.sleep != time.Nanosecond {
		t.Fatalf("after reset: spins=%d yields=%d sleep=%s", strategy.spins, strategy.yields, strategy.sleep)
	}
}

func TestBackoffIdleStrategyNormalizesConfiguration(t *testing.T) {
	strategy := NewBackoffIdleStrategy(-1, -1, -time.Nanosecond, -time.Nanosecond)

	if strategy.MaxSpins != 0 || strategy.MaxYields != 0 || strategy.MinSleep != 0 || strategy.MaxSleep != 0 || strategy.sleep != 0 {
		t.Fatalf("unexpected normalized strategy: %#v", strategy)
	}

	strategy.MinSleep = 2 * time.Nanosecond
	strategy.MaxSleep = time.Nanosecond
	strategy.Reset()
	if strategy.MaxSleep != strategy.MinSleep || strategy.sleep != strategy.MinSleep {
		t.Fatalf("max sleep was not clamped to min sleep: %#v", strategy)
	}
}

func TestIdleStrategiesReset(t *testing.T) {
	strategies := []IdleStrategy{
		NoOpIdleStrategy{},
		BusySpinIdleStrategy{},
		YieldingIdleStrategy{},
		SleepingIdleStrategy{},
		NewBackoffIdleStrategy(0, 0, 0, 0),
	}

	for _, strategy := range strategies {
		strategy.Idle(1)
		strategy.Reset()
	}
}
