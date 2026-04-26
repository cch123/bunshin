package bunshin

import (
	"context"
	"errors"
)

var ErrControlledPollAbort = errors.New("bunshin: controlled poll abort")

type ControlledPollAction int

const (
	ControlledPollContinue ControlledPollAction = iota
	ControlledPollBreak
	ControlledPollAbort
	ControlledPollCommit
)

type ControlledHandler func(context.Context, Message) ControlledPollAction

func (s *Subscription) ControlledPoll(ctx context.Context, handler ControlledHandler) (int, error) {
	return s.ControlledPollN(ctx, 1, handler)
}

func (s *Subscription) ControlledPollN(ctx context.Context, fragmentLimit int, handler ControlledHandler) (int, error) {
	if handler == nil {
		return 0, errors.New("controlled handler is required")
	}
	state := controlledPollState{}
	n, err := s.poll(ctx, fragmentLimit, fragmentLimit, state.wrap(handler), state.stop)
	if errors.Is(err, ErrControlledPollAbort) {
		return n, err
	}
	return n, err
}

type controlledPollState struct {
	stopPolling bool
}

func (s *controlledPollState) stop() bool {
	return s != nil && s.stopPolling
}

func (s *controlledPollState) wrap(handler ControlledHandler) Handler {
	return func(ctx context.Context, msg Message) error {
		switch action := handler(ctx, msg); action {
		case ControlledPollContinue:
			return nil
		case ControlledPollBreak:
			s.stopPolling = true
			return nil
		case ControlledPollAbort:
			s.stopPolling = true
			return ErrControlledPollAbort
		case ControlledPollCommit:
			return nil
		default:
			s.stopPolling = true
			return invalidConfigf("invalid controlled poll action: %d", action)
		}
	}
}
