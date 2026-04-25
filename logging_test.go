package bunshin

import (
	"context"
	"testing"
	"time"
)

func TestLoggerFuncReceivesTimestampedEvent(t *testing.T) {
	events := make(chan LogEvent, 1)
	logger := LoggerFunc(func(_ context.Context, event LogEvent) {
		events <- event
	})

	logEvent(context.Background(), logger, LogEvent{
		Level:     LogLevelInfo,
		Component: "test",
		Operation: "unit",
		Message:   "event",
	})

	select {
	case event := <-events:
		if event.Time.IsZero() || event.Level != LogLevelInfo || event.Component != "test" || event.Operation != "unit" {
			t.Fatalf("unexpected event: %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("logger did not receive event")
	}
}

func TestPublicationSubscriptionStructuredLogging(t *testing.T) {
	events := make(chan LogEvent, 32)
	logger := LoggerFunc(func(_ context.Context, event LogEvent) {
		events <- event
	})

	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  110,
		LocalAddr: "127.0.0.1:0",
		Logger:    logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		_ = sub.Serve(ctx, func(context.Context, Message) error {
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:   110,
		SessionID:  211,
		RemoteAddr: sub.LocalAddr().String(),
		Logger:     logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Send(ctx, []byte("payload")); err != nil {
		t.Fatal(err)
	}

	seen := waitForLogEvents(t, events,
		func(event LogEvent) bool {
			return event.Component == "subscription" && event.Operation == "listen" && event.Level == LogLevelInfo
		},
		func(event LogEvent) bool {
			return event.Component == "publication" && event.Operation == "dial" && event.Level == LogLevelInfo
		},
		func(event LogEvent) bool {
			return event.Component == "publication" && event.Operation == "send" &&
				event.Message == "message sent" && event.Fields["sequence"] == uint64(1) && event.Fields["bytes"] == len("payload")
		},
		func(event LogEvent) bool {
			return event.Component == "subscription" && event.Operation == "deliver" &&
				event.Message == "message delivered" && event.Fields["sequence"] == uint64(1) && event.Fields["bytes"] == len("payload")
		},
	)
	for _, event := range seen {
		if event.Time.IsZero() {
			t.Fatalf("event time was not set: %#v", event)
		}
	}
}

func waitForLogEvents(t *testing.T, events <-chan LogEvent, predicates ...func(LogEvent) bool) []LogEvent {
	t.Helper()

	found := make([]bool, len(predicates))
	var seen []LogEvent
	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	for {
		allFound := true
		for _, ok := range found {
			if !ok {
				allFound = false
				break
			}
		}
		if allFound {
			return seen
		}

		select {
		case event := <-events:
			seen = append(seen, event)
			for i, predicate := range predicates {
				if !found[i] && predicate(event) {
					found[i] = true
				}
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for log events; seen=%#v found=%#v", seen, found)
		}
	}
}
