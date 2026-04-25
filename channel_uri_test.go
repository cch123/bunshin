package bunshin

import (
	"errors"
	"testing"
)

func TestParseChannelURIUDP(t *testing.T) {
	ch, err := ParseChannelURI("bunshin:udp?endpoint=127.0.0.1%3A40456&alias=main")
	if err != nil {
		t.Fatal(err)
	}
	if ch.Transport != TransportUDP || ch.Endpoint != "127.0.0.1:40456" || ch.Params["alias"] != "main" {
		t.Fatalf("unexpected channel: %#v", ch)
	}
	if got := ch.String(); got != "bunshin:udp?alias=main&endpoint=127.0.0.1%3A40456" {
		t.Fatalf("channel string = %q", got)
	}
}

func TestParseChannelURIIPC(t *testing.T) {
	ch, err := ParseChannelURI("bunshin:ipc?path=%2Ftmp%2Fbunshin.ring")
	if err != nil {
		t.Fatal(err)
	}
	if ch.Transport != TransportIPC || ch.Path != "/tmp/bunshin.ring" {
		t.Fatalf("unexpected channel: %#v", ch)
	}
}

func TestChannelURIConfigHelpers(t *testing.T) {
	ch := ChannelURI{Transport: TransportQUIC, Endpoint: "127.0.0.1:40456"}
	pub := ch.PublicationConfig(PublicationConfig{StreamID: 7})
	if pub.Transport != TransportQUIC || pub.RemoteAddr != "127.0.0.1:40456" || pub.StreamID != 7 {
		t.Fatalf("unexpected publication config: %#v", pub)
	}
	sub := ch.SubscriptionConfig(SubscriptionConfig{StreamID: 8})
	if sub.Transport != TransportQUIC || sub.LocalAddr != "127.0.0.1:40456" || sub.StreamID != 8 {
		t.Fatalf("unexpected subscription config: %#v", sub)
	}
}

func TestParseChannelURIRejectsInvalidValues(t *testing.T) {
	tests := []string{
		"udp?endpoint=127.0.0.1:1",
		"bunshin:",
		"bunshin:bogus?endpoint=127.0.0.1:1",
		"bunshin:udp",
		"bunshin:ipc",
	}
	for _, raw := range tests {
		t.Run(raw, func(t *testing.T) {
			if _, err := ParseChannelURI(raw); !errors.Is(err, ErrInvalidChannelURI) {
				t.Fatalf("ParseChannelURI(%q) err = %v, want %v", raw, err, ErrInvalidChannelURI)
			}
		})
	}
}
