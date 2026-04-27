package core

import (
	"errors"
	"testing"
	"time"
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

func TestParseChannelURIUDPDestinations(t *testing.T) {
	ch, err := ParseChannelURI("bunshin:udp?endpoint=127.0.0.1%3A40456&destination=127.0.0.1%3A40457&destination=127.0.0.1%3A40458")
	if err != nil {
		t.Fatal(err)
	}
	if got := ch.DestinationList(); len(got) != 2 || got[0] != "127.0.0.1:40457" || got[1] != "127.0.0.1:40458" {
		t.Fatalf("destinations = %#v", got)
	}
	if got := ch.String(); got != "bunshin:udp?destination=127.0.0.1%3A40457&destination=127.0.0.1%3A40458&endpoint=127.0.0.1%3A40456" {
		t.Fatalf("channel string = %q", got)
	}
	ch.AddDestination("127.0.0.1:40457")
	ch.AddDestination("127.0.0.1:40459")
	ch.RemoveDestination("127.0.0.1:40458")
	if got := ch.DestinationList(); len(got) != 2 || got[0] != "127.0.0.1:40457" || got[1] != "127.0.0.1:40459" {
		t.Fatalf("updated destinations = %#v", got)
	}
}

func TestParseChannelURILocalSpy(t *testing.T) {
	ch, err := ParseChannelURI("bunshin:udp?endpoint=127.0.0.1%3A40456&spy=true")
	if err != nil {
		t.Fatal(err)
	}
	if !ch.Spy {
		t.Fatalf("spy = %v, want true", ch.Spy)
	}
	if got := ch.String(); got != "bunshin:udp?endpoint=127.0.0.1%3A40456&spy=true" {
		t.Fatalf("channel string = %q", got)
	}
	sub := ch.SubscriptionConfig(SubscriptionConfig{StreamID: 7})
	if !sub.LocalSpy || sub.Transport != TransportUDP || sub.LocalAddr != "127.0.0.1:40456" || sub.StreamID != 7 {
		t.Fatalf("unexpected spy subscription config: %#v", sub)
	}
}

func TestParseChannelURIUDPNameResolutionInterval(t *testing.T) {
	ch, err := ParseChannelURI("bunshin:udp?endpoint=127.0.0.1%3A40456&name-resolution-interval=10ms")
	if err != nil {
		t.Fatal(err)
	}
	if ch.NameResolutionInterval != 10*time.Millisecond {
		t.Fatalf("name resolution interval = %s", ch.NameResolutionInterval)
	}
	if got := ch.String(); got != "bunshin:udp?endpoint=127.0.0.1%3A40456&name-resolution-interval=10ms" {
		t.Fatalf("channel string = %q", got)
	}
	pub := ch.PublicationConfig(PublicationConfig{StreamID: 8})
	if pub.UDPNameResolutionInterval != 10*time.Millisecond {
		t.Fatalf("publication name resolution interval = %s", pub.UDPNameResolutionInterval)
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

	udp := ChannelURI{Transport: TransportUDP, Endpoint: "127.0.0.1:40456", Destinations: []string{"127.0.0.1:40457"}}
	pub = udp.PublicationConfig(PublicationConfig{StreamID: 9, UDPDestinations: []string{"127.0.0.1:40458"}})
	if pub.Transport != TransportUDP || pub.RemoteAddr != "127.0.0.1:40456" || pub.StreamID != 9 ||
		len(pub.UDPDestinations) != 2 || pub.UDPDestinations[0] != "127.0.0.1:40458" || pub.UDPDestinations[1] != "127.0.0.1:40457" {
		t.Fatalf("unexpected udp publication config: %#v", pub)
	}
}

func TestParseChannelURIRejectsInvalidValues(t *testing.T) {
	tests := []string{
		"udp?endpoint=127.0.0.1:1",
		"bunshin:",
		"bunshin:bogus?endpoint=127.0.0.1:1",
		"bunshin:udp",
		"bunshin:ipc",
		"bunshin:quic?endpoint=127.0.0.1%3A1&destination=127.0.0.1%3A2",
		"bunshin:udp?endpoint=127.0.0.1%3A1&destination=",
		"bunshin:ipc?path=%2Ftmp%2Fbunshin.ring&spy=true",
		"bunshin:udp?endpoint=127.0.0.1%3A1&spy=maybe",
		"bunshin:quic?endpoint=127.0.0.1%3A1&name-resolution-interval=10ms",
		"bunshin:udp?endpoint=127.0.0.1%3A1&name-resolution-interval=-1s",
	}
	for _, raw := range tests {
		t.Run(raw, func(t *testing.T) {
			if _, err := ParseChannelURI(raw); !errors.Is(err, ErrInvalidChannelURI) {
				t.Fatalf("ParseChannelURI(%q) err = %v, want %v", raw, err, ErrInvalidChannelURI)
			}
		})
	}
}
