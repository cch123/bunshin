package bunshin

import (
	"crypto/tls"
	"errors"
	"testing"
	"time"
)

func TestPublicationConfigNormalizeAppliesDefaults(t *testing.T) {
	cfg, err := normalizePublicationConfig(PublicationConfig{
		RemoteAddr: "127.0.0.1:40456",
	})
	if err != nil {
		t.Fatal(err)
	}

	if cfg.StreamID != defaultStreamID {
		t.Fatalf("stream id = %d, want %d", cfg.StreamID, defaultStreamID)
	}
	if cfg.SessionID == 0 {
		t.Fatal("session id was not defaulted")
	}
	if cfg.MTUBytes != maxFrameSize || cfg.mtuPayload != maxFrameSize-headerLen {
		t.Fatalf("mtu defaults = %d/%d", cfg.MTUBytes, cfg.mtuPayload)
	}
	if cfg.MaxPayloadBytes != cfg.mtuPayload {
		t.Fatalf("max payload = %d, want %d", cfg.MaxPayloadBytes, cfg.mtuPayload)
	}
	if cfg.TermBufferLength != minTermLength {
		t.Fatalf("term buffer length = %d, want %d", cfg.TermBufferLength, minTermLength)
	}
	if cfg.PublicationWindowBytes <= 0 {
		t.Fatalf("publication window was not defaulted: %d", cfg.PublicationWindowBytes)
	}
	if cfg.FlowControl == nil || cfg.flowLimit != int64(minTermLength) {
		t.Fatalf("flow control defaults = %#v/%d", cfg.FlowControl, cfg.flowLimit)
	}
	if cfg.TLSConfig == nil || len(cfg.TLSConfig.NextProtos) != 1 || cfg.TLSConfig.NextProtos[0] != quicALPN {
		t.Fatalf("tls config was not defaulted: %#v", cfg.TLSConfig)
	}
}

func TestPublicationConfigNormalizeClonesTLSConfig(t *testing.T) {
	base := &tls.Config{}
	cfg, err := normalizePublicationConfig(PublicationConfig{
		RemoteAddr: "127.0.0.1:40456",
		TLSConfig:  base,
	})
	if err != nil {
		t.Fatal(err)
	}

	if cfg.TLSConfig == base {
		t.Fatal("tls config was not cloned")
	}
	if len(cfg.TLSConfig.NextProtos) != 1 || cfg.TLSConfig.NextProtos[0] != quicALPN {
		t.Fatalf("tls ALPN was not defaulted: %#v", cfg.TLSConfig.NextProtos)
	}
	if len(base.NextProtos) != 0 {
		t.Fatalf("base tls config was mutated: %#v", base.NextProtos)
	}
}

func TestPublicationConfigValidateRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		cfg  PublicationConfig
	}{
		{name: "missing remote", cfg: PublicationConfig{}},
		{name: "negative read buffer", cfg: PublicationConfig{RemoteAddr: "127.0.0.1:1", ReadBufferBytes: -1}},
		{name: "negative write buffer", cfg: PublicationConfig{RemoteAddr: "127.0.0.1:1", WriteBufferBytes: -1}},
		{name: "negative retransmit", cfg: PublicationConfig{RemoteAddr: "127.0.0.1:1", RetransmitEvery: -time.Nanosecond}},
		{name: "small mtu", cfg: PublicationConfig{RemoteAddr: "127.0.0.1:1", MTUBytes: headerLen}},
		{name: "large mtu", cfg: PublicationConfig{RemoteAddr: "127.0.0.1:1", MTUBytes: maxFrameSize + 1}},
		{name: "negative max payload", cfg: PublicationConfig{RemoteAddr: "127.0.0.1:1", MaxPayloadBytes: -1}},
		{name: "invalid term", cfg: PublicationConfig{RemoteAddr: "127.0.0.1:1", TermBufferLength: minTermLength - 1}},
		{name: "invalid window", cfg: PublicationConfig{RemoteAddr: "127.0.0.1:1", PublicationWindowBytes: -1}},
		{name: "invalid flow", cfg: PublicationConfig{RemoteAddr: "127.0.0.1:1", FlowControl: zeroFlowControl{}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate() err = %v, want %v", err, ErrInvalidConfig)
			}
		})
	}
}

func TestSubscriptionConfigNormalizeAppliesDefaults(t *testing.T) {
	cfg, err := normalizeSubscriptionConfig(SubscriptionConfig{
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}

	if cfg.StreamID != defaultStreamID {
		t.Fatalf("stream id = %d, want %d", cfg.StreamID, defaultStreamID)
	}
	if cfg.TLSConfig == nil || len(cfg.TLSConfig.NextProtos) != 1 || cfg.TLSConfig.NextProtos[0] != quicALPN {
		t.Fatalf("tls config was not defaulted: %#v", cfg.TLSConfig)
	}
}

func TestSubscriptionConfigNormalizeClonesTLSConfig(t *testing.T) {
	base := &tls.Config{}
	cfg, err := normalizeSubscriptionConfig(SubscriptionConfig{
		LocalAddr: "127.0.0.1:0",
		TLSConfig: base,
	})
	if err != nil {
		t.Fatal(err)
	}

	if cfg.TLSConfig == base {
		t.Fatal("tls config was not cloned")
	}
	if len(cfg.TLSConfig.NextProtos) != 1 || cfg.TLSConfig.NextProtos[0] != quicALPN {
		t.Fatalf("tls ALPN was not defaulted: %#v", cfg.TLSConfig.NextProtos)
	}
	if len(base.NextProtos) != 0 {
		t.Fatalf("base tls config was mutated: %#v", base.NextProtos)
	}
}

func TestSubscriptionConfigValidateRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		cfg  SubscriptionConfig
	}{
		{name: "missing local", cfg: SubscriptionConfig{}},
		{name: "negative read buffer", cfg: SubscriptionConfig{LocalAddr: "127.0.0.1:0", ReadBufferBytes: -1}},
		{name: "negative write buffer", cfg: SubscriptionConfig{LocalAddr: "127.0.0.1:0", WriteBufferBytes: -1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate() err = %v, want %v", err, ErrInvalidConfig)
			}
		})
	}
}

type zeroFlowControl struct{}

func (zeroFlowControl) InitialLimit(int) int64 {
	return 0
}

func (zeroFlowControl) OnStatus(FlowControlStatus, int64) int64 {
	return 0
}

func (zeroFlowControl) OnIdle(time.Time, int64, int64) int64 {
	return 0
}
