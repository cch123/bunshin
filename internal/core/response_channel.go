package core

import "context"

type ResponseChannel struct {
	Transport  TransportMode
	RemoteAddr string
	StreamID   uint32
}

func (r ResponseChannel) IsZero() bool {
	return r.Transport == "" && r.RemoteAddr == "" && r.StreamID == 0
}

func (r ResponseChannel) PublicationConfig(cfg PublicationConfig) PublicationConfig {
	if r.Transport != "" {
		cfg.Transport = r.Transport
	}
	if r.RemoteAddr != "" {
		cfg.RemoteAddr = r.RemoteAddr
	}
	if r.StreamID != 0 {
		cfg.StreamID = r.StreamID
	}
	return cfg
}

func (r ResponseChannel) Respond(ctx context.Context, payload []byte, cfg PublicationConfig) error {
	if r.IsZero() {
		return invalidConfigf("response channel is required")
	}
	pub, err := DialPublication(r.PublicationConfig(cfg))
	if err != nil {
		return err
	}
	defer pub.Close()
	return pub.Send(ctx, payload)
}

func (m Message) HasResponseChannel() bool {
	return !m.ResponseChannel.IsZero()
}

func (m Message) Respond(ctx context.Context, payload []byte, cfg PublicationConfig) error {
	return m.ResponseChannel.Respond(ctx, payload, cfg)
}

func normalizeResponseChannel(response ResponseChannel, fallback TransportMode) (ResponseChannel, error) {
	if response.IsZero() {
		return ResponseChannel{}, nil
	}
	if response.RemoteAddr == "" {
		return ResponseChannel{}, invalidConfigf("response channel remote address is required")
	}
	if response.Transport == "" {
		response.Transport = fallback
	}
	if response.Transport == "" {
		response.Transport = TransportQUIC
	}
	switch response.Transport {
	case TransportQUIC, TransportUDP:
	default:
		return ResponseChannel{}, invalidConfigf("invalid response channel transport: %s", response.Transport)
	}
	if response.StreamID == 0 {
		response.StreamID = defaultStreamID
	}
	return response, nil
}
