package bunshin

import (
	"errors"
	"net/url"
	"strings"
)

var ErrInvalidChannelURI = errors.New("bunshin: invalid channel URI")

type ChannelURI struct {
	Transport       TransportMode
	Endpoint        string
	ControlEndpoint string
	Path            string
	Params          map[string]string
}

func ParseChannelURI(raw string) (ChannelURI, error) {
	if !strings.HasPrefix(raw, "bunshin:") {
		return ChannelURI{}, invalidChannelURI("missing bunshin scheme")
	}
	body := strings.TrimPrefix(raw, "bunshin:")
	transport, rawQuery, _ := strings.Cut(body, "?")
	if transport == "" {
		return ChannelURI{}, invalidChannelURI("missing transport")
	}

	ch := ChannelURI{
		Transport: TransportMode(transport),
		Params:    make(map[string]string),
	}
	switch ch.Transport {
	case TransportQUIC, TransportUDP, TransportIPC:
	default:
		return ChannelURI{}, invalidChannelURI("unsupported transport")
	}

	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return ChannelURI{}, err
	}
	for key, value := range values {
		if len(value) > 0 {
			ch.Params[key] = value[0]
		}
	}
	ch.Endpoint = ch.Params["endpoint"]
	ch.ControlEndpoint = ch.Params["control"]
	ch.Path = ch.Params["path"]

	switch ch.Transport {
	case TransportQUIC, TransportUDP:
		if ch.Endpoint == "" {
			return ChannelURI{}, invalidChannelURI("endpoint is required")
		}
	case TransportIPC:
		if ch.Path == "" {
			return ChannelURI{}, invalidChannelURI("path is required")
		}
	}
	return ch, nil
}

func (c ChannelURI) String() string {
	values := url.Values{}
	for key, value := range c.Params {
		if value != "" {
			values.Set(key, value)
		}
	}
	if c.Endpoint != "" {
		values.Set("endpoint", c.Endpoint)
	}
	if c.ControlEndpoint != "" {
		values.Set("control", c.ControlEndpoint)
	}
	if c.Path != "" {
		values.Set("path", c.Path)
	}
	if encoded := values.Encode(); encoded != "" {
		return "bunshin:" + string(c.Transport) + "?" + encoded
	}
	return "bunshin:" + string(c.Transport)
}

func (c ChannelURI) PublicationConfig(cfg PublicationConfig) PublicationConfig {
	cfg.Transport = c.Transport
	cfg.RemoteAddr = c.Endpoint
	return cfg
}

func (c ChannelURI) SubscriptionConfig(cfg SubscriptionConfig) SubscriptionConfig {
	cfg.Transport = c.Transport
	cfg.LocalAddr = c.Endpoint
	return cfg
}

func invalidChannelURI(message string) error {
	return errors.Join(ErrInvalidChannelURI, errors.New(message))
}
