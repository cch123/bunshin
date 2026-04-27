package core

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var ErrInvalidChannelURI = errors.New("bunshin: invalid channel URI")

const channelDestinationParam = "destination"
const channelNameResolutionIntervalParam = "name-resolution-interval"
const channelSpyParam = "spy"

type ChannelURI struct {
	Transport              TransportMode
	Endpoint               string
	ControlEndpoint        string
	Path                   string
	Destinations           []string
	NameResolutionInterval time.Duration
	Spy                    bool
	Params                 map[string]string
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
		if key == channelDestinationParam {
			for _, destination := range value {
				if destination == "" {
					return ChannelURI{}, invalidChannelURI("destination is required")
				}
				ch.AddDestination(destination)
			}
			continue
		}
		if len(value) > 0 {
			ch.Params[key] = value[0]
		}
	}
	ch.Endpoint = ch.Params["endpoint"]
	ch.ControlEndpoint = ch.Params["control"]
	ch.Path = ch.Params["path"]
	if rawSpy := ch.Params[channelSpyParam]; rawSpy != "" {
		spy, err := strconv.ParseBool(rawSpy)
		if err != nil {
			return ChannelURI{}, invalidChannelURI("invalid spy parameter")
		}
		ch.Spy = spy
		delete(ch.Params, channelSpyParam)
	}
	if rawInterval := ch.Params[channelNameResolutionIntervalParam]; rawInterval != "" {
		interval, err := time.ParseDuration(rawInterval)
		if err != nil || interval < 0 {
			return ChannelURI{}, invalidChannelURI("invalid name resolution interval")
		}
		ch.NameResolutionInterval = interval
		delete(ch.Params, channelNameResolutionIntervalParam)
	}

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
	if ch.Transport != TransportUDP && len(ch.Destinations) > 0 {
		return ChannelURI{}, invalidChannelURI("destinations require udp transport")
	}
	if ch.Transport != TransportUDP && ch.NameResolutionInterval > 0 {
		return ChannelURI{}, invalidChannelURI("name resolution interval requires udp transport")
	}
	if ch.Spy && ch.Transport == TransportIPC {
		return ChannelURI{}, invalidChannelURI("spy requires quic or udp transport")
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
	if c.Spy {
		values.Set(channelSpyParam, "true")
	}
	if c.NameResolutionInterval > 0 {
		values.Set(channelNameResolutionIntervalParam, c.NameResolutionInterval.String())
	}
	for _, destination := range c.Destinations {
		if destination != "" {
			values.Add(channelDestinationParam, destination)
		}
	}
	if encoded := values.Encode(); encoded != "" {
		return "bunshin:" + string(c.Transport) + "?" + encoded
	}
	return "bunshin:" + string(c.Transport)
}

func (c *ChannelURI) AddDestination(remoteAddr string) {
	if remoteAddr == "" {
		return
	}
	for _, destination := range c.Destinations {
		if destination == remoteAddr {
			return
		}
	}
	c.Destinations = append(c.Destinations, remoteAddr)
}

func (c *ChannelURI) RemoveDestination(remoteAddr string) {
	for i, destination := range c.Destinations {
		if destination == remoteAddr {
			c.Destinations = append(c.Destinations[:i], c.Destinations[i+1:]...)
			return
		}
	}
}

func (c ChannelURI) DestinationList() []string {
	return append([]string(nil), c.Destinations...)
}

func (c ChannelURI) PublicationConfig(cfg PublicationConfig) PublicationConfig {
	cfg.Transport = c.Transport
	cfg.RemoteAddr = c.Endpoint
	if c.Transport == TransportUDP && len(c.Destinations) > 0 {
		cfg.UDPDestinations = append(cfg.UDPDestinations, c.Destinations...)
	}
	if c.Transport == TransportUDP && c.NameResolutionInterval > 0 {
		cfg.UDPNameResolutionInterval = c.NameResolutionInterval
	}
	return cfg
}

func (c ChannelURI) SubscriptionConfig(cfg SubscriptionConfig) SubscriptionConfig {
	cfg.Transport = c.Transport
	cfg.LocalAddr = c.Endpoint
	cfg.LocalSpy = c.Spy
	return cfg
}

func invalidChannelURI(message string) error {
	return errors.Join(ErrInvalidChannelURI, errors.New(message))
}
