package bunshin

import (
	"errors"
	"fmt"
	"math/rand/v2"
)

const defaultStreamID uint32 = 1

var ErrInvalidConfig = errors.New("bunshin: invalid config")

type normalizedPublicationConfig struct {
	PublicationConfig
	mtuPayload          int
	flowLimit           int64
	udpCongestionWindow int
}

func (cfg PublicationConfig) Validate() error {
	_, err := normalizePublicationConfig(cfg)
	return err
}

func (cfg SubscriptionConfig) Validate() error {
	_, err := normalizeSubscriptionConfig(cfg)
	return err
}

func normalizePublicationConfig(cfg PublicationConfig) (normalizedPublicationConfig, error) {
	if cfg.Transport == "" {
		cfg.Transport = TransportQUIC
	}
	switch cfg.Transport {
	case TransportQUIC, TransportUDP:
	default:
		return normalizedPublicationConfig{}, invalidConfigf("invalid publication transport: %s", cfg.Transport)
	}
	if cfg.RemoteAddr == "" {
		return normalizedPublicationConfig{}, invalidConfigf("remote address is required")
	}
	if cfg.ReadBufferBytes < 0 {
		return normalizedPublicationConfig{}, invalidConfigf("invalid read buffer bytes: %d", cfg.ReadBufferBytes)
	}
	if cfg.WriteBufferBytes < 0 {
		return normalizedPublicationConfig{}, invalidConfigf("invalid write buffer bytes: %d", cfg.WriteBufferBytes)
	}
	if cfg.RetransmitEvery < 0 {
		return normalizedPublicationConfig{}, invalidConfigf("invalid retransmit interval: %s", cfg.RetransmitEvery)
	}
	if cfg.UDPRetransmitBufferBytes < 0 {
		return normalizedPublicationConfig{}, invalidConfigf("invalid UDP retransmit buffer bytes: %d", cfg.UDPRetransmitBufferBytes)
	}
	if cfg.UDPNameResolutionInterval < 0 {
		return normalizedPublicationConfig{}, invalidConfigf("invalid UDP name resolution interval: %s", cfg.UDPNameResolutionInterval)
	}
	if cfg.UDPReceiverTimeout < 0 {
		return normalizedPublicationConfig{}, invalidConfigf("invalid UDP receiver timeout: %s", cfg.UDPReceiverTimeout)
	}
	if cfg.StreamID == 0 {
		cfg.StreamID = defaultStreamID
	}
	if cfg.SessionID == 0 {
		cfg.SessionID = defaultSessionID()
	}
	if cfg.MTUBytes == 0 && cfg.Transport == TransportUDP {
		cfg.MTUBytes = defaultUDPTransportMTUBytes
	}
	if cfg.UDPRetransmitBufferBytes == 0 && cfg.Transport == TransportUDP {
		cfg.UDPRetransmitBufferBytes = defaultUDPRetransmitBufferBytes
	}
	if cfg.UDPReceiverTimeout == 0 && cfg.Transport == TransportUDP {
		cfg.UDPReceiverTimeout = defaultFlowControlReceiverTimeout
	}
	if cfg.MTUBytes == 0 {
		cfg.MTUBytes = maxFrameSize
	}
	if cfg.MTUBytes <= headerLen || cfg.MTUBytes > maxFrameSize {
		return normalizedPublicationConfig{}, invalidConfigf("invalid MTU bytes: %d", cfg.MTUBytes)
	}

	mtuPayload := cfg.MTUBytes - headerLen
	if cfg.MaxPayloadBytes == 0 {
		cfg.MaxPayloadBytes = mtuPayload
	}
	if cfg.MaxPayloadBytes < 0 || cfg.MaxPayloadBytes > mtuPayload*maxFrameFragments {
		return normalizedPublicationConfig{}, invalidConfigf("invalid max payload bytes: %d", cfg.MaxPayloadBytes)
	}
	if cfg.TermBufferLength == 0 {
		cfg.TermBufferLength = minTermLength
	}
	if err := validateTermLength(cfg.TermBufferLength); err != nil {
		return normalizedPublicationConfig{}, invalidConfigWrap(err)
	}
	if cfg.FlowControl == nil {
		cfg.FlowControl = UnicastFlowControl{}
	}
	responseChannel, err := normalizeResponseChannel(cfg.ResponseChannel, cfg.Transport)
	if err != nil {
		return normalizedPublicationConfig{}, err
	}
	cfg.ResponseChannel = responseChannel

	flowLimit := cfg.FlowControl.InitialLimit(cfg.TermBufferLength)
	if flowLimit <= 0 {
		return normalizedPublicationConfig{}, invalidConfigf("invalid flow control initial limit: %d", flowLimit)
	}
	if cfg.PublicationWindowBytes == 0 {
		cfg.PublicationWindowBytes = max(int(flowLimit), fragmentedWindowBytes(cfg.MaxPayloadBytes, mtuPayload))
	}
	if cfg.PublicationWindowBytes <= 0 {
		return normalizedPublicationConfig{}, invalidConfigf("invalid publication window bytes: %d", cfg.PublicationWindowBytes)
	}
	udpCongestionWindow := 0
	if cfg.UDPCongestionControl != nil {
		if cfg.Transport != TransportUDP {
			return normalizedPublicationConfig{}, invalidConfigf("UDP congestion control requires UDP transport")
		}
		udpCongestionWindow = cfg.UDPCongestionControl.InitialWindow(cfg.PublicationWindowBytes)
		if udpCongestionWindow <= 0 {
			return normalizedPublicationConfig{}, invalidConfigf("invalid UDP congestion window bytes: %d", udpCongestionWindow)
		}
	}

	if cfg.Transport == TransportQUIC {
		if cfg.TLSConfig == nil {
			cfg.TLSConfig = defaultClientTLSConfig()
		} else {
			cfg.TLSConfig = cfg.TLSConfig.Clone()
			if len(cfg.TLSConfig.NextProtos) == 0 {
				cfg.TLSConfig.NextProtos = []string{quicALPN}
			}
		}
	}

	return normalizedPublicationConfig{
		PublicationConfig:   cfg,
		mtuPayload:          mtuPayload,
		flowLimit:           flowLimit,
		udpCongestionWindow: udpCongestionWindow,
	}, nil
}

func normalizeSubscriptionConfig(cfg SubscriptionConfig) (SubscriptionConfig, error) {
	if cfg.Transport == "" {
		cfg.Transport = TransportQUIC
	}
	switch cfg.Transport {
	case TransportQUIC, TransportUDP:
	default:
		return SubscriptionConfig{}, invalidConfigf("invalid subscription transport: %s", cfg.Transport)
	}
	if cfg.LocalAddr == "" && (cfg.PacketConn == nil || cfg.LocalSpy) {
		return SubscriptionConfig{}, invalidConfigf("local address is required")
	}
	if cfg.ReadBufferBytes < 0 {
		return SubscriptionConfig{}, invalidConfigf("invalid read buffer bytes: %d", cfg.ReadBufferBytes)
	}
	if cfg.WriteBufferBytes < 0 {
		return SubscriptionConfig{}, invalidConfigf("invalid write buffer bytes: %d", cfg.WriteBufferBytes)
	}
	if cfg.ReceiverWindowBytes < 0 {
		return SubscriptionConfig{}, invalidConfigf("invalid receiver window bytes: %d", cfg.ReceiverWindowBytes)
	}
	if cfg.UDPNakRetryInterval < 0 {
		return SubscriptionConfig{}, invalidConfigf("invalid UDP NAK retry interval: %s", cfg.UDPNakRetryInterval)
	}
	if cfg.TermBufferLength < 0 {
		return SubscriptionConfig{}, invalidConfigf("invalid subscription term buffer length: %d", cfg.TermBufferLength)
	}
	if cfg.DriverDataRingCapacity < 0 {
		return SubscriptionConfig{}, invalidConfigf("invalid subscription driver data ring capacity: %d", cfg.DriverDataRingCapacity)
	}
	if cfg.DriverDataRingCapacity > 0 {
		if err := validateIPCRingCapacity(cfg.DriverDataRingCapacity); err != nil {
			return SubscriptionConfig{}, invalidConfigWrap(err)
		}
		if cfg.DriverDataRingCapacity < minDriverSubscriptionImageRecordBytes {
			return SubscriptionConfig{}, invalidConfigf("invalid subscription driver data ring capacity: %d", cfg.DriverDataRingCapacity)
		}
	}
	if cfg.LocalSpyBuffer < 0 {
		return SubscriptionConfig{}, invalidConfigf("invalid local spy buffer: %d", cfg.LocalSpyBuffer)
	}
	if cfg.StreamID == 0 {
		cfg.StreamID = defaultStreamID
	}
	if cfg.ReceiverWindowBytes == 0 {
		cfg.ReceiverWindowBytes = minTermLength
	}
	if cfg.TermBufferLength == 0 {
		cfg.TermBufferLength = minTermLength
	}
	if err := validateTermLength(cfg.TermBufferLength); err != nil {
		return SubscriptionConfig{}, invalidConfigWrap(err)
	}
	if cfg.LocalSpyBuffer == 0 {
		cfg.LocalSpyBuffer = defaultLocalSpyBuffer
	}
	if cfg.LocalSpy {
		return cfg, nil
	}

	if cfg.Transport == TransportQUIC {
		if cfg.TLSConfig == nil {
			tlsConf, err := defaultServerTLSConfig()
			if err != nil {
				return SubscriptionConfig{}, err
			}
			cfg.TLSConfig = tlsConf
		} else {
			cfg.TLSConfig = cfg.TLSConfig.Clone()
			if len(cfg.TLSConfig.NextProtos) == 0 {
				cfg.TLSConfig.NextProtos = []string{quicALPN}
			}
		}
	}

	return cfg, nil
}

func invalidConfigf(format string, args ...any) error {
	values := append([]any{ErrInvalidConfig}, args...)
	return fmt.Errorf("%w: "+format, values...)
}

func invalidConfigWrap(err error) error {
	return fmt.Errorf("%w: %w", ErrInvalidConfig, err)
}

func defaultSessionID() uint32 {
	for {
		sessionID := rand.Uint32()
		if sessionID != 0 {
			return sessionID
		}
	}
}
