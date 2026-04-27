package transport

import (
	"crypto/tls"
	"github.com/xargin/bunshin/internal/core"
	"time"
)

const (
	ControlledPollAbort    = core.ControlledPollAbort
	ControlledPollBreak    = core.ControlledPollBreak
	ControlledPollCommit   = core.ControlledPollCommit
	ControlledPollContinue = core.ControlledPollContinue
	LogLevelDebug          = core.LogLevelDebug
	LogLevelError          = core.LogLevelError
	LogLevelInfo           = core.LogLevelInfo
	LogLevelWarn           = core.LogLevelWarn
	OfferAccepted          = core.OfferAccepted
	OfferBackPressured     = core.OfferBackPressured
	OfferClosed            = core.OfferClosed
	OfferFailed            = core.OfferFailed
	OfferPayloadTooLarge   = core.OfferPayloadTooLarge
	TransportIPC           = core.TransportIPC
	TransportQUIC          = core.TransportQUIC
	TransportUDP           = core.TransportUDP
)

type (
	AIMDUDPCongestionControl      = core.AIMDUDPCongestionControl
	BackoffIdleStrategy           = core.BackoffIdleStrategy
	BusySpinIdleStrategy          = core.BusySpinIdleStrategy
	ChannelURI                    = core.ChannelURI
	ClientTLSFiles                = core.ClientTLSFiles
	ControlledHandler             = core.ControlledHandler
	ControlledPollAction          = core.ControlledPollAction
	ExclusivePublication          = core.ExclusivePublication
	FlowControlStatus             = core.FlowControlStatus
	FlowControlStrategy           = core.FlowControlStrategy
	Handler                       = core.Handler
	IdleStrategy                  = core.IdleStrategy
	Image                         = core.Image
	ImageHandler                  = core.ImageHandler
	ImageSnapshot                 = core.ImageSnapshot
	LogEvent                      = core.LogEvent
	LogLevel                      = core.LogLevel
	Logger                        = core.Logger
	LoggerFunc                    = core.LoggerFunc
	LossHandler                   = core.LossHandler
	LossObservation               = core.LossObservation
	LossReport                    = core.LossReport
	MaxMulticastFlowControl       = core.MaxMulticastFlowControl
	Message                       = core.Message
	MinMulticastFlowControl       = core.MinMulticastFlowControl
	NoOpIdleStrategy              = core.NoOpIdleStrategy
	PreferredMulticastFlowControl = core.PreferredMulticastFlowControl
	ProtocolError                 = core.ProtocolError
	Publication                   = core.Publication
	PublicationClaim              = core.PublicationClaim
	PublicationConfig             = core.PublicationConfig
	PublicationOfferResult        = core.PublicationOfferResult
	PublicationOfferStatus        = core.PublicationOfferStatus
	ReservedValueSupplier         = core.ReservedValueSupplier
	ResponseChannel               = core.ResponseChannel
	ServerTLSFiles                = core.ServerTLSFiles
	SleepingIdleStrategy          = core.SleepingIdleStrategy
	Subscription                  = core.Subscription
	SubscriptionConfig            = core.SubscriptionConfig
	SubscriptionLagReport         = core.SubscriptionLagReport
	TransportFeedback             = core.TransportFeedback
	TransportFeedbackHandler      = core.TransportFeedbackHandler
	TransportMode                 = core.TransportMode
	UDPCongestionControl          = core.UDPCongestionControl
	UDPDestinationStatus          = core.UDPDestinationStatus
	UDPSubscriptionPeerStatus     = core.UDPSubscriptionPeerStatus
	UnicastFlowControl            = core.UnicastFlowControl
	YieldingIdleStrategy          = core.YieldingIdleStrategy
)

var (
	ErrBackPressure            = core.ErrBackPressure
	ErrClosed                  = core.ErrClosed
	ErrControlledPollAbort     = core.ErrControlledPollAbort
	ErrInvalidChannelURI       = core.ErrInvalidChannelURI
	ErrInvalidConfig           = core.ErrInvalidConfig
	ErrPublicationClaimAborted = core.ErrPublicationClaimAborted
	ErrPublicationClaimClosed  = core.ErrPublicationClaimClosed
)

func ClientTLSConfigFromFiles(files ClientTLSFiles) (*tls.Config, error) {
	return core.ClientTLSConfigFromFiles(files)
}

func DialExclusivePublication(cfg PublicationConfig) (*ExclusivePublication, error) {
	return core.DialExclusivePublication(cfg)
}

func DialPublication(cfg PublicationConfig) (*Publication, error) {
	return core.DialPublication(cfg)
}

func ListenSubscription(cfg SubscriptionConfig) (*Subscription, error) {
	return core.ListenSubscription(cfg)
}

func NewAIMDUDPCongestionControl() *AIMDUDPCongestionControl {
	return core.NewAIMDUDPCongestionControl()
}

func NewBackoffIdleStrategy(maxSpins, maxYields int, minSleep, maxSleep time.Duration) *BackoffIdleStrategy {
	return core.NewBackoffIdleStrategy(maxSpins, maxYields, minSleep, maxSleep)
}

func NewDefaultBackoffIdleStrategy() *BackoffIdleStrategy {
	return core.NewDefaultBackoffIdleStrategy()
}

func NewMinMulticastFlowControl(timeout time.Duration) *MinMulticastFlowControl {
	return core.NewMinMulticastFlowControl(timeout)
}

func NewPreferredMulticastFlowControl(timeout time.Duration, preferredReceiverIDs ...string) *PreferredMulticastFlowControl {
	return core.NewPreferredMulticastFlowControl(timeout, preferredReceiverIDs...)
}

func ParseChannelURI(raw string) (ChannelURI, error) {
	return core.ParseChannelURI(raw)
}

func ServerTLSConfigFromFiles(files ServerTLSFiles) (*tls.Config, error) {
	return core.ServerTLSConfigFromFiles(files)
}
