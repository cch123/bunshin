package observability

import (
	"github.com/quic-go/quic-go"
	"github.com/xargin/bunshin/internal/core"
)

const (
	CounterDriverActiveClients           = core.CounterDriverActiveClients
	CounterDriverActivePublications      = core.CounterDriverActivePublications
	CounterDriverActiveSubscriptions     = core.CounterDriverActiveSubscriptions
	CounterDriverAvailableImages         = core.CounterDriverAvailableImages
	CounterDriverChannelEndpoints        = core.CounterDriverChannelEndpoints
	CounterDriverCleanupRuns             = core.CounterDriverCleanupRuns
	CounterDriverClientsClosed           = core.CounterDriverClientsClosed
	CounterDriverClientsRegistered       = core.CounterDriverClientsRegistered
	CounterDriverCommandsFailed          = core.CounterDriverCommandsFailed
	CounterDriverCommandsProcessed       = core.CounterDriverCommandsProcessed
	CounterDriverConductorDutyCycles     = core.CounterDriverConductorDutyCycles
	CounterDriverDutyCycleMaxNanos       = core.CounterDriverDutyCycleMaxNanos
	CounterDriverDutyCycleNanos          = core.CounterDriverDutyCycleNanos
	CounterDriverDutyCycles              = core.CounterDriverDutyCycles
	CounterDriverImages                  = core.CounterDriverImages
	CounterDriverLaggingImages           = core.CounterDriverLaggingImages
	CounterDriverPublicationEndpoints    = core.CounterDriverPublicationEndpoints
	CounterDriverPublicationsClosed      = core.CounterDriverPublicationsClosed
	CounterDriverPublicationsRegistered  = core.CounterDriverPublicationsRegistered
	CounterDriverReceiverDutyCycles      = core.CounterDriverReceiverDutyCycles
	CounterDriverSenderDutyCycles        = core.CounterDriverSenderDutyCycles
	CounterDriverStaleClientsClosed      = core.CounterDriverStaleClientsClosed
	CounterDriverStallMaxNanos           = core.CounterDriverStallMaxNanos
	CounterDriverStallNanos              = core.CounterDriverStallNanos
	CounterDriverStalls                  = core.CounterDriverStalls
	CounterDriverSubscriptionEndpoints   = core.CounterDriverSubscriptionEndpoints
	CounterDriverSubscriptionsClosed     = core.CounterDriverSubscriptionsClosed
	CounterDriverSubscriptionsRegistered = core.CounterDriverSubscriptionsRegistered
	CounterDriverUnavailableImages       = core.CounterDriverUnavailableImages
	CounterMetricsAcksReceived           = core.CounterMetricsAcksReceived
	CounterMetricsAcksSent               = core.CounterMetricsAcksSent
	CounterMetricsBackPressureEvents     = core.CounterMetricsBackPressureEvents
	CounterMetricsBytesReceived          = core.CounterMetricsBytesReceived
	CounterMetricsBytesSent              = core.CounterMetricsBytesSent
	CounterMetricsConnectionsAccepted    = core.CounterMetricsConnectionsAccepted
	CounterMetricsConnectionsOpened      = core.CounterMetricsConnectionsOpened
	CounterMetricsFramesDropped          = core.CounterMetricsFramesDropped
	CounterMetricsFramesReceived         = core.CounterMetricsFramesReceived
	CounterMetricsFramesSent             = core.CounterMetricsFramesSent
	CounterMetricsLossGapEvents          = core.CounterMetricsLossGapEvents
	CounterMetricsLossGapMessages        = core.CounterMetricsLossGapMessages
	CounterMetricsMessagesReceived       = core.CounterMetricsMessagesReceived
	CounterMetricsMessagesSent           = core.CounterMetricsMessagesSent
	CounterMetricsProtocolErrors         = core.CounterMetricsProtocolErrors
	CounterMetricsRTTLatestNanos         = core.CounterMetricsRTTLatestNanos
	CounterMetricsRTTMaxNanos            = core.CounterMetricsRTTMaxNanos
	CounterMetricsRTTMeasurements        = core.CounterMetricsRTTMeasurements
	CounterMetricsRTTMinNanos            = core.CounterMetricsRTTMinNanos
	CounterMetricsReceiveErrors          = core.CounterMetricsReceiveErrors
	CounterMetricsRetransmits            = core.CounterMetricsRetransmits
	CounterMetricsSendErrors             = core.CounterMetricsSendErrors
	CounterScopeDriver                   = core.CounterScopeDriver
	CounterScopeMetrics                  = core.CounterScopeMetrics
)

type (
	CounterScope    = core.CounterScope
	CounterSnapshot = core.CounterSnapshot
	CounterTypeID   = core.CounterTypeID
	Metrics         = core.Metrics
	MetricsSnapshot = core.MetricsSnapshot
)

func QUICConfigWithQLog(cfg *quic.Config) *quic.Config {
	return core.QUICConfigWithQLog(cfg)
}
