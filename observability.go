package bunshin

import (
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
)

type Metrics struct {
	connectionsOpened   atomic.Uint64
	connectionsAccepted atomic.Uint64
	messagesSent        atomic.Uint64
	messagesReceived    atomic.Uint64
	framesSent          atomic.Uint64
	framesReceived      atomic.Uint64
	framesDropped       atomic.Uint64
	retransmits         atomic.Uint64
	bytesSent           atomic.Uint64
	bytesReceived       atomic.Uint64
	acksSent            atomic.Uint64
	acksReceived        atomic.Uint64
	backPressureEvents  atomic.Uint64
	lossGapEvents       atomic.Uint64
	lossGapMessages     atomic.Uint64
	sendErrors          atomic.Uint64
	receiveErrors       atomic.Uint64
	protocolErrors      atomic.Uint64
	rttMeasurements     atomic.Uint64
	rttLatestNanos      atomic.Uint64
	rttMinNanos         atomic.Uint64
	rttMaxNanos         atomic.Uint64
}

type CounterScope string

const (
	CounterScopeDriver  CounterScope = "driver"
	CounterScopeMetrics CounterScope = "metrics"
)

type CounterTypeID uint32

const (
	CounterDriverCommandsProcessed       CounterTypeID = 1001
	CounterDriverCommandsFailed          CounterTypeID = 1002
	CounterDriverClientsRegistered       CounterTypeID = 1003
	CounterDriverClientsClosed           CounterTypeID = 1004
	CounterDriverPublicationsRegistered  CounterTypeID = 1005
	CounterDriverPublicationsClosed      CounterTypeID = 1006
	CounterDriverSubscriptionsRegistered CounterTypeID = 1007
	CounterDriverSubscriptionsClosed     CounterTypeID = 1008
	CounterDriverCleanupRuns             CounterTypeID = 1009
	CounterDriverStaleClientsClosed      CounterTypeID = 1010
	CounterDriverDutyCycles              CounterTypeID = 1011
	CounterDriverDutyCycleNanos          CounterTypeID = 1012
	CounterDriverDutyCycleMaxNanos       CounterTypeID = 1013
	CounterDriverStalls                  CounterTypeID = 1014
	CounterDriverStallNanos              CounterTypeID = 1015
	CounterDriverStallMaxNanos           CounterTypeID = 1016
	CounterDriverConductorDutyCycles     CounterTypeID = 1017
	CounterDriverSenderDutyCycles        CounterTypeID = 1018
	CounterDriverReceiverDutyCycles      CounterTypeID = 1019

	CounterDriverActiveClients         CounterTypeID = 1101
	CounterDriverChannelEndpoints      CounterTypeID = 1102
	CounterDriverPublicationEndpoints  CounterTypeID = 1103
	CounterDriverSubscriptionEndpoints CounterTypeID = 1104
	CounterDriverActivePublications    CounterTypeID = 1105
	CounterDriverActiveSubscriptions   CounterTypeID = 1106
	CounterDriverImages                CounterTypeID = 1107
	CounterDriverAvailableImages       CounterTypeID = 1108
	CounterDriverUnavailableImages     CounterTypeID = 1109
	CounterDriverLaggingImages         CounterTypeID = 1110

	CounterMetricsConnectionsOpened   CounterTypeID = 2001
	CounterMetricsConnectionsAccepted CounterTypeID = 2002
	CounterMetricsMessagesSent        CounterTypeID = 2003
	CounterMetricsMessagesReceived    CounterTypeID = 2004
	CounterMetricsFramesSent          CounterTypeID = 2005
	CounterMetricsFramesReceived      CounterTypeID = 2006
	CounterMetricsFramesDropped       CounterTypeID = 2007
	CounterMetricsRetransmits         CounterTypeID = 2008
	CounterMetricsBytesSent           CounterTypeID = 2009
	CounterMetricsBytesReceived       CounterTypeID = 2010
	CounterMetricsAcksSent            CounterTypeID = 2011
	CounterMetricsAcksReceived        CounterTypeID = 2012
	CounterMetricsBackPressureEvents  CounterTypeID = 2013
	CounterMetricsLossGapEvents       CounterTypeID = 2014
	CounterMetricsLossGapMessages     CounterTypeID = 2015
	CounterMetricsSendErrors          CounterTypeID = 2016
	CounterMetricsReceiveErrors       CounterTypeID = 2017
	CounterMetricsProtocolErrors      CounterTypeID = 2018
	CounterMetricsRTTMeasurements     CounterTypeID = 2019
	CounterMetricsRTTLatestNanos      CounterTypeID = 2020
	CounterMetricsRTTMinNanos         CounterTypeID = 2021
	CounterMetricsRTTMaxNanos         CounterTypeID = 2022
)

type CounterSnapshot struct {
	TypeID CounterTypeID `json:"type_id"`
	Scope  CounterScope  `json:"scope"`
	Name   string        `json:"name"`
	Label  string        `json:"label"`
	Value  uint64        `json:"value"`
}

type MetricsSnapshot struct {
	ConnectionsOpened   uint64
	ConnectionsAccepted uint64
	MessagesSent        uint64
	MessagesReceived    uint64
	FramesSent          uint64
	FramesReceived      uint64
	FramesDropped       uint64
	Retransmits         uint64
	BytesSent           uint64
	BytesReceived       uint64
	AcksSent            uint64
	AcksReceived        uint64
	BackPressureEvents  uint64
	LossGapEvents       uint64
	LossGapMessages     uint64
	SendErrors          uint64
	ReceiveErrors       uint64
	ProtocolErrors      uint64
	RTTMeasurements     uint64
	RTTLatest           time.Duration
	RTTMin              time.Duration
	RTTMax              time.Duration
}

type counterDefinition[T any] struct {
	TypeID CounterTypeID
	Scope  CounterScope
	Name   string
	Label  string
	Value  func(T) uint64
}

var driverCounterDefinitions = []counterDefinition[DriverCounters]{
	{TypeID: CounterDriverCommandsProcessed, Scope: CounterScopeDriver, Name: "driver_commands_processed", Label: "Driver commands processed", Value: func(c DriverCounters) uint64 { return c.CommandsProcessed }},
	{TypeID: CounterDriverCommandsFailed, Scope: CounterScopeDriver, Name: "driver_commands_failed", Label: "Driver commands failed", Value: func(c DriverCounters) uint64 { return c.CommandsFailed }},
	{TypeID: CounterDriverClientsRegistered, Scope: CounterScopeDriver, Name: "driver_clients_registered", Label: "Driver clients registered", Value: func(c DriverCounters) uint64 { return c.ClientsRegistered }},
	{TypeID: CounterDriverClientsClosed, Scope: CounterScopeDriver, Name: "driver_clients_closed", Label: "Driver clients closed", Value: func(c DriverCounters) uint64 { return c.ClientsClosed }},
	{TypeID: CounterDriverPublicationsRegistered, Scope: CounterScopeDriver, Name: "driver_publications_registered", Label: "Driver publications registered", Value: func(c DriverCounters) uint64 { return c.PublicationsRegistered }},
	{TypeID: CounterDriverPublicationsClosed, Scope: CounterScopeDriver, Name: "driver_publications_closed", Label: "Driver publications closed", Value: func(c DriverCounters) uint64 { return c.PublicationsClosed }},
	{TypeID: CounterDriverSubscriptionsRegistered, Scope: CounterScopeDriver, Name: "driver_subscriptions_registered", Label: "Driver subscriptions registered", Value: func(c DriverCounters) uint64 { return c.SubscriptionsRegistered }},
	{TypeID: CounterDriverSubscriptionsClosed, Scope: CounterScopeDriver, Name: "driver_subscriptions_closed", Label: "Driver subscriptions closed", Value: func(c DriverCounters) uint64 { return c.SubscriptionsClosed }},
	{TypeID: CounterDriverCleanupRuns, Scope: CounterScopeDriver, Name: "driver_cleanup_runs", Label: "Driver cleanup runs", Value: func(c DriverCounters) uint64 { return c.CleanupRuns }},
	{TypeID: CounterDriverStaleClientsClosed, Scope: CounterScopeDriver, Name: "driver_stale_clients_closed", Label: "Driver stale clients closed", Value: func(c DriverCounters) uint64 { return c.StaleClientsClosed }},
	{TypeID: CounterDriverDutyCycles, Scope: CounterScopeDriver, Name: "driver_duty_cycles", Label: "Driver duty cycles", Value: func(c DriverCounters) uint64 { return c.DutyCycles }},
	{TypeID: CounterDriverConductorDutyCycles, Scope: CounterScopeDriver, Name: "driver_conductor_duty_cycles", Label: "Driver conductor duty cycles", Value: func(c DriverCounters) uint64 { return c.ConductorDutyCycles }},
	{TypeID: CounterDriverSenderDutyCycles, Scope: CounterScopeDriver, Name: "driver_sender_duty_cycles", Label: "Driver sender duty cycles", Value: func(c DriverCounters) uint64 { return c.SenderDutyCycles }},
	{TypeID: CounterDriverReceiverDutyCycles, Scope: CounterScopeDriver, Name: "driver_receiver_duty_cycles", Label: "Driver receiver duty cycles", Value: func(c DriverCounters) uint64 { return c.ReceiverDutyCycles }},
	{TypeID: CounterDriverDutyCycleNanos, Scope: CounterScopeDriver, Name: "driver_duty_cycle_nanos", Label: "Driver duty cycle nanoseconds", Value: func(c DriverCounters) uint64 { return c.DutyCycleNanos }},
	{TypeID: CounterDriverDutyCycleMaxNanos, Scope: CounterScopeDriver, Name: "driver_duty_cycle_max_nanos", Label: "Driver maximum duty cycle nanoseconds", Value: func(c DriverCounters) uint64 { return c.DutyCycleMaxNanos }},
	{TypeID: CounterDriverStalls, Scope: CounterScopeDriver, Name: "driver_stalls", Label: "Driver stalls", Value: func(c DriverCounters) uint64 { return c.Stalls }},
	{TypeID: CounterDriverStallNanos, Scope: CounterScopeDriver, Name: "driver_stall_nanos", Label: "Driver stall nanoseconds", Value: func(c DriverCounters) uint64 { return c.StallNanos }},
	{TypeID: CounterDriverStallMaxNanos, Scope: CounterScopeDriver, Name: "driver_stall_max_nanos", Label: "Driver maximum stall nanoseconds", Value: func(c DriverCounters) uint64 { return c.StallMaxNanos }},
}

var driverStatusCounterDefinitions = []counterDefinition[DriverStatusCounters]{
	{TypeID: CounterDriverActiveClients, Scope: CounterScopeDriver, Name: "driver_active_clients", Label: "Driver active clients", Value: func(c DriverStatusCounters) uint64 { return c.ActiveClients }},
	{TypeID: CounterDriverChannelEndpoints, Scope: CounterScopeDriver, Name: "driver_channel_endpoints", Label: "Driver channel endpoints", Value: func(c DriverStatusCounters) uint64 { return c.ChannelEndpoints }},
	{TypeID: CounterDriverPublicationEndpoints, Scope: CounterScopeDriver, Name: "driver_publication_endpoints", Label: "Driver publication endpoints", Value: func(c DriverStatusCounters) uint64 { return c.PublicationEndpoints }},
	{TypeID: CounterDriverSubscriptionEndpoints, Scope: CounterScopeDriver, Name: "driver_subscription_endpoints", Label: "Driver subscription endpoints", Value: func(c DriverStatusCounters) uint64 { return c.SubscriptionEndpoints }},
	{TypeID: CounterDriverActivePublications, Scope: CounterScopeDriver, Name: "driver_active_publications", Label: "Driver active publications", Value: func(c DriverStatusCounters) uint64 { return c.ActivePublications }},
	{TypeID: CounterDriverActiveSubscriptions, Scope: CounterScopeDriver, Name: "driver_active_subscriptions", Label: "Driver active subscriptions", Value: func(c DriverStatusCounters) uint64 { return c.ActiveSubscriptions }},
	{TypeID: CounterDriverImages, Scope: CounterScopeDriver, Name: "driver_images", Label: "Driver images", Value: func(c DriverStatusCounters) uint64 { return c.Images }},
	{TypeID: CounterDriverAvailableImages, Scope: CounterScopeDriver, Name: "driver_available_images", Label: "Driver available images", Value: func(c DriverStatusCounters) uint64 { return c.AvailableImages }},
	{TypeID: CounterDriverUnavailableImages, Scope: CounterScopeDriver, Name: "driver_unavailable_images", Label: "Driver unavailable images", Value: func(c DriverStatusCounters) uint64 { return c.UnavailableImages }},
	{TypeID: CounterDriverLaggingImages, Scope: CounterScopeDriver, Name: "driver_lagging_images", Label: "Driver lagging images", Value: func(c DriverStatusCounters) uint64 { return c.LaggingImages }},
}

var metricsCounterDefinitions = []counterDefinition[MetricsSnapshot]{
	{TypeID: CounterMetricsConnectionsOpened, Scope: CounterScopeMetrics, Name: "connections_opened", Label: "Connections opened", Value: func(s MetricsSnapshot) uint64 { return s.ConnectionsOpened }},
	{TypeID: CounterMetricsConnectionsAccepted, Scope: CounterScopeMetrics, Name: "connections_accepted", Label: "Connections accepted", Value: func(s MetricsSnapshot) uint64 { return s.ConnectionsAccepted }},
	{TypeID: CounterMetricsMessagesSent, Scope: CounterScopeMetrics, Name: "messages_sent", Label: "Messages sent", Value: func(s MetricsSnapshot) uint64 { return s.MessagesSent }},
	{TypeID: CounterMetricsMessagesReceived, Scope: CounterScopeMetrics, Name: "messages_received", Label: "Messages received", Value: func(s MetricsSnapshot) uint64 { return s.MessagesReceived }},
	{TypeID: CounterMetricsFramesSent, Scope: CounterScopeMetrics, Name: "frames_sent", Label: "Frames sent", Value: func(s MetricsSnapshot) uint64 { return s.FramesSent }},
	{TypeID: CounterMetricsFramesReceived, Scope: CounterScopeMetrics, Name: "frames_received", Label: "Frames received", Value: func(s MetricsSnapshot) uint64 { return s.FramesReceived }},
	{TypeID: CounterMetricsFramesDropped, Scope: CounterScopeMetrics, Name: "frames_dropped", Label: "Frames dropped", Value: func(s MetricsSnapshot) uint64 { return s.FramesDropped }},
	{TypeID: CounterMetricsRetransmits, Scope: CounterScopeMetrics, Name: "retransmits", Label: "Retransmits", Value: func(s MetricsSnapshot) uint64 { return s.Retransmits }},
	{TypeID: CounterMetricsBytesSent, Scope: CounterScopeMetrics, Name: "bytes_sent", Label: "Bytes sent", Value: func(s MetricsSnapshot) uint64 { return s.BytesSent }},
	{TypeID: CounterMetricsBytesReceived, Scope: CounterScopeMetrics, Name: "bytes_received", Label: "Bytes received", Value: func(s MetricsSnapshot) uint64 { return s.BytesReceived }},
	{TypeID: CounterMetricsAcksSent, Scope: CounterScopeMetrics, Name: "acks_sent", Label: "ACKs sent", Value: func(s MetricsSnapshot) uint64 { return s.AcksSent }},
	{TypeID: CounterMetricsAcksReceived, Scope: CounterScopeMetrics, Name: "acks_received", Label: "ACKs received", Value: func(s MetricsSnapshot) uint64 { return s.AcksReceived }},
	{TypeID: CounterMetricsBackPressureEvents, Scope: CounterScopeMetrics, Name: "back_pressure_events", Label: "Back pressure events", Value: func(s MetricsSnapshot) uint64 { return s.BackPressureEvents }},
	{TypeID: CounterMetricsLossGapEvents, Scope: CounterScopeMetrics, Name: "loss_gap_events", Label: "Loss gap events", Value: func(s MetricsSnapshot) uint64 { return s.LossGapEvents }},
	{TypeID: CounterMetricsLossGapMessages, Scope: CounterScopeMetrics, Name: "loss_gap_messages", Label: "Loss gap messages", Value: func(s MetricsSnapshot) uint64 { return s.LossGapMessages }},
	{TypeID: CounterMetricsSendErrors, Scope: CounterScopeMetrics, Name: "send_errors", Label: "Send errors", Value: func(s MetricsSnapshot) uint64 { return s.SendErrors }},
	{TypeID: CounterMetricsReceiveErrors, Scope: CounterScopeMetrics, Name: "receive_errors", Label: "Receive errors", Value: func(s MetricsSnapshot) uint64 { return s.ReceiveErrors }},
	{TypeID: CounterMetricsProtocolErrors, Scope: CounterScopeMetrics, Name: "protocol_errors", Label: "Protocol errors", Value: func(s MetricsSnapshot) uint64 { return s.ProtocolErrors }},
	{TypeID: CounterMetricsRTTMeasurements, Scope: CounterScopeMetrics, Name: "rtt_measurements", Label: "RTT measurements", Value: func(s MetricsSnapshot) uint64 { return s.RTTMeasurements }},
	{TypeID: CounterMetricsRTTLatestNanos, Scope: CounterScopeMetrics, Name: "rtt_latest_nanos", Label: "RTT latest nanoseconds", Value: func(s MetricsSnapshot) uint64 { return uint64(s.RTTLatest) }},
	{TypeID: CounterMetricsRTTMinNanos, Scope: CounterScopeMetrics, Name: "rtt_min_nanos", Label: "RTT minimum nanoseconds", Value: func(s MetricsSnapshot) uint64 { return uint64(s.RTTMin) }},
	{TypeID: CounterMetricsRTTMaxNanos, Scope: CounterScopeMetrics, Name: "rtt_max_nanos", Label: "RTT maximum nanoseconds", Value: func(s MetricsSnapshot) uint64 { return uint64(s.RTTMax) }},
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	if m == nil {
		return MetricsSnapshot{}
	}
	return MetricsSnapshot{
		ConnectionsOpened:   m.connectionsOpened.Load(),
		ConnectionsAccepted: m.connectionsAccepted.Load(),
		MessagesSent:        m.messagesSent.Load(),
		MessagesReceived:    m.messagesReceived.Load(),
		FramesSent:          m.framesSent.Load(),
		FramesReceived:      m.framesReceived.Load(),
		FramesDropped:       m.framesDropped.Load(),
		Retransmits:         m.retransmits.Load(),
		BytesSent:           m.bytesSent.Load(),
		BytesReceived:       m.bytesReceived.Load(),
		AcksSent:            m.acksSent.Load(),
		AcksReceived:        m.acksReceived.Load(),
		BackPressureEvents:  m.backPressureEvents.Load(),
		LossGapEvents:       m.lossGapEvents.Load(),
		LossGapMessages:     m.lossGapMessages.Load(),
		SendErrors:          m.sendErrors.Load(),
		ReceiveErrors:       m.receiveErrors.Load(),
		ProtocolErrors:      m.protocolErrors.Load(),
		RTTMeasurements:     m.rttMeasurements.Load(),
		RTTLatest:           time.Duration(m.rttLatestNanos.Load()),
		RTTMin:              time.Duration(m.rttMinNanos.Load()),
		RTTMax:              time.Duration(m.rttMaxNanos.Load()),
	}
}

func (m *Metrics) CounterSnapshots() []CounterSnapshot {
	return m.Snapshot().CounterSnapshots()
}

func (s MetricsSnapshot) CounterSnapshots() []CounterSnapshot {
	return buildCounterSnapshots(s, metricsCounterDefinitions)
}

func (c DriverCounters) CounterSnapshots() []CounterSnapshot {
	return buildCounterSnapshots(c, driverCounterDefinitions)
}

func (c DriverStatusCounters) CounterSnapshots() []CounterSnapshot {
	return buildCounterSnapshots(c, driverStatusCounterDefinitions)
}

func buildDriverCounterSnapshots(counters DriverCounters, status DriverStatusCounters, metrics *Metrics) []CounterSnapshot {
	driverCounters := counters.CounterSnapshots()
	statusCounters := status.CounterSnapshots()
	metricCounters := metrics.CounterSnapshots()
	snapshots := make([]CounterSnapshot, 0, len(driverCounters)+len(statusCounters)+len(metricCounters))
	snapshots = append(snapshots, driverCounters...)
	snapshots = append(snapshots, statusCounters...)
	snapshots = append(snapshots, metricCounters...)
	return snapshots
}

func buildCounterSnapshots[T any](value T, definitions []counterDefinition[T]) []CounterSnapshot {
	snapshots := make([]CounterSnapshot, 0, len(definitions))
	for _, definition := range definitions {
		snapshots = append(snapshots, CounterSnapshot{
			TypeID: definition.TypeID,
			Scope:  definition.Scope,
			Name:   definition.Name,
			Label:  definition.Label,
			Value:  definition.Value(value),
		})
	}
	return snapshots
}

func QUICConfigWithQLog(cfg *quic.Config) *quic.Config {
	clone := cloneQUICConfig(cfg)
	clone.Tracer = qlog.DefaultConnectionTracer
	return clone
}

func cloneQUICConfig(cfg *quic.Config) *quic.Config {
	if cfg == nil {
		return &quic.Config{}
	}
	clone := *cfg
	return &clone
}

func (m *Metrics) incConnectionsOpened() {
	if m != nil {
		m.connectionsOpened.Add(1)
	}
}

func (m *Metrics) incConnectionsAccepted() {
	if m != nil {
		m.connectionsAccepted.Add(1)
	}
}

func (m *Metrics) incMessagesSent(bytes int) {
	if m != nil {
		m.messagesSent.Add(1)
		m.bytesSent.Add(uint64(bytes))
	}
}

func (m *Metrics) incMessagesReceived(bytes int) {
	if m != nil {
		m.messagesReceived.Add(1)
		m.bytesReceived.Add(uint64(bytes))
	}
}

func (m *Metrics) incFramesSent(frames int) {
	if m != nil && frames > 0 {
		m.framesSent.Add(uint64(frames))
	}
}

func (m *Metrics) incFramesReceived(frames int) {
	if m != nil && frames > 0 {
		m.framesReceived.Add(uint64(frames))
	}
}

func (m *Metrics) incFramesDropped(frames int) {
	if m != nil && frames > 0 {
		m.framesDropped.Add(uint64(frames))
	}
}

func (m *Metrics) incRetransmits(frames int) {
	if m != nil && frames > 0 {
		m.retransmits.Add(uint64(frames))
	}
}

func (m *Metrics) incAcksSent() {
	if m != nil {
		m.acksSent.Add(1)
	}
}

func (m *Metrics) incAcksReceived() {
	if m != nil {
		m.acksReceived.Add(1)
	}
}

func (m *Metrics) incBackPressureEvents() {
	if m != nil {
		m.backPressureEvents.Add(1)
	}
}

func (m *Metrics) incLossGap(missingMessages uint64) {
	if m != nil {
		m.lossGapEvents.Add(1)
		m.lossGapMessages.Add(missingMessages)
	}
}

func (m *Metrics) incSendErrors() {
	if m != nil {
		m.sendErrors.Add(1)
	}
}

func (m *Metrics) incReceiveErrors() {
	if m != nil {
		m.receiveErrors.Add(1)
	}
}

func (m *Metrics) incProtocolErrors() {
	if m != nil {
		m.protocolErrors.Add(1)
	}
}

func (m *Metrics) observeRTT(duration time.Duration) {
	if m == nil || duration <= 0 {
		return
	}
	nanos := uint64(duration)
	m.rttMeasurements.Add(1)
	m.rttLatestNanos.Store(nanos)
	for {
		current := m.rttMinNanos.Load()
		if current != 0 && current <= nanos {
			break
		}
		if m.rttMinNanos.CompareAndSwap(current, nanos) {
			break
		}
	}
	for {
		current := m.rttMaxNanos.Load()
		if current >= nanos {
			break
		}
		if m.rttMaxNanos.CompareAndSwap(current, nanos) {
			break
		}
	}
}
