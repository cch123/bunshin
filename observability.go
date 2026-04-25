package bunshin

import (
	"sync/atomic"

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
	}
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
