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
	bytesSent           atomic.Uint64
	bytesReceived       atomic.Uint64
	acksSent            atomic.Uint64
	acksReceived        atomic.Uint64
	sendErrors          atomic.Uint64
	receiveErrors       atomic.Uint64
	protocolErrors      atomic.Uint64
}

type MetricsSnapshot struct {
	ConnectionsOpened   uint64
	ConnectionsAccepted uint64
	MessagesSent        uint64
	MessagesReceived    uint64
	BytesSent           uint64
	BytesReceived       uint64
	AcksSent            uint64
	AcksReceived        uint64
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
		BytesSent:           m.bytesSent.Load(),
		BytesReceived:       m.bytesReceived.Load(),
		AcksSent:            m.acksSent.Load(),
		AcksReceived:        m.acksReceived.Load(),
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
