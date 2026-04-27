package core

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"time"
)

const (
	clusterMemberProtoMaxMessageBytes = 512 << 20

	clusterMemberProtoWireVarint  = 0
	clusterMemberProtoWireFixed64 = 1
	clusterMemberProtoWireBytes   = 2
	clusterMemberProtoWireFixed32 = 5
)

func writeClusterMemberProtocolMessage(w io.Writer, msg ClusterMemberProtocolMessage) error {
	payload := encodeClusterMemberProtocolMessage(msg)
	var prefix [10]byte
	n := clusterMemberProtoPutVarint(prefix[:], uint64(len(payload)))
	frame := make([]byte, 0, n+len(payload))
	frame = append(frame, prefix[:n]...)
	frame = append(frame, payload...)
	return clusterMemberProtoWriteAll(w, frame)
}

func readClusterMemberProtocolMessage(r *bufio.Reader) (ClusterMemberProtocolMessage, error) {
	size, err := clusterMemberProtoReadFrameVarint(r)
	if err != nil {
		return ClusterMemberProtocolMessage{}, err
	}
	if size > clusterMemberProtoMaxMessageBytes {
		return ClusterMemberProtocolMessage{}, fmt.Errorf("%w: cluster member protobuf message too large: %d", ErrInvalidConfig, size)
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(r, payload); err != nil {
		return ClusterMemberProtocolMessage{}, err
	}
	return decodeClusterMemberProtocolMessage(payload)
}

func encodeClusterMemberProtocolMessage(msg ClusterMemberProtocolMessage) []byte {
	var buf []byte
	buf = clusterMemberProtoAppendUint64(buf, 1, uint64(msg.Version))
	buf = clusterMemberProtoAppendString(buf, 2, string(msg.Type))
	buf = clusterMemberProtoAppendUint64(buf, 3, msg.CorrelationID)
	buf = clusterMemberProtoAppendString(buf, 4, string(msg.Action))
	buf = clusterMemberProtoAppendString(buf, 5, msg.Error)
	buf = clusterMemberProtoAppendString(buf, 6, msg.ErrorCode)
	if !clusterMemberProtoZeroLogEntry(msg.Entry) {
		buf = clusterMemberProtoAppendMessage(buf, 7, encodeClusterLogEntryProto(msg.Entry))
	}
	for _, entry := range msg.Entries {
		buf = clusterMemberProtoAppendMessage(buf, 8, encodeClusterLogEntryProto(entry))
	}
	buf = clusterMemberProtoAppendInt64(buf, 9, msg.Position)
	if !clusterMemberProtoZeroSnapshot(msg.Snapshot) {
		buf = clusterMemberProtoAppendMessage(buf, 10, encodeClusterStateSnapshotProto(msg.Snapshot))
	}
	buf = clusterMemberProtoAppendBool(buf, 11, msg.HasSnapshot)
	if !clusterMemberProtoZeroIngress(msg.Ingress) {
		buf = clusterMemberProtoAppendMessage(buf, 12, encodeClusterIngressProto(msg.Ingress))
	}
	if !clusterMemberProtoZeroEgress(msg.Egress) {
		buf = clusterMemberProtoAppendMessage(buf, 13, encodeClusterEgressProto(msg.Egress))
	}
	if !clusterMemberProtoZeroDescription(msg.Description) {
		buf = clusterMemberProtoAppendMessage(buf, 14, encodeClusterDescriptionProto(msg.Description))
	}
	for _, ingress := range msg.Ingresses {
		buf = clusterMemberProtoAppendMessage(buf, 15, encodeClusterIngressProto(ingress))
	}
	for _, egress := range msg.Egresses {
		buf = clusterMemberProtoAppendMessage(buf, 16, encodeClusterEgressProto(egress))
	}
	return buf
}

func decodeClusterMemberProtocolMessage(data []byte) (ClusterMemberProtocolMessage, error) {
	var msg ClusterMemberProtocolMessage
	i := 0
	for i < len(data) {
		field, wire, err := clusterMemberProtoReadTag(data, &i)
		if err != nil {
			return ClusterMemberProtocolMessage{}, err
		}
		switch field {
		case 1:
			value, err := clusterMemberProtoReadUint64(data, &i, wire)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			msg.Version = int(value)
		case 2:
			value, err := clusterMemberProtoReadString(data, &i, wire)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			msg.Type = ClusterMemberProtocolMessageType(value)
		case 3:
			value, err := clusterMemberProtoReadUint64(data, &i, wire)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			msg.CorrelationID = value
		case 4:
			value, err := clusterMemberProtoReadString(data, &i, wire)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			msg.Action = ClusterMemberProtocolAction(value)
		case 5:
			value, err := clusterMemberProtoReadString(data, &i, wire)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			msg.Error = value
		case 6:
			value, err := clusterMemberProtoReadString(data, &i, wire)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			msg.ErrorCode = value
		case 7:
			payload, err := clusterMemberProtoReadBytes(data, &i, wire)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			entry, err := decodeClusterLogEntryProto(payload)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			msg.Entry = entry
		case 8:
			payload, err := clusterMemberProtoReadBytes(data, &i, wire)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			entry, err := decodeClusterLogEntryProto(payload)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			msg.Entries = append(msg.Entries, entry)
		case 9:
			value, err := clusterMemberProtoReadInt64(data, &i, wire)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			msg.Position = value
		case 10:
			payload, err := clusterMemberProtoReadBytes(data, &i, wire)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			snapshot, err := decodeClusterStateSnapshotProto(payload)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			msg.Snapshot = snapshot
		case 11:
			value, err := clusterMemberProtoReadBool(data, &i, wire)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			msg.HasSnapshot = value
		case 12:
			payload, err := clusterMemberProtoReadBytes(data, &i, wire)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			ingress, err := decodeClusterIngressProto(payload)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			msg.Ingress = ingress
		case 13:
			payload, err := clusterMemberProtoReadBytes(data, &i, wire)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			egress, err := decodeClusterEgressProto(payload)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			msg.Egress = egress
		case 14:
			payload, err := clusterMemberProtoReadBytes(data, &i, wire)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			description, err := decodeClusterDescriptionProto(payload)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			msg.Description = description
		case 15:
			payload, err := clusterMemberProtoReadBytes(data, &i, wire)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			ingress, err := decodeClusterIngressProto(payload)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			msg.Ingresses = append(msg.Ingresses, ingress)
		case 16:
			payload, err := clusterMemberProtoReadBytes(data, &i, wire)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			egress, err := decodeClusterEgressProto(payload)
			if err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
			msg.Egresses = append(msg.Egresses, egress)
		default:
			if err := clusterMemberProtoSkip(data, &i, wire); err != nil {
				return ClusterMemberProtocolMessage{}, err
			}
		}
	}
	return msg, nil
}

func encodeClusterLogEntryProto(entry ClusterLogEntry) []byte {
	var buf []byte
	buf = clusterMemberProtoAppendInt64(buf, 1, entry.Position)
	buf = clusterMemberProtoAppendString(buf, 2, string(entry.Type))
	buf = clusterMemberProtoAppendUint64(buf, 3, uint64(entry.SessionID))
	buf = clusterMemberProtoAppendUint64(buf, 4, uint64(entry.CorrelationID))
	buf = clusterMemberProtoAppendUint64(buf, 5, uint64(entry.TimerID))
	buf = clusterMemberProtoAppendTime(buf, 6, entry.Deadline)
	buf = clusterMemberProtoAppendString(buf, 7, entry.SourceService)
	buf = clusterMemberProtoAppendString(buf, 8, entry.TargetService)
	buf = clusterMemberProtoAppendBytes(buf, 9, entry.Payload)
	return buf
}

func decodeClusterLogEntryProto(data []byte) (ClusterLogEntry, error) {
	var entry ClusterLogEntry
	i := 0
	for i < len(data) {
		field, wire, err := clusterMemberProtoReadTag(data, &i)
		if err != nil {
			return ClusterLogEntry{}, err
		}
		switch field {
		case 1:
			entry.Position, err = clusterMemberProtoReadInt64(data, &i, wire)
		case 2:
			var value string
			value, err = clusterMemberProtoReadString(data, &i, wire)
			entry.Type = ClusterLogEntryType(value)
		case 3:
			var value uint64
			value, err = clusterMemberProtoReadUint64(data, &i, wire)
			entry.SessionID = ClusterSessionID(value)
		case 4:
			var value uint64
			value, err = clusterMemberProtoReadUint64(data, &i, wire)
			entry.CorrelationID = ClusterCorrelationID(value)
		case 5:
			var value uint64
			value, err = clusterMemberProtoReadUint64(data, &i, wire)
			entry.TimerID = ClusterTimerID(value)
		case 6:
			var value int64
			value, err = clusterMemberProtoReadInt64(data, &i, wire)
			entry.Deadline = clusterMemberProtoTime(value)
		case 7:
			entry.SourceService, err = clusterMemberProtoReadString(data, &i, wire)
		case 8:
			entry.TargetService, err = clusterMemberProtoReadString(data, &i, wire)
		case 9:
			entry.Payload, err = clusterMemberProtoReadBytes(data, &i, wire)
		default:
			err = clusterMemberProtoSkip(data, &i, wire)
		}
		if err != nil {
			return ClusterLogEntry{}, err
		}
	}
	return entry, nil
}

func encodeClusterStateSnapshotProto(snapshot ClusterStateSnapshot) []byte {
	var buf []byte
	buf = clusterMemberProtoAppendUint64(buf, 1, uint64(snapshot.NodeID))
	buf = clusterMemberProtoAppendString(buf, 2, string(snapshot.Role))
	buf = clusterMemberProtoAppendInt64(buf, 3, snapshot.Position)
	buf = clusterMemberProtoAppendTime(buf, 4, snapshot.TakenAt)
	buf = clusterMemberProtoAppendBytes(buf, 5, snapshot.Payload)
	for _, timer := range snapshot.Timers {
		buf = clusterMemberProtoAppendMessage(buf, 6, encodeClusterTimerProto(timer))
	}
	return buf
}

func decodeClusterStateSnapshotProto(data []byte) (ClusterStateSnapshot, error) {
	var snapshot ClusterStateSnapshot
	i := 0
	for i < len(data) {
		field, wire, err := clusterMemberProtoReadTag(data, &i)
		if err != nil {
			return ClusterStateSnapshot{}, err
		}
		switch field {
		case 1:
			var value uint64
			value, err = clusterMemberProtoReadUint64(data, &i, wire)
			snapshot.NodeID = ClusterNodeID(value)
		case 2:
			var value string
			value, err = clusterMemberProtoReadString(data, &i, wire)
			snapshot.Role = ClusterRole(value)
		case 3:
			snapshot.Position, err = clusterMemberProtoReadInt64(data, &i, wire)
		case 4:
			var value int64
			value, err = clusterMemberProtoReadInt64(data, &i, wire)
			snapshot.TakenAt = clusterMemberProtoTime(value)
		case 5:
			snapshot.Payload, err = clusterMemberProtoReadBytes(data, &i, wire)
		case 6:
			var payload []byte
			payload, err = clusterMemberProtoReadBytes(data, &i, wire)
			if err == nil {
				var timer ClusterTimer
				timer, err = decodeClusterTimerProto(payload)
				snapshot.Timers = append(snapshot.Timers, timer)
			}
		default:
			err = clusterMemberProtoSkip(data, &i, wire)
		}
		if err != nil {
			return ClusterStateSnapshot{}, err
		}
	}
	return snapshot, nil
}

func encodeClusterTimerProto(timer ClusterTimer) []byte {
	var buf []byte
	buf = clusterMemberProtoAppendUint64(buf, 1, uint64(timer.TimerID))
	buf = clusterMemberProtoAppendTime(buf, 2, timer.Deadline)
	buf = clusterMemberProtoAppendBytes(buf, 3, timer.Payload)
	return buf
}

func decodeClusterTimerProto(data []byte) (ClusterTimer, error) {
	var timer ClusterTimer
	i := 0
	for i < len(data) {
		field, wire, err := clusterMemberProtoReadTag(data, &i)
		if err != nil {
			return ClusterTimer{}, err
		}
		switch field {
		case 1:
			var value uint64
			value, err = clusterMemberProtoReadUint64(data, &i, wire)
			timer.TimerID = ClusterTimerID(value)
		case 2:
			var value int64
			value, err = clusterMemberProtoReadInt64(data, &i, wire)
			timer.Deadline = clusterMemberProtoTime(value)
		case 3:
			timer.Payload, err = clusterMemberProtoReadBytes(data, &i, wire)
		default:
			err = clusterMemberProtoSkip(data, &i, wire)
		}
		if err != nil {
			return ClusterTimer{}, err
		}
	}
	return timer, nil
}

func encodeClusterIngressProto(ingress ClusterIngress) []byte {
	var buf []byte
	buf = clusterMemberProtoAppendUint64(buf, 1, uint64(ingress.SessionID))
	buf = clusterMemberProtoAppendUint64(buf, 2, uint64(ingress.CorrelationID))
	buf = clusterMemberProtoAppendBytes(buf, 3, ingress.Payload)
	return buf
}

func decodeClusterIngressProto(data []byte) (ClusterIngress, error) {
	var ingress ClusterIngress
	i := 0
	for i < len(data) {
		field, wire, err := clusterMemberProtoReadTag(data, &i)
		if err != nil {
			return ClusterIngress{}, err
		}
		switch field {
		case 1:
			var value uint64
			value, err = clusterMemberProtoReadUint64(data, &i, wire)
			ingress.SessionID = ClusterSessionID(value)
		case 2:
			var value uint64
			value, err = clusterMemberProtoReadUint64(data, &i, wire)
			ingress.CorrelationID = ClusterCorrelationID(value)
		case 3:
			ingress.Payload, err = clusterMemberProtoReadBytes(data, &i, wire)
		default:
			err = clusterMemberProtoSkip(data, &i, wire)
		}
		if err != nil {
			return ClusterIngress{}, err
		}
	}
	return ingress, nil
}

func encodeClusterEgressProto(egress ClusterEgress) []byte {
	var buf []byte
	buf = clusterMemberProtoAppendUint64(buf, 1, uint64(egress.SessionID))
	buf = clusterMemberProtoAppendUint64(buf, 2, uint64(egress.CorrelationID))
	buf = clusterMemberProtoAppendInt64(buf, 3, egress.LogPosition)
	buf = clusterMemberProtoAppendString(buf, 4, string(egress.Type))
	buf = clusterMemberProtoAppendUint64(buf, 5, uint64(egress.TimerID))
	buf = clusterMemberProtoAppendTime(buf, 6, egress.Deadline)
	buf = clusterMemberProtoAppendString(buf, 7, egress.SourceService)
	buf = clusterMemberProtoAppendString(buf, 8, egress.TargetService)
	buf = clusterMemberProtoAppendBytes(buf, 9, egress.Payload)
	return buf
}

func decodeClusterEgressProto(data []byte) (ClusterEgress, error) {
	var egress ClusterEgress
	i := 0
	for i < len(data) {
		field, wire, err := clusterMemberProtoReadTag(data, &i)
		if err != nil {
			return ClusterEgress{}, err
		}
		switch field {
		case 1:
			var value uint64
			value, err = clusterMemberProtoReadUint64(data, &i, wire)
			egress.SessionID = ClusterSessionID(value)
		case 2:
			var value uint64
			value, err = clusterMemberProtoReadUint64(data, &i, wire)
			egress.CorrelationID = ClusterCorrelationID(value)
		case 3:
			egress.LogPosition, err = clusterMemberProtoReadInt64(data, &i, wire)
		case 4:
			var value string
			value, err = clusterMemberProtoReadString(data, &i, wire)
			egress.Type = ClusterLogEntryType(value)
		case 5:
			var value uint64
			value, err = clusterMemberProtoReadUint64(data, &i, wire)
			egress.TimerID = ClusterTimerID(value)
		case 6:
			var value int64
			value, err = clusterMemberProtoReadInt64(data, &i, wire)
			egress.Deadline = clusterMemberProtoTime(value)
		case 7:
			egress.SourceService, err = clusterMemberProtoReadString(data, &i, wire)
		case 8:
			egress.TargetService, err = clusterMemberProtoReadString(data, &i, wire)
		case 9:
			egress.Payload, err = clusterMemberProtoReadBytes(data, &i, wire)
		default:
			err = clusterMemberProtoSkip(data, &i, wire)
		}
		if err != nil {
			return ClusterEgress{}, err
		}
	}
	return egress, nil
}

func encodeClusterDescriptionProto(description ClusterDescription) []byte {
	var buf []byte
	buf = clusterMemberProtoAppendUint64(buf, 1, uint64(description.NodeID))
	buf = clusterMemberProtoAppendString(buf, 2, string(description.Mode))
	buf = clusterMemberProtoAppendString(buf, 3, string(description.Role))
	buf = clusterMemberProtoAppendInt64(buf, 4, description.LastPosition)
	buf = clusterMemberProtoAppendInt64(buf, 5, description.SnapshotPosition)
	buf = clusterMemberProtoAppendBool(buf, 6, description.Closed)
	buf = clusterMemberProtoAppendBool(buf, 7, description.Suspended)
	if !clusterMemberProtoZeroLearnerStatus(description.Learner) {
		buf = clusterMemberProtoAppendMessage(buf, 8, encodeClusterLearnerStatusProto(description.Learner))
	}
	if !clusterMemberProtoZeroReplicationStatus(description.Replication) {
		buf = clusterMemberProtoAppendMessage(buf, 9, encodeClusterReplicationStatusProto(description.Replication))
	}
	if !clusterMemberProtoZeroElectionStatus(description.Election) {
		buf = clusterMemberProtoAppendMessage(buf, 10, encodeClusterElectionStatusProto(description.Election))
	}
	if !clusterMemberProtoZeroQuorumStatus(description.Quorum) {
		buf = clusterMemberProtoAppendMessage(buf, 11, encodeClusterQuorumStatusProto(description.Quorum))
	}
	return buf
}

func decodeClusterDescriptionProto(data []byte) (ClusterDescription, error) {
	var description ClusterDescription
	i := 0
	for i < len(data) {
		field, wire, err := clusterMemberProtoReadTag(data, &i)
		if err != nil {
			return ClusterDescription{}, err
		}
		switch field {
		case 1:
			var value uint64
			value, err = clusterMemberProtoReadUint64(data, &i, wire)
			description.NodeID = ClusterNodeID(value)
		case 2:
			var value string
			value, err = clusterMemberProtoReadString(data, &i, wire)
			description.Mode = ClusterMode(value)
		case 3:
			var value string
			value, err = clusterMemberProtoReadString(data, &i, wire)
			description.Role = ClusterRole(value)
		case 4:
			description.LastPosition, err = clusterMemberProtoReadInt64(data, &i, wire)
		case 5:
			description.SnapshotPosition, err = clusterMemberProtoReadInt64(data, &i, wire)
		case 6:
			description.Closed, err = clusterMemberProtoReadBool(data, &i, wire)
		case 7:
			description.Suspended, err = clusterMemberProtoReadBool(data, &i, wire)
		case 8:
			var payload []byte
			payload, err = clusterMemberProtoReadBytes(data, &i, wire)
			if err == nil {
				description.Learner, err = decodeClusterLearnerStatusProto(payload)
			}
		case 9:
			var payload []byte
			payload, err = clusterMemberProtoReadBytes(data, &i, wire)
			if err == nil {
				description.Replication, err = decodeClusterReplicationStatusProto(payload)
			}
		case 10:
			var payload []byte
			payload, err = clusterMemberProtoReadBytes(data, &i, wire)
			if err == nil {
				description.Election, err = decodeClusterElectionStatusProto(payload)
			}
		case 11:
			var payload []byte
			payload, err = clusterMemberProtoReadBytes(data, &i, wire)
			if err == nil {
				description.Quorum, err = decodeClusterQuorumStatusProto(payload)
			}
		default:
			err = clusterMemberProtoSkip(data, &i, wire)
		}
		if err != nil {
			return ClusterDescription{}, err
		}
	}
	return description, nil
}

func encodeClusterLearnerStatusProto(status ClusterLearnerStatus) []byte {
	var buf []byte
	buf = clusterMemberProtoAppendBool(buf, 1, status.Enabled)
	buf = clusterMemberProtoAppendInt64(buf, 2, status.MasterPosition)
	buf = clusterMemberProtoAppendInt64(buf, 3, status.SyncedPosition)
	buf = clusterMemberProtoAppendUint64(buf, 4, status.Snapshots)
	buf = clusterMemberProtoAppendUint64(buf, 5, status.AppliedSinceSnapshot)
	buf = clusterMemberProtoAppendTime(buf, 6, status.LastSync)
	buf = clusterMemberProtoAppendString(buf, 7, status.LastError)
	return buf
}

func decodeClusterLearnerStatusProto(data []byte) (ClusterLearnerStatus, error) {
	var status ClusterLearnerStatus
	i := 0
	for i < len(data) {
		field, wire, err := clusterMemberProtoReadTag(data, &i)
		if err != nil {
			return ClusterLearnerStatus{}, err
		}
		switch field {
		case 1:
			status.Enabled, err = clusterMemberProtoReadBool(data, &i, wire)
		case 2:
			status.MasterPosition, err = clusterMemberProtoReadInt64(data, &i, wire)
		case 3:
			status.SyncedPosition, err = clusterMemberProtoReadInt64(data, &i, wire)
		case 4:
			status.Snapshots, err = clusterMemberProtoReadUint64(data, &i, wire)
		case 5:
			status.AppliedSinceSnapshot, err = clusterMemberProtoReadUint64(data, &i, wire)
		case 6:
			var value int64
			value, err = clusterMemberProtoReadInt64(data, &i, wire)
			status.LastSync = clusterMemberProtoTime(value)
		case 7:
			status.LastError, err = clusterMemberProtoReadString(data, &i, wire)
		default:
			err = clusterMemberProtoSkip(data, &i, wire)
		}
		if err != nil {
			return ClusterLearnerStatus{}, err
		}
	}
	return status, nil
}

func encodeClusterReplicationStatusProto(status ClusterReplicationStatus) []byte {
	var buf []byte
	buf = clusterMemberProtoAppendBool(buf, 1, status.Enabled)
	buf = clusterMemberProtoAppendInt64(buf, 2, status.SourcePosition)
	buf = clusterMemberProtoAppendInt64(buf, 3, status.SyncedPosition)
	buf = clusterMemberProtoAppendUint64(buf, 4, status.Applied)
	buf = clusterMemberProtoAppendTime(buf, 5, status.LastSync)
	buf = clusterMemberProtoAppendString(buf, 6, status.LastError)
	return buf
}

func decodeClusterReplicationStatusProto(data []byte) (ClusterReplicationStatus, error) {
	var status ClusterReplicationStatus
	i := 0
	for i < len(data) {
		field, wire, err := clusterMemberProtoReadTag(data, &i)
		if err != nil {
			return ClusterReplicationStatus{}, err
		}
		switch field {
		case 1:
			status.Enabled, err = clusterMemberProtoReadBool(data, &i, wire)
		case 2:
			status.SourcePosition, err = clusterMemberProtoReadInt64(data, &i, wire)
		case 3:
			status.SyncedPosition, err = clusterMemberProtoReadInt64(data, &i, wire)
		case 4:
			status.Applied, err = clusterMemberProtoReadUint64(data, &i, wire)
		case 5:
			var value int64
			value, err = clusterMemberProtoReadInt64(data, &i, wire)
			status.LastSync = clusterMemberProtoTime(value)
		case 6:
			status.LastError, err = clusterMemberProtoReadString(data, &i, wire)
		default:
			err = clusterMemberProtoSkip(data, &i, wire)
		}
		if err != nil {
			return ClusterReplicationStatus{}, err
		}
	}
	return status, nil
}

func encodeClusterElectionStatusProto(status ClusterElectionStatus) []byte {
	var buf []byte
	buf = clusterMemberProtoAppendBool(buf, 1, status.Enabled)
	buf = clusterMemberProtoAppendUint64(buf, 2, uint64(status.LeaderID))
	buf = clusterMemberProtoAppendUint64(buf, 3, status.Term)
	buf = clusterMemberProtoAppendString(buf, 4, string(status.Role))
	for _, member := range status.Members {
		buf = clusterMemberProtoAppendMessage(buf, 5, encodeClusterElectionMemberStatusProto(member))
	}
	buf = clusterMemberProtoAppendTime(buf, 6, status.LastElection)
	buf = clusterMemberProtoAppendString(buf, 7, status.LastError)
	return buf
}

func decodeClusterElectionStatusProto(data []byte) (ClusterElectionStatus, error) {
	var status ClusterElectionStatus
	i := 0
	for i < len(data) {
		field, wire, err := clusterMemberProtoReadTag(data, &i)
		if err != nil {
			return ClusterElectionStatus{}, err
		}
		switch field {
		case 1:
			status.Enabled, err = clusterMemberProtoReadBool(data, &i, wire)
		case 2:
			var value uint64
			value, err = clusterMemberProtoReadUint64(data, &i, wire)
			status.LeaderID = ClusterNodeID(value)
		case 3:
			status.Term, err = clusterMemberProtoReadUint64(data, &i, wire)
		case 4:
			var value string
			value, err = clusterMemberProtoReadString(data, &i, wire)
			status.Role = ClusterRole(value)
		case 5:
			var payload []byte
			payload, err = clusterMemberProtoReadBytes(data, &i, wire)
			if err == nil {
				var member ClusterElectionMemberStatus
				member, err = decodeClusterElectionMemberStatusProto(payload)
				status.Members = append(status.Members, member)
			}
		case 6:
			var value int64
			value, err = clusterMemberProtoReadInt64(data, &i, wire)
			status.LastElection = clusterMemberProtoTime(value)
		case 7:
			status.LastError, err = clusterMemberProtoReadString(data, &i, wire)
		default:
			err = clusterMemberProtoSkip(data, &i, wire)
		}
		if err != nil {
			return ClusterElectionStatus{}, err
		}
	}
	return status, nil
}

func encodeClusterElectionMemberStatusProto(status ClusterElectionMemberStatus) []byte {
	var buf []byte
	buf = clusterMemberProtoAppendUint64(buf, 1, uint64(status.NodeID))
	buf = clusterMemberProtoAppendTime(buf, 2, status.LastHeartbeat)
	buf = clusterMemberProtoAppendBool(buf, 3, status.Alive)
	return buf
}

func decodeClusterElectionMemberStatusProto(data []byte) (ClusterElectionMemberStatus, error) {
	var status ClusterElectionMemberStatus
	i := 0
	for i < len(data) {
		field, wire, err := clusterMemberProtoReadTag(data, &i)
		if err != nil {
			return ClusterElectionMemberStatus{}, err
		}
		switch field {
		case 1:
			var value uint64
			value, err = clusterMemberProtoReadUint64(data, &i, wire)
			status.NodeID = ClusterNodeID(value)
		case 2:
			var value int64
			value, err = clusterMemberProtoReadInt64(data, &i, wire)
			status.LastHeartbeat = clusterMemberProtoTime(value)
		case 3:
			status.Alive, err = clusterMemberProtoReadBool(data, &i, wire)
		default:
			err = clusterMemberProtoSkip(data, &i, wire)
		}
		if err != nil {
			return ClusterElectionMemberStatus{}, err
		}
	}
	return status, nil
}

func encodeClusterQuorumStatusProto(status ClusterQuorumStatus) []byte {
	var buf []byte
	buf = clusterMemberProtoAppendBool(buf, 1, status.Enabled)
	buf = clusterMemberProtoAppendUint64(buf, 2, uint64(status.Members))
	buf = clusterMemberProtoAppendUint64(buf, 3, uint64(status.RequiredAck))
	buf = clusterMemberProtoAppendInt64(buf, 4, status.LastCommit)
	buf = clusterMemberProtoAppendUint64(buf, 5, uint64(status.LastAcks))
	buf = clusterMemberProtoAppendString(buf, 6, status.LastError)
	buf = clusterMemberProtoAppendTime(buf, 7, status.LastAt)
	return buf
}

func decodeClusterQuorumStatusProto(data []byte) (ClusterQuorumStatus, error) {
	var status ClusterQuorumStatus
	i := 0
	for i < len(data) {
		field, wire, err := clusterMemberProtoReadTag(data, &i)
		if err != nil {
			return ClusterQuorumStatus{}, err
		}
		switch field {
		case 1:
			status.Enabled, err = clusterMemberProtoReadBool(data, &i, wire)
		case 2:
			var value uint64
			value, err = clusterMemberProtoReadUint64(data, &i, wire)
			status.Members = int(value)
		case 3:
			var value uint64
			value, err = clusterMemberProtoReadUint64(data, &i, wire)
			status.RequiredAck = int(value)
		case 4:
			status.LastCommit, err = clusterMemberProtoReadInt64(data, &i, wire)
		case 5:
			var value uint64
			value, err = clusterMemberProtoReadUint64(data, &i, wire)
			status.LastAcks = int(value)
		case 6:
			status.LastError, err = clusterMemberProtoReadString(data, &i, wire)
		case 7:
			var value int64
			value, err = clusterMemberProtoReadInt64(data, &i, wire)
			status.LastAt = clusterMemberProtoTime(value)
		default:
			err = clusterMemberProtoSkip(data, &i, wire)
		}
		if err != nil {
			return ClusterQuorumStatus{}, err
		}
	}
	return status, nil
}

func clusterMemberProtoAppendTag(buf []byte, field int, wire int) []byte {
	return clusterMemberProtoAppendRawVarint(buf, uint64(field<<3|wire))
}

func clusterMemberProtoAppendUint64(buf []byte, field int, value uint64) []byte {
	if value == 0 {
		return buf
	}
	buf = clusterMemberProtoAppendTag(buf, field, clusterMemberProtoWireVarint)
	return clusterMemberProtoAppendRawVarint(buf, value)
}

func clusterMemberProtoAppendInt64(buf []byte, field int, value int64) []byte {
	if value == 0 {
		return buf
	}
	buf = clusterMemberProtoAppendTag(buf, field, clusterMemberProtoWireVarint)
	return clusterMemberProtoAppendRawVarint(buf, uint64(value))
}

func clusterMemberProtoAppendBool(buf []byte, field int, value bool) []byte {
	if !value {
		return buf
	}
	buf = clusterMemberProtoAppendTag(buf, field, clusterMemberProtoWireVarint)
	return append(buf, 1)
}

func clusterMemberProtoAppendString(buf []byte, field int, value string) []byte {
	if value == "" {
		return buf
	}
	return clusterMemberProtoAppendBytes(buf, field, []byte(value))
}

func clusterMemberProtoAppendBytes(buf []byte, field int, value []byte) []byte {
	if len(value) == 0 {
		return buf
	}
	buf = clusterMemberProtoAppendTag(buf, field, clusterMemberProtoWireBytes)
	buf = clusterMemberProtoAppendRawVarint(buf, uint64(len(value)))
	return append(buf, value...)
}

func clusterMemberProtoAppendMessage(buf []byte, field int, value []byte) []byte {
	if len(value) == 0 {
		return buf
	}
	return clusterMemberProtoAppendBytes(buf, field, value)
}

func clusterMemberProtoAppendTime(buf []byte, field int, value time.Time) []byte {
	if value.IsZero() {
		return buf
	}
	return clusterMemberProtoAppendInt64(buf, field, value.UTC().UnixNano())
}

func clusterMemberProtoAppendRawVarint(buf []byte, value uint64) []byte {
	for value >= 0x80 {
		buf = append(buf, byte(value)|0x80)
		value >>= 7
	}
	return append(buf, byte(value))
}

func clusterMemberProtoPutVarint(buf []byte, value uint64) int {
	i := 0
	for value >= 0x80 {
		buf[i] = byte(value) | 0x80
		value >>= 7
		i++
	}
	buf[i] = byte(value)
	return i + 1
}

func clusterMemberProtoWriteAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func clusterMemberProtoReadFrameVarint(r io.ByteReader) (uint64, error) {
	var value uint64
	for i := 0; i < 10; i++ {
		b, err := r.ReadByte()
		if err != nil {
			if i > 0 && errors.Is(err, io.EOF) {
				return 0, io.ErrUnexpectedEOF
			}
			return 0, err
		}
		if b < 0x80 {
			if i == 9 && b > 1 {
				return 0, fmt.Errorf("%w: protobuf varint overflow", ErrInvalidConfig)
			}
			return value | uint64(b)<<uint(7*i), nil
		}
		value |= uint64(b&0x7f) << uint(7*i)
	}
	return 0, fmt.Errorf("%w: protobuf varint overflow", ErrInvalidConfig)
}

func clusterMemberProtoReadTag(data []byte, i *int) (int, uint64, error) {
	tag, err := clusterMemberProtoConsumeVarint(data, i)
	if err != nil {
		return 0, 0, err
	}
	if tag == 0 {
		return 0, 0, fmt.Errorf("%w: protobuf field tag is zero", ErrInvalidConfig)
	}
	return int(tag >> 3), tag & 0x7, nil
}

func clusterMemberProtoReadUint64(data []byte, i *int, wire uint64) (uint64, error) {
	if wire != clusterMemberProtoWireVarint {
		return 0, clusterMemberProtoWireError(wire)
	}
	return clusterMemberProtoConsumeVarint(data, i)
}

func clusterMemberProtoReadInt64(data []byte, i *int, wire uint64) (int64, error) {
	value, err := clusterMemberProtoReadUint64(data, i, wire)
	if err != nil {
		return 0, err
	}
	return int64(value), nil
}

func clusterMemberProtoReadBool(data []byte, i *int, wire uint64) (bool, error) {
	value, err := clusterMemberProtoReadUint64(data, i, wire)
	if err != nil {
		return false, err
	}
	return value != 0, nil
}

func clusterMemberProtoReadString(data []byte, i *int, wire uint64) (string, error) {
	value, err := clusterMemberProtoReadBytes(data, i, wire)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func clusterMemberProtoReadBytes(data []byte, i *int, wire uint64) ([]byte, error) {
	if wire != clusterMemberProtoWireBytes {
		return nil, clusterMemberProtoWireError(wire)
	}
	size, err := clusterMemberProtoConsumeVarint(data, i)
	if err != nil {
		return nil, err
	}
	if size > uint64(len(data)-*i) {
		return nil, io.ErrUnexpectedEOF
	}
	value := data[*i : *i+int(size)]
	*i += int(size)
	return cloneBytes(value), nil
}

func clusterMemberProtoConsumeVarint(data []byte, i *int) (uint64, error) {
	var value uint64
	for shift := uint(0); shift < 64; shift += 7 {
		if *i >= len(data) {
			return 0, io.ErrUnexpectedEOF
		}
		b := data[*i]
		*i++
		if b < 0x80 {
			if shift == 63 && b > 1 {
				return 0, fmt.Errorf("%w: protobuf varint overflow", ErrInvalidConfig)
			}
			return value | uint64(b)<<shift, nil
		}
		value |= uint64(b&0x7f) << shift
	}
	return 0, fmt.Errorf("%w: protobuf varint overflow", ErrInvalidConfig)
}

func clusterMemberProtoSkip(data []byte, i *int, wire uint64) error {
	switch wire {
	case clusterMemberProtoWireVarint:
		_, err := clusterMemberProtoConsumeVarint(data, i)
		return err
	case clusterMemberProtoWireFixed64:
		if len(data)-*i < 8 {
			return io.ErrUnexpectedEOF
		}
		*i += 8
		return nil
	case clusterMemberProtoWireBytes:
		size, err := clusterMemberProtoConsumeVarint(data, i)
		if err != nil {
			return err
		}
		if size > uint64(len(data)-*i) {
			return io.ErrUnexpectedEOF
		}
		*i += int(size)
		return nil
	case clusterMemberProtoWireFixed32:
		if len(data)-*i < 4 {
			return io.ErrUnexpectedEOF
		}
		*i += 4
		return nil
	default:
		return clusterMemberProtoWireError(wire)
	}
}

func clusterMemberProtoWireError(wire uint64) error {
	return fmt.Errorf("%w: unsupported protobuf wire type %d", ErrInvalidConfig, wire)
}

func clusterMemberProtoTime(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.Unix(0, value).UTC()
}

func clusterMemberProtoZeroLogEntry(entry ClusterLogEntry) bool {
	return entry.Position == 0 && entry.Type == "" && entry.SessionID == 0 && entry.CorrelationID == 0 &&
		entry.TimerID == 0 && entry.Deadline.IsZero() && entry.SourceService == "" &&
		entry.TargetService == "" && len(entry.Payload) == 0
}

func clusterMemberProtoZeroSnapshot(snapshot ClusterStateSnapshot) bool {
	return snapshot.NodeID == 0 && snapshot.Role == "" && snapshot.Position == 0 &&
		snapshot.TakenAt.IsZero() && len(snapshot.Payload) == 0 && len(snapshot.Timers) == 0
}

func clusterMemberProtoZeroIngress(ingress ClusterIngress) bool {
	return ingress.SessionID == 0 && ingress.CorrelationID == 0 && len(ingress.Payload) == 0
}

func clusterMemberProtoZeroEgress(egress ClusterEgress) bool {
	return egress.SessionID == 0 && egress.CorrelationID == 0 && egress.LogPosition == 0 &&
		egress.Type == "" && egress.TimerID == 0 && egress.Deadline.IsZero() &&
		egress.SourceService == "" && egress.TargetService == "" && len(egress.Payload) == 0
}

func clusterMemberProtoZeroDescription(description ClusterDescription) bool {
	return description.NodeID == 0 && description.Mode == "" && description.Role == "" &&
		description.LastPosition == 0 && description.SnapshotPosition == 0 && !description.Closed &&
		!description.Suspended && clusterMemberProtoZeroLearnerStatus(description.Learner) &&
		clusterMemberProtoZeroReplicationStatus(description.Replication) &&
		clusterMemberProtoZeroElectionStatus(description.Election) &&
		clusterMemberProtoZeroQuorumStatus(description.Quorum)
}

func clusterMemberProtoZeroLearnerStatus(status ClusterLearnerStatus) bool {
	return !status.Enabled && status.MasterPosition == 0 && status.SyncedPosition == 0 &&
		status.Snapshots == 0 && status.AppliedSinceSnapshot == 0 &&
		status.LastSync.IsZero() && status.LastError == ""
}

func clusterMemberProtoZeroReplicationStatus(status ClusterReplicationStatus) bool {
	return !status.Enabled && status.SourcePosition == 0 && status.SyncedPosition == 0 &&
		status.Applied == 0 && status.LastSync.IsZero() && status.LastError == ""
}

func clusterMemberProtoZeroElectionStatus(status ClusterElectionStatus) bool {
	return !status.Enabled && status.LeaderID == 0 && status.Term == 0 && status.Role == "" &&
		len(status.Members) == 0 && status.LastElection.IsZero() && status.LastError == ""
}

func clusterMemberProtoZeroQuorumStatus(status ClusterQuorumStatus) bool {
	return !status.Enabled && status.Members == 0 && status.RequiredAck == 0 &&
		status.LastCommit == 0 && status.LastAcks == 0 && status.LastError == "" &&
		status.LastAt.IsZero()
}
