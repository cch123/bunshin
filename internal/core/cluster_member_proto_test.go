package core

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
	"time"
)

func TestClusterMemberProtocolProtobufRoundTrip(t *testing.T) {
	now := time.Unix(1, 234).UTC()
	input := ClusterMemberProtocolMessage{
		Version:       ClusterMemberProtocolVersion,
		Type:          ClusterMemberProtocolResponse,
		CorrelationID: 42,
		Action:        ClusterMemberActionDescribe,
		Entry: ClusterLogEntry{
			Position:      3,
			Type:          ClusterLogEntryServiceMessage,
			SessionID:     4,
			CorrelationID: 5,
			TimerID:       6,
			Deadline:      now,
			SourceService: "source",
			TargetService: "target",
			Payload:       []byte("entry"),
		},
		Entries: []ClusterLogEntry{{
			Position:      7,
			Type:          ClusterLogEntryIngress,
			SessionID:     8,
			CorrelationID: 9,
			Payload:       []byte("history"),
		}},
		Position: 10,
		Snapshot: ClusterStateSnapshot{
			NodeID:   11,
			Role:     ClusterRoleFollower,
			Position: 12,
			TakenAt:  now,
			Payload:  []byte("state"),
			Timers: []ClusterTimer{{
				TimerID:  13,
				Deadline: now,
				Payload:  []byte("timer"),
			}},
		},
		HasSnapshot: true,
		Ingress: ClusterIngress{
			SessionID:     14,
			CorrelationID: 15,
			Payload:       []byte("ingress"),
		},
		Ingresses: []ClusterIngress{{
			SessionID:     37,
			CorrelationID: 38,
			Payload:       []byte("batch-ingress"),
		}},
		Egress: ClusterEgress{
			SessionID:     16,
			CorrelationID: 17,
			LogPosition:   18,
			Type:          ClusterLogEntryIngress,
			TimerID:       19,
			Deadline:      now,
			SourceService: "egress-source",
			TargetService: "egress-target",
			Payload:       []byte("egress"),
		},
		Egresses: []ClusterEgress{{
			SessionID:     39,
			CorrelationID: 40,
			LogPosition:   41,
			Type:          ClusterLogEntryIngress,
			Payload:       []byte("batch-egress"),
		}},
		Description: ClusterDescription{
			NodeID:           20,
			Mode:             ClusterModeAppointedLeader,
			Role:             ClusterRoleLeader,
			LastPosition:     21,
			SnapshotPosition: 22,
			Closed:           true,
			Suspended:        true,
			Learner: ClusterLearnerStatus{
				Enabled:              true,
				MasterPosition:       23,
				SyncedPosition:       24,
				Snapshots:            25,
				AppliedSinceSnapshot: 26,
				LastSync:             now,
				LastError:            "learner",
			},
			Replication: ClusterReplicationStatus{
				Enabled:        true,
				SourcePosition: 27,
				SyncedPosition: 28,
				Applied:        29,
				LastSync:       now,
				LastError:      "replication",
			},
			Election: ClusterElectionStatus{
				Enabled:      true,
				LeaderID:     30,
				Term:         31,
				Role:         ClusterRoleLeader,
				LastElection: now,
				LastError:    "election",
				Members: []ClusterElectionMemberStatus{{
					NodeID:        32,
					LastHeartbeat: now,
					Alive:         true,
				}},
			},
			Quorum: ClusterQuorumStatus{
				Enabled:     true,
				Members:     33,
				RequiredAck: 34,
				LastCommit:  35,
				LastAcks:    36,
				LastError:   "quorum",
				LastAt:      now,
			},
		},
	}

	var buf bytes.Buffer
	if err := writeClusterMemberProtocolMessage(&buf, input); err != nil {
		t.Fatal(err)
	}
	got, err := readClusterMemberProtocolMessage(bufio.NewReader(&buf))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("protobuf round trip mismatch:\ngot:  %#v\nwant: %#v", got, input)
	}
}
