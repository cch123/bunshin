package core

import (
	"context"
	"sync"
	"testing"
	"time"
)

const clusterBenchmarkPayloadBytes = 256

type clusterBenchmarkNoopService struct{}

func (clusterBenchmarkNoopService) OnClusterMessage(context.Context, ClusterMessage) ([]byte, error) {
	return nil, nil
}

func BenchmarkClusterIngress(b *testing.B) {
	b.Run("single_node", func(b *testing.B) {
		benchmarkClusterClientSend(b, ClusterConfig{})
	})
	b.Run("quorum_in_memory", func(b *testing.B) {
		benchmarkClusterClientSend(b, ClusterConfig{
			NodeID:            1,
			Mode:              ClusterModeAppointedLeader,
			AppointedLeaderID: 1,
			Quorum: &ClusterQuorumConfig{
				MemberLogs: []ClusterLog{
					NewInMemoryClusterLog(),
					NewInMemoryClusterLog(),
				},
			},
		})
	})
	b.Run("remote_quic", benchmarkClusterRemoteIngress)
	b.Run("remote_quorum_quic", benchmarkClusterRemoteQuorumIngress)
}

func BenchmarkClusterBatchIngress(b *testing.B) {
	for _, batchSize := range []int{8, 32, 128, 256} {
		b.Run("remote_quorum_quic/batch_"+itoaBenchmark(batchSize), func(b *testing.B) {
			benchmarkClusterRemoteQuorumBatchIngress(b, batchSize)
		})
	}
}

func BenchmarkClusterAutoBatchIngress(b *testing.B) {
	b.Run("remote_quorum_quic/default_128_50us", benchmarkClusterRemoteQuorumAutoBatchIngress)
}

func BenchmarkClusterReplicationCatchUp(b *testing.B) {
	b.Run("in_memory", func(b *testing.B) {
		ctx := context.Background()
		payload := make([]byte, clusterBenchmarkPayloadBytes)
		leaderLog := NewInMemoryClusterLog()
		appendBenchmarkClusterEntries(b, leaderLog, b.N, payload)

		follower, err := StartClusterNode(ctx, ClusterConfig{
			NodeID:            2,
			Mode:              ClusterModeAppointedLeader,
			AppointedLeaderID: 1,
			DisableTimerLoop:  true,
			Log:               NewInMemoryClusterLog(),
			Service:           clusterBenchmarkNoopService{},
			Replication: &ClusterReplicationConfig{
				SourceLog:      leaderLog,
				DisableAutoRun: true,
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		defer follower.Close(ctx)

		b.SetBytes(int64(len(payload)))
		b.ReportAllocs()
		b.ResetTimer()
		start := time.Now()
		result, err := follower.SyncReplication(ctx)
		elapsed := time.Since(start)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if result.Applied != uint64(b.N) {
			b.Fatalf("applied = %d, want %d", result.Applied, b.N)
		}
		reportClusterBenchmarkThroughput(b, elapsed)
	})
	b.Run("remote_quic", func(b *testing.B) {
		ctx := context.Background()
		payload := make([]byte, clusterBenchmarkPayloadBytes)
		leaderLog := NewInMemoryClusterLog()
		leader, err := StartClusterNode(ctx, ClusterConfig{
			NodeID:            1,
			Mode:              ClusterModeAppointedLeader,
			AppointedLeaderID: 1,
			DisableTimerLoop:  true,
			Log:               leaderLog,
			Service:           clusterBenchmarkNoopService{},
		})
		if err != nil {
			b.Fatal(err)
		}
		defer leader.Close(ctx)
		appendBenchmarkClusterEntries(b, leaderLog, b.N, payload)
		server, remoteLeader := startClusterMemberTransportForBenchmark(b, leader)
		defer remoteLeader.Close()
		defer server.Close()

		follower, err := StartClusterNode(ctx, ClusterConfig{
			NodeID:            2,
			Mode:              ClusterModeAppointedLeader,
			AppointedLeaderID: 1,
			DisableTimerLoop:  true,
			Log:               NewInMemoryClusterLog(),
			Service:           clusterBenchmarkNoopService{},
			Replication: &ClusterReplicationConfig{
				SourceLog:      remoteLeader,
				DisableAutoRun: true,
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		defer follower.Close(ctx)

		b.SetBytes(int64(len(payload)))
		b.ReportAllocs()
		b.ResetTimer()
		start := time.Now()
		result, err := follower.SyncReplication(ctx)
		elapsed := time.Since(start)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if result.Applied != uint64(b.N) {
			b.Fatalf("applied = %d, want %d", result.Applied, b.N)
		}
		reportClusterBenchmarkThroughput(b, elapsed)
	})
}

func BenchmarkClusterIngressLatency(b *testing.B) {
	b.Run("single_node", func(b *testing.B) {
		ctx := context.Background()
		node, err := StartClusterNode(ctx, ClusterConfig{
			DisableTimerLoop: true,
			Service:          clusterBenchmarkNoopService{},
		})
		if err != nil {
			b.Fatal(err)
		}
		defer node.Close(ctx)
		client, err := node.NewClient(ctx)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkClusterLatency(b, func(ctx context.Context, payload []byte, _ int) error {
			_, err := client.Send(ctx, payload)
			return err
		})
	})
	b.Run("remote_quic", func(b *testing.B) {
		ctx := context.Background()
		node, err := StartClusterNode(ctx, ClusterConfig{
			DisableTimerLoop: true,
			Service:          clusterBenchmarkNoopService{},
		})
		if err != nil {
			b.Fatal(err)
		}
		defer node.Close(ctx)
		server, remote := startClusterMemberTransportForBenchmark(b, node)
		defer remote.Close()
		defer server.Close()
		benchmarkClusterLatency(b, func(ctx context.Context, payload []byte, i int) error {
			_, err := remote.Submit(ctx, ClusterIngress{
				SessionID:     1,
				CorrelationID: ClusterCorrelationID(i + 1),
				Payload:       payload,
			})
			return err
		})
	})
	b.Run("remote_quorum_quic", func(b *testing.B) {
		ctx := context.Background()
		follower, err := StartClusterNode(ctx, ClusterConfig{
			NodeID:            2,
			Mode:              ClusterModeAppointedLeader,
			AppointedLeaderID: 1,
			DisableTimerLoop:  true,
			Service:           clusterBenchmarkNoopService{},
		})
		if err != nil {
			b.Fatal(err)
		}
		defer follower.Close(ctx)
		followerServer, remoteFollower := startClusterMemberTransportForBenchmark(b, follower)
		defer remoteFollower.Close()
		defer followerServer.Close()

		leader, err := StartClusterNode(ctx, ClusterConfig{
			NodeID:            1,
			Mode:              ClusterModeAppointedLeader,
			AppointedLeaderID: 1,
			DisableTimerLoop:  true,
			Service:           clusterBenchmarkNoopService{},
			Quorum: &ClusterQuorumConfig{
				MemberLogs:  []ClusterLog{remoteFollower},
				RequiredAck: 2,
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		defer leader.Close(ctx)
		client, err := leader.NewClient(ctx)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkClusterLatency(b, func(ctx context.Context, payload []byte, _ int) error {
			_, err := client.Send(ctx, payload)
			return err
		})
	})
}

func benchmarkClusterClientSend(b *testing.B, cfg ClusterConfig) {
	b.Helper()
	ctx := context.Background()
	cfg.DisableTimerLoop = true
	if cfg.Service == nil {
		cfg.Service = clusterBenchmarkNoopService{}
	}
	node, err := StartClusterNode(ctx, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer node.Close(ctx)
	client, err := node.NewClient(ctx)
	if err != nil {
		b.Fatal(err)
	}

	payload := make([]byte, clusterBenchmarkPayloadBytes)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.SetParallelism(8)
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		if _, err := client.Send(ctx, payload); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()
	reportClusterBenchmarkThroughput(b, elapsed)
}

func benchmarkClusterRemoteIngress(b *testing.B) {
	ctx := context.Background()
	node, err := StartClusterNode(ctx, ClusterConfig{
		DisableTimerLoop: true,
		Service:          clusterBenchmarkNoopService{},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer node.Close(ctx)
	server, remote := startClusterMemberTransportForBenchmark(b, node)
	defer remote.Close()
	defer server.Close()

	payload := make([]byte, clusterBenchmarkPayloadBytes)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		if _, err := remote.Submit(ctx, ClusterIngress{
			SessionID:     1,
			CorrelationID: ClusterCorrelationID(i + 1),
			Payload:       payload,
		}); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()
	reportClusterBenchmarkThroughput(b, elapsed)
}

func benchmarkClusterRemoteQuorumIngress(b *testing.B) {
	ctx := context.Background()
	follower, err := StartClusterNode(ctx, ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Service:           clusterBenchmarkNoopService{},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer follower.Close(ctx)
	followerServer, remoteFollower := startClusterMemberTransportForBenchmark(b, follower)
	defer remoteFollower.Close()
	defer followerServer.Close()

	leader, err := StartClusterNode(ctx, ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Service:           clusterBenchmarkNoopService{},
		Quorum: &ClusterQuorumConfig{
			MemberLogs:  []ClusterLog{remoteFollower},
			RequiredAck: 2,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer leader.Close(ctx)
	client, err := leader.NewClient(ctx)
	if err != nil {
		b.Fatal(err)
	}

	payload := make([]byte, clusterBenchmarkPayloadBytes)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		if _, err := client.Send(ctx, payload); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()
	reportClusterBenchmarkThroughput(b, elapsed)
}

func benchmarkClusterRemoteQuorumBatchIngress(b *testing.B, batchSize int) {
	ctx := context.Background()
	follower, err := StartClusterNode(ctx, ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Service:           clusterBenchmarkNoopService{},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer follower.Close(ctx)
	followerServer, remoteFollower := startClusterMemberTransportForBenchmark(b, follower)
	defer remoteFollower.Close()
	defer followerServer.Close()

	leader, err := StartClusterNode(ctx, ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Service:           clusterBenchmarkNoopService{},
		Quorum: &ClusterQuorumConfig{
			MemberLogs:  []ClusterLog{remoteFollower},
			RequiredAck: 2,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer leader.Close(ctx)
	client, err := leader.NewClient(ctx)
	if err != nil {
		b.Fatal(err)
	}

	payloads := make([][]byte, batchSize)
	for i := range payloads {
		payloads[i] = make([]byte, clusterBenchmarkPayloadBytes)
	}
	b.SetBytes(int64(batchSize * clusterBenchmarkPayloadBytes))
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	var messages int
	for i := 0; i < b.N; i++ {
		egresses, err := client.SendBatch(ctx, payloads)
		if err != nil {
			b.Fatal(err)
		}
		messages += len(egresses)
	}
	elapsed := time.Since(start)
	b.StopTimer()
	reportClusterMessageBenchmarkThroughput(b, messages, elapsed)
}

func benchmarkClusterRemoteQuorumAutoBatchIngress(b *testing.B) {
	ctx := context.Background()
	follower, err := StartClusterNode(ctx, ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Service:           clusterBenchmarkNoopService{},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer follower.Close(ctx)
	followerServer, remoteFollower := startClusterMemberTransportForBenchmark(b, follower)
	defer remoteFollower.Close()
	defer followerServer.Close()

	leader, err := StartClusterNode(ctx, ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Service:           clusterBenchmarkNoopService{},
		Quorum: &ClusterQuorumConfig{
			MemberLogs:  []ClusterLog{remoteFollower},
			RequiredAck: 2,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer leader.Close(ctx)
	client, err := leader.NewClientWithConfig(ctx, DefaultClusterClientConfig())
	if err != nil {
		b.Fatal(err)
	}

	payload := make([]byte, clusterBenchmarkPayloadBytes)
	var sendErr error
	var sendErrOnce sync.Once
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := client.Send(ctx, payload); err != nil {
				sendErrOnce.Do(func() {
					sendErr = err
				})
				return
			}
		}
	})
	elapsed := time.Since(start)
	b.StopTimer()
	if sendErr != nil {
		b.Fatal(sendErr)
	}
	reportClusterBenchmarkThroughput(b, elapsed)
}

func benchmarkClusterLatency(b *testing.B, submit func(context.Context, []byte, int) error) {
	b.Helper()
	ctx := context.Background()
	payload := make([]byte, clusterBenchmarkPayloadBytes)
	latencies := make([]time.Duration, 0, b.N)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	started := time.Now()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if err := submit(ctx, payload, i); err != nil {
			b.Fatal(err)
		}
		latencies = append(latencies, time.Since(start))
	}
	elapsed := time.Since(started)
	b.StopTimer()
	reportClusterBenchmarkThroughput(b, elapsed)
	reportLatencyBenchmarkMetrics(b, latencies)
}

func appendBenchmarkClusterEntries(b *testing.B, log ClusterLog, entries int, payload []byte) {
	b.Helper()
	ctx := context.Background()
	for i := 0; i < entries; i++ {
		if _, err := log.Append(ctx, ClusterLogEntry{
			Type:          ClusterLogEntryIngress,
			SessionID:     1,
			CorrelationID: ClusterCorrelationID(i + 1),
			Payload:       payload,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func startClusterMemberTransportForBenchmark(b *testing.B, node *ClusterNode) (*ClusterMemberTransportServer, *ClusterMemberClient) {
	b.Helper()
	server, err := ListenClusterMemberTransport(context.Background(), node, ClusterMemberTransportConfig{})
	if err != nil {
		b.Fatal(err)
	}
	client, err := DialClusterMember(context.Background(), ClusterMemberClientConfig{Addr: server.Addr().String()})
	if err != nil {
		_ = server.Close()
		b.Fatal(err)
	}
	return server, client
}

func reportClusterBenchmarkThroughput(b *testing.B, elapsed time.Duration) {
	b.Helper()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msg/s")
	}
}

func reportClusterMessageBenchmarkThroughput(b *testing.B, messages int, elapsed time.Duration) {
	b.Helper()
	if elapsed > 0 && messages > 0 {
		b.ReportMetric(float64(messages)/elapsed.Seconds(), "msg/s")
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(messages), "ns/msg")
	}
}

func itoaBenchmark(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	i := len(digits)
	for value > 0 {
		i--
		digits[i] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[i:])
}
