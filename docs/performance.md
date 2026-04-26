# Performance Notes

This document collects deployment and measurement guidance for low-latency Bunshin experiments. Treat these settings as workload-specific starting points, not universal defaults.

## Measurement

Use the benchmark commands in `docs/benchmarks.md` before changing runtime or operating-system settings. At minimum, capture:

- throughput and allocation benchmarks with `-benchmem`;
- p99 and p999 transport latency benchmarks;
- race-test results for code changes that touch shared state;
- CPU and memory profiles for driver, publication, subscription, archive, and cluster loops when investigating regressions.

Example profile command:

```sh
go test -run '^$' -bench BenchmarkPublicationSubscriptionSend -benchmem -cpuprofile cpu.out -memprofile mem.out ./...
```

The focused memory/GC benchmarks provide a lighter regression check when a full profile is unnecessary:

```sh
go test -run '^$' -bench 'Benchmark(PublicationSubscriptionMemoryProfile|ArchiveMemoryProfile|MediaDriverMemoryProfile)$' -benchmem ./...
```

## Linux Socket Tuning

For UDP-heavy tests, Linux socket defaults can cap throughput before Bunshin code is the bottleneck. Common settings to review:

- `net.core.rmem_max` and `net.core.wmem_max`: raise these before requesting larger socket receive/send buffers.
- `net.core.rmem_default` and `net.core.wmem_default`: consider raising defaults for dedicated benchmark hosts.
- `net.core.netdev_max_backlog`: raise when packet bursts are dropped before reaching sockets.
- `net.ipv4.udp_rmem_min` and `net.ipv4.udp_wmem_min`: verify minimum UDP buffer behavior on constrained systems.
- NIC ring sizes and offload settings: check with `ethtool -g` and `ethtool -k` during production tuning.

Keep kernel settings outside Bunshin defaults. Bunshin config should request socket buffer sizes where useful, while host provisioning owns system-wide sysctl and NIC policy.

## Runtime Pinning

For latency experiments, pinning can reduce scheduler noise but can also make tail latency worse if it starves runtime support goroutines.

Recommended evaluation order:

1. Set `GOMAXPROCS` explicitly for the benchmark host.
2. Run the default backoff idle strategy and capture p99/p999.
3. Try `YieldingIdleStrategy` or `BusySpinIdleStrategy` only on isolated CPUs.
4. If a loop owns an OS thread, use `runtime.LockOSThread` only in a narrow component and document how it is released.
5. On Linux, pair process or thread affinity tools with CPU isolation only after the unpinned baseline is stable.

Busy spinning is appropriate only for dedicated low-latency loops with clear CPU ownership. It should not be the default for general applications, CI, or shared hosts.

## Allocation Work

Current microbenchmarks expose the main allocation surfaces: frame encode/decode, transport send/receive, and handler dispatch. The QUIC receive path decodes frame payloads as views over the per-stream read buffer to avoid an extra copy; UDP keeps copy semantics because its datagram buffer is reused across reads. Buffer pooling should be added only where ownership is simple:

- fixed-size temporary frame buffers;
- short-lived reassembly buffers that are not retained by handlers;
- IPC command/event encoding buffers after JSON protocol shape stabilizes.

Do not pool application payloads handed to user handlers unless the API explicitly defines the lifetime and copy requirements.

Archive replication uses a pooled fixed-size copy buffer because the buffer is private to synchronous file-copy work and never crosses into user handlers.
