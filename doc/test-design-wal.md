# CrowKV - Test Design: WAL

Depends on: [`test-design.md`](test-design.md), [`design-wal.md`](design-wal.md)
Satisfies: [requirement.md §8.1](requirement.md#81-wal-write-ahead-log), [requirement.md §8.2](requirement.md#82-acceptor)

Invariants and test strategy for the write-ahead log.

## 1. Invariants

| ID | Claim | Trigger | Ref |
|---|---|---|---|
| W1 | Ack only after fsync | `Accepted` response sent | [`design-wal.md`](design-wal.md) §5.1 |
| W2 | Replay deterministic | Startup segment walk | [`design-wal.md`](design-wal.md) §6 |
| W3 | CRC failure truncates local | Bad CRC during replay | [`design-wal.md`](design-wal.md) §6.2 |
| W4 | GC only below both watermarks | Segment unlink | [`design-wal.md`](design-wal.md) §7 |
| W5 | Multi-disk parallel fsync | Aggregate IOPS measurement | [`design-wal.md`](design-wal.md) §3 |
| W6 | Disk loss → fail-out, not partial | fsync error | [`design-wal.md`](design-wal.md) §8.1 |

## 2. Unit Tests

| Module | Test | Assertion |
|---|---|---|
| `segment` | `write_read_roundtrip` | Record survives close/reopen |
| `segment` | `crc_failure_truncate` | Replay stops at bad CRC, later segments still read |
| `fsync_worker` | `batch_coalesce` | 100 records fsynced in ≤ 3 calls |
| `manager` | `multi_disk_round_robin` | Slots distributed across disks |
| `replay` | `kill9_recovery` | State after restart = state before un-fsynced batch |

## 3. Failure Injection

| Failure | Sim | Invariant | Assertion |
|---|---|---|---|
| Crash before fsync | `TestDisk::crash_before_fsync()` | W1 | Record lost; no client was acked |
| Crash after fsync, before `Accepted` sent | `TestDisk::crash_after_fsync()` | W2 | Replay recovers; node rejoins, included in quorum |
| CRC corruption mid-segment | `TestDisk::corrupt_at_offset(bytes)` | W3 | Replay truncates; later segments preserved |
| Disk full | `TestDisk::set_full()` | W6 | Acceptor stops fsync; leader excludes from quorum |
| Disk error (EIO) | `TestDisk::inject_io_error()` | W6 | Node fails out of group; rebuilds via snapshot install (P5) |

## 4. Integration Scenarios

**S-W1 — 1000-record crash recovery:**
1. Write 1000 `Accept` records across 3 disks.
2. Inject crash with last batch unfsynced.
3. Restart, replay, assert acceptor state matches expected.

**S-W2 — Multi-disk throughput:**
1. Saturate writes with 1, 2, 4 disks.
2. Assert IOPS scales (sub-linear acceptable; document slope).

**S-W3 — CRC corruption survival:**
1. Write 100 records, manually corrupt one record's CRC.
2. Replay, assert truncation at corrupted record, records before are recovered, records after in same segment lost.

## 5. Resolved Decisions

- **Test directories:** per-test `tempfile`-managed directories for integration tests against the real `tokio-uring` / fallback backend; the simulated `SimDisk` backend is used for unit tests (per [`design-async-io.md`](design-async-io.md) §10).
- **Disk error injection:** `LD_PRELOAD` (libfiu-style) for end-to-end fault tests against the real backend; `SimDisk::inject_io_error()` for unit tests.
