# WAL Benchmark History

## Benchmark Design

- **What**: Multi-threaded WAL append throughput with durable flush ack.
- **Payload**: 1 KB per record (fixed).
- **Backends**: `mem` (in-memory `BlockDevice`), `file` (real disk `fdatasync`).
- **Thread counts**: 1, 32, 128 concurrent loader tasks.
- **Duration**: 5 seconds per case (time-based, not fixed record count).
- **Metrics**: records, TPS, per-record avg latency, per-batch avg latency, avg batch size.
- **No Criterion**: Custom `fn main()` with `std::thread::sleep` timing and `AtomicBool` stop flag.

### How to run

```sh
# All cases
cargo bench --bench wal

# Single case (exact name match)
cargo bench --bench wal -- mem_1
cargo bench --bench wal -- file_128
```

---

## Run 1 — 2025-06-28

### Hardware / OS

| Item | Value |
| --- | --- |
| CPU | Apple M5 Pro |
| P-cores | 6 (6 threads) |
| E-cores | 12 (12 threads) |
| Total logical CPUs | 18 |
| P-core L2 cache | 16 MB (shared per 6 cores) |
| E-core L2 cache | 8 MB (shared per 6 cores) |
| L1 data cache | 128 KB (P-core) / 64 KB (E-core) |
| RAM | 64 GB |
| Disk | APPLE SSD AP1024Z (1 TB, APFS, internal Apple Fabric SSD) |
| OS | macOS 26.5.1 (Build 25F80) |

### Results

| case | records | TPS | lat_us | batch_lat_us | avg_batch |
| --- | ---: | ---: | ---: | ---: | ---: |
| mem_1 | 2,775,166 | 554,592 | 1.8 | 1.8 | 1.0 |
| mem_32 | 3,382,956 | 676,162 | 1.5 | 19.3 | 13.0 |
| mem_128 | 3,506,686 | 700,661 | 1.4 | 84.3 | 59.1 |
| file_1 | 1,641 | 328 | 3,050.1 | 3,050.1 | 1.0 |
| file_32 | 26,441 | 5,284 | 189.3 | 3,032.9 | 16.0 |
| file_128 | 98,528 | 19,656 | 50.9 | 3,050.9 | 60.0 |

### Notes

- **mem backend**: Single-threaded achieves 555K TPS. 128 threads only 26% faster (701K) — pipeline writer (single-threaded) is the bottleneck, not the loaders. Batch aggregation scales well (1 → 59 records/batch) but writer throughput is capped.
- **file backend**: `fdatasync` on macOS maps to `F_FULLFSYNC` (forces disk internal cache flush), costing ~3 ms per flush regardless of batch size. This dominates latency. On Linux, `fdatasync` only flushes OS page cache and should be significantly faster.
- **Batching benefit (file)**: 128 threads achieves 60× higher TPS than single-thread (19,656 vs 328) purely through batch aggregation — 60 records per `fdatasync` instead of 1.
- **Per-record latency**: `lat_us = batch_lat_us / avg_batch`. For file_128: 3,050 µs / 60 = 50.9 µs per record.
