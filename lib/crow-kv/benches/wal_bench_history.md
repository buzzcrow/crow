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
| CPU cores | Apple M5 Pro, 18 total logical CPUs |
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

### Important conclusions

- **macOS file durability is expensive**: file-backed batch flush latency stays around **3.05 ms**, which is the dominant bottleneck for the file backend.
- **Batching is essential for file throughput**: `file_128` reaches **19,656 TPS** vs **328 TPS** for `file_1` by batching about **60** records per durable flush.
- **The mem backend is limited by the single pipeline writer**: increasing concurrency improves throughput, but much less dramatically than the file backend.

---

## Run 2 — 2026-06-29

### Hardware / OS

| Item | Value |
| --- | --- |
| CPU cores | AMD Ryzen 9 5950X, 16 physical cores / 32 threads |
| Disk | `/dev/nvme1n1p2` (NVMe-backed root filesystem) |
| OS | Linux 6.8.0-124-generic x86_64 |

### Results

| case | records | TPS | lat_us | batch_lat_us | avg_batch |
| --- | ---: | ---: | ---: | ---: | ---: |
| mem_1 | 1,639,842 | 327,962 | 3.0 | 3.0 | 1.0 |
| mem_32 | 1,765,271 | 353,040 | 2.8 | 43.0 | 15.0 |
| mem_128 | 2,268,489 | 453,665 | 2.2 | 134.4 | 60.0 |
| file_1 | 8,685 | 1,737 | 575.7 | 575.7 | 1.0 |
| file_32 | 122,318 | 24,459 | 40.9 | 654.6 | 16.0 |
| file_128 | 417,002 | 83,368 | 12.0 | 731.6 | 60.0 |

### Comparison vs Run 1 (macOS)

| case | macOS TPS | Linux TPS | speedup |
| --- | ---: | ---: | ---: |
| mem_1 | 554,592 | 327,962 | 0.59x |
| mem_32 | 676,162 | 353,040 | 0.52x |
| mem_128 | 700,661 | 453,665 | 0.65x |
| file_1 | 328 | 1,737 | 5.30x |
| file_32 | 5,284 | 24,459 | 4.63x |
| file_128 | 19,656 | 83,368 | 4.24x |

### Important conclusions

- **macOS has much larger durable flush latency**: the earlier macOS run showed about **3.05 ms** batch flush latency on the file backend, while this Linux NVMe run stayed around **0.58-0.73 ms**. That is the main reason Linux file TPS is about **4.2x-5.3x** higher.
- **Batching is the main throughput multiplier for the file backend**: on Linux, `file_128` reached **83,368 TPS** vs **1,737 TPS** for `file_1`, driven by average batch size growing from **1** to **60** records.
- **The mem backend is not flush-limited, but still capped by the single pipeline writer**: adding concurrency improves throughput, but much less dramatically than the file backend.
