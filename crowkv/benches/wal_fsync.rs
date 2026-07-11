//! Single-disk WAL fsync benchmark (P2 W7).

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_single_disk_fsync(c: &mut Criterion) {
    // TODO: benchmark fsync throughput + p50/p99 latency
    c.bench_function("wal_fsync_placeholder", |b| {
        b.iter(|| {
            std::hint::black_box(42);
        });
    });
}

criterion_group!(benches, bench_single_disk_fsync);
criterion_main!(benches);
