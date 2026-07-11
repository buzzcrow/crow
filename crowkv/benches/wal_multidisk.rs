//! Multi-disk WAL aggregate benchmark (P2 W9).

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_multi_disk_aggregate(c: &mut Criterion) {
    // TODO: benchmark aggregate throughput vs disk count
    c.bench_function("wal_multidisk_placeholder", |b| {
        b.iter(|| {
            std::hint::black_box(42);
        });
    });
}

criterion_group!(benches, bench_multi_disk_aggregate);
criterion_main!(benches);
