//! Single-disk WAL append + fsync benchmark (P2 W7).
//!
//! Measures sequential append throughput through the lock-free enqueue +
//! dedicated writer batch flush path on an in-memory `BlockDevice`.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use crowkv::paxos::roles::{PxBallot, PxLogEntry, PxLogEntryKind};
use crowkv::wal::record::WALRecord;
use crowkv::wal::wal_engine::WalEngine;
use crowkv::wal::{BlockDevice, IoBackend, WalConfig};

fn make_record(group: u64, slot: u64) -> WALRecord {
    let entry = PxLogEntry {
        slot,
        ballot: PxBallot::new(0, 1),
        term: 1,
        kind: PxLogEntryKind::Write,
        payload: Bytes::from(format!("bench-val-{slot}")),
        client_id: Some(7),
        seq: Some(slot),
    };
    WALRecord::from_accepted(group, &entry)
}

fn bench_sequential_append(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("wal_append_single_disk", |b| {
        b.iter_batched(
            || {
                let device = BlockDevice::new();
                let backend = Arc::new(IoBackend::BlockDevice(device));
                let config = WalConfig {
                    wal_disks: vec![PathBuf::from("/bench-wal")],
                    wal_segment_size: 8 * 1024 * 1024,
                    ..Default::default()
                };
                rt.block_on(async { WalEngine::create(backend, config, 1).await.unwrap() })
            },
            |wal| {
                rt.block_on(async {
                    for slot in 1..=100u64 {
                        wal.append(&make_record(1, slot)).await.unwrap();
                    }
                });
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_sequential_append);
criterion_main!(benches);
