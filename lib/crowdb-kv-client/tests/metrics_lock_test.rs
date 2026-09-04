// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_kv_client::ClientMetrics;

#[test]
fn concurrent_latency_shards_preserve_every_sample() {
    let metrics = Arc::new(ClientMetrics::default());
    let threads: Vec<_> = (0_u64..16)
        .map(|thread| {
            let metrics = Arc::clone(&metrics);
            std::thread::spawn(move || {
                for sample in 0..1_000 {
                    metrics.record_latency_for_tests(thread * 1_000 + sample + 1);
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }

    let snapshot = metrics.drain_window();
    assert_eq!(snapshot.put.len(), 16_000);
    assert_eq!(metrics.drain_window().put.len(), 0);
}
