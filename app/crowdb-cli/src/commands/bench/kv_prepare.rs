// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `bench kv prepare` — pre-populate `--keys` keys into store 0 / group 0.
//!
//! Concurrent bulk-put loop with `--concurrency` tasks. No latency
//! histogram — this is a setup step, not a measurement. Reports total
//! keys written, elapsed time, and errors to stderr.

use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::kv_client::{build_kv_client, KvClientTunables};
use super::verb::PrepareArgs;
use crate::Cli;

/// Store 0 / group 0 — the default bench target.
const STORE_ID: u64 = 0;
const GROUP_ID: u64 = 0;

pub async fn run(cli: &Cli, args: PrepareArgs) -> ExitCode {
    let client = match build_kv_client(
        cli,
        crowdb_kv_client::ReadEndpointPolicy::Leader,
        &KvClientTunables::default(),
    ) {
        Ok(c) => Arc::new(c),
        Err(c) => return c,
    };

    let value_size = args.value_size.max(1);

    // Warm up: wait for the leader to be ready before bulk-putting.
    // local-deploy may return before the leader is fully stable.
    let warm_key = b"k__warmup__";
    let warm_val = vec![0u8; value_size];
    for _ in 0..50 {
        if client
            .put(STORE_ID, GROUP_ID, warm_key, &warm_val, None)
            .await
            .is_ok()
        {
            // Extra settle time: the leader may be elected but not yet
            // processing writes at full capacity.
            tokio::time::sleep(Duration::from_millis(200)).await;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let next_id = Arc::new(AtomicU64::new(0));
    let ok_count = Arc::new(AtomicU64::new(0));
    let err_count = Arc::new(AtomicU64::new(0));
    let total = args.keys;

    let concurrency = args.concurrency.max(1);
    let start = Instant::now();

    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let client = Arc::clone(&client);
        let next_id = Arc::clone(&next_id);
        let ok_count = Arc::clone(&ok_count);
        let err_count = Arc::clone(&err_count);
        handles.push(tokio::spawn(async move {
            loop {
                let id = next_id.fetch_add(1, Ordering::Relaxed);
                if id >= total {
                    break;
                }
                let key = format!("k{id:020}");
                let value = build_value(id, value_size);
                // The client's put() already retries 4 times, but the
                // leader may still be stabilizing. Add one extra retry
                // with a short backoff for the first few keys.
                let mut succeeded = false;
                let mut last_err = None;
                for attempt in 0..3u32 {
                    match client.put(STORE_ID, GROUP_ID, key.as_bytes(), &value, None).await {
                        Ok(_) => {
                            ok_count.fetch_add(1, Ordering::Relaxed);
                            succeeded = true;
                            break;
                        }
                        Err(e) => {
                            last_err = Some(e);
                            if attempt < 2 {
                                tokio::time::sleep(Duration::from_millis(50 * u64::from(attempt + 1))).await;
                            }
                        }
                    }
                }
                if !succeeded {
                    err_count.fetch_add(1, Ordering::Relaxed);
                    if err_count.load(Ordering::Relaxed) <= 3 {
                        if let Some(e) = &last_err {
                            eprintln!("prepare: put key={key} failed: {e}");
                        }
                    }
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let elapsed = start.elapsed();
    let ok = ok_count.load(Ordering::Relaxed);
    let err = err_count.load(Ordering::Relaxed);
    eprintln!(
        "bench kv prepare: {ok} keys written, {err} errors, {:.1}s",
        elapsed.as_secs_f64()
    );

    if err > 0 {
        ExitCode::from(2)
    } else {
        ExitCode::SUCCESS
    }
}

/// Build a deterministic value for key `id`: byte `i` = `(id + i) % 256`.
/// This pattern is reused by `bench kv read --verify-bytes` for
/// correctness verification.
fn build_value(id: u64, size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| u8::try_from((id + i as u64) % 256).unwrap_or(0))
        .collect()
}
