// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! diskdb-side watch/notify handler. Subscribes to group-0 prefixes
//! and wakes the keepalive sync loop on notify.

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::bg_task::StopHandle;
use crow_kv_client::WatchNotifyClient;
use crow_protocol::DISKDB_WATCH_PREFIXES;

/// Group-0 store + group ids (system group).
const G0_STORE: u64 = 0;
const G0_GROUP: u64 = 0;

/// diskdb-side watch/notify handler. Subscribes to group-0 prefixes
/// and wakes the keepalive sync loop on notify.
pub struct NotifyHandler {
    watch: WatchNotifyClient,
    keepalive_trigger: Arc<tokio::sync::Notify>,
}

impl NotifyHandler {
    /// Create a new notify handler. The `keepalive_trigger` is the
    /// same `Arc<Notify>` passed to `KeepAlive::with_sync_trigger`.
    #[must_use]
    pub fn new(watch: WatchNotifyClient, keepalive_trigger: Arc<tokio::sync::Notify>) -> Self {
        Self {
            watch,
            keepalive_trigger,
        }
    }

    /// Open subscriptions and loop on notify frames. Each frame wakes
    /// the keepalive via `keepalive_trigger.notify_one()`. Runs until
    /// the stop signal.
    pub async fn run(self, stop: StopHandle) {
        let prefixes: &[&[u8]] = DISKDB_WATCH_PREFIXES;

        // Keep subscriptions alive for the lifetime of the handler.
        // Dropping a `WatchSubscription` aborts its reader task, so
        // we hold them in a Vec that lives until `run` returns.
        let mut subscriptions = Vec::new();
        for prefix in prefixes {
            match self.watch.subscribe(G0_STORE, G0_GROUP, prefix) {
                Ok(sub) => {
                    info!(prefix = ?prefix, "notify handler subscribed");
                    subscriptions.push(sub);
                }
                Err(e) => {
                    warn!(prefix = ?prefix, error = %e, "notify handler subscribe failed");
                }
            }
        }

        if subscriptions.is_empty() {
            warn!("notify handler: no subscriptions established, exiting");
            return;
        }

        // Merge all subscription receivers into one stream. We move
        // the receivers out by replacing them with dummy receivers
        // (the subscriptions themselves stay alive — only their
        // `notify_rx` is moved into the merge tasks). The merge-task
        // join handles are tracked so they can be aborted on stop
        // (otherwise they linger until the crow-rpc streams close).
        let (merge_tx, mut merge_rx) = mpsc::channel::<()>(64);
        let mut merge_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        for mut sub in subscriptions {
            let rx = std::mem::replace(&mut sub.notify_rx, mpsc::channel(1).1);
            let tx = merge_tx.clone();
            merge_handles.push(tokio::spawn(async move {
                let mut rx = rx;
                while let Some(_notify) = rx.recv().await {
                    if tx.send(()).await.is_err() {
                        break;
                    }
                }
                // `sub` is held in this task; when the task exits
                // (subscriber dropped or stream closed), `sub` drops
                // and aborts the reader.
                drop(sub);
            }));
        }
        drop(merge_tx);

        loop {
            tokio::select! {
                () = stop.notified() => {
                    info!("notify handler stopping");
                    // Abort the per-subscription merge tasks so they
                    // don't linger after the handler stops; dropping
                    // each `WatchSubscription` (held inside the task)
                    // aborts its reader task in turn.
                    for handle in merge_handles {
                        handle.abort();
                    }
                    return;
                }
                recv = merge_rx.recv() => {
                    if recv.is_some() {
                        self.keepalive_trigger.notify_one();
                    } else {
                        // All merge senders dropped (subscriptions
                        // closed); exit.
                        return;
                    }
                }
            }
        }
    }
}
