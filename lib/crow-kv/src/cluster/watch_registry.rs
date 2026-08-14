// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Per-group watch registry for the watch/notify extension. Wired
//! into `PxLearner` via `set_watch_registry`; the learner's
//! `apply_entry` calls `emit` after each successful engine apply,
//! gated by `has_watchers`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::kv::BatchOp;
use crate::rpc::{WatchNotify, WatchNotifyResponse};

/// One watcher: an outbound channel to push notify frames.
pub struct Watcher {
    pub tx: mpsc::Sender<Result<WatchNotifyResponse, tonic::Status>>,
}

/// Per-group watch registry. Wired into `PxLearner` via
/// `set_watch_registry`; the learner's `apply_entry` calls `emit`
/// after each successful engine apply, gated by `has_watchers`.
pub struct WatchRegistry {
    /// prefix -> list of watchers. `DashMap` for concurrent subscribe/
    /// unsubscribe/emit. Each watcher has a unique `watcher_id` so a
    /// stream can remove its own watchers on disconnect.
    watchers: DashMap<Vec<u8>, Vec<(u64, Watcher)>>,
    next_id: AtomicU64,
    /// Atomic fast-path flag: true iff at least one watcher is
    /// registered. The apply path checks this (one Acquire load)
    /// before touching the `DashMap` — zero overhead when no watchers.
    has_watchers: AtomicBool,
}

impl Default for WatchRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            watchers: DashMap::new(),
            next_id: AtomicU64::new(1),
            has_watchers: AtomicBool::new(false),
        }
    }

    /// Register a watcher for `prefix`. Returns the `watcher_id` for
    /// later removal. Sets `has_watchers = true`.
    pub fn subscribe(
        &self,
        prefix: &[u8],
        tx: mpsc::Sender<Result<WatchNotifyResponse, tonic::Status>>,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut entry = self.watchers.entry(prefix.to_vec()).or_default();
        entry.push((id, Watcher { tx }));
        self.has_watchers.store(true, Ordering::Release);
        id
    }

    /// Remove a specific watcher by `(prefix, watcher_id)`. Updates
    /// `has_watchers` if the registry becomes empty.
    pub fn unsubscribe(&self, prefix: &[u8], watcher_id: u64) {
        let mut should_check = false;
        if let Some(mut entry) = self.watchers.get_mut(prefix) {
            entry.retain(|(id, _)| *id != watcher_id);
            if entry.is_empty() {
                drop(entry);
                self.watchers.remove(prefix);
                should_check = true;
            }
        }
        if should_check {
            self.recompute_has_watchers();
        }
    }

    /// Remove all watchers whose `watcher_id` is in the list (stream-
    /// end cleanup). Updates `has_watchers`.
    pub fn remove_all(&self, watcher_ids: &[u64]) {
        let id_set: HashSet<u64> = watcher_ids.iter().copied().collect();
        let prefixes_to_remove: Vec<Vec<u8>> = self
            .watchers
            .iter_mut()
            .filter_map(|mut entry| {
                entry.retain(|(id, _)| !id_set.contains(id));
                if entry.is_empty() {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect();
        for p in prefixes_to_remove {
            self.watchers.remove(&p);
        }
        self.recompute_has_watchers();
    }

    /// Clear all watchers (leader step-down). Drops all tx senders,
    /// closing client streams. Sets `has_watchers = false`.
    pub fn clear(&self) {
        self.watchers.clear();
        self.has_watchers.store(false, Ordering::Release);
    }

    /// For a set of changed keys (from `Batch::decode`), find matching
    /// prefixes and enqueue notify frames. Called by the coalescer or
    /// directly (debounce=0). Non-blocking: uses `try_send`.
    pub fn emit(&self, group_id: u64, slot: u64, changed: &[BatchOp]) {
        // Group changed keys by matching prefix.
        let mut prefix_keys: HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();
        for op in changed {
            for entry in &self.watchers {
                let prefix = entry.key();
                if op.key.starts_with(prefix) {
                    prefix_keys
                        .entry(prefix.clone())
                        .or_default()
                        .push(op.key.to_vec());
                }
            }
        }
        for (prefix, keys) in prefix_keys {
            let notify = WatchNotify {
                group_id,
                prefix,
                keys,
                slot,
            };
            if let Some(entry) = self.watchers.get(&notify.prefix) {
                for (_, watcher) in entry.iter() {
                    let _ = watcher.tx.try_send(Ok(WatchNotifyResponse {
                        frame: Some(crate::rpc::watch_notify_response::Frame::Notify(notify.clone())),
                    }));
                }
            }
        }
    }

    /// True if at least one watcher is registered. Atomic load — the
    /// apply-path fast path.
    pub fn has_watchers(&self) -> bool {
        self.has_watchers.load(Ordering::Acquire)
    }

    /// Recompute `has_watchers` by scanning the `DashMap`. Called after
    /// removals.
    fn recompute_has_watchers(&self) {
        let empty = self.watchers.is_empty();
        self.has_watchers.store(!empty, Ordering::Release);
    }
}
