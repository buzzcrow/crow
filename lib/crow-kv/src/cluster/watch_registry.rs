// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version  2.0.

//! Per-group watch registry for the watch/notify extension. Wired
//! into `PxLearner` via `set_watch_registry`; the learner's
//! `apply_entry` calls `emit` after each successful engine apply,
//! gated by `has_watchers`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use flatbuffers::FlatBufferBuilder;
use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::kv::{BatchOp, Op};
use crate::rpc::{WatchNotify, WatchNotifyResponse};

/// crow-rpc push target for `WatchNotify` (R117). The server pushes
/// `FBWatchNotify` frames fire-and-forget via `RpcClient::send` on the
/// captured connection. On `ConnectionClosed`/`ConnectionError` the
/// watcher is lazily removed by `emit` (no connection-close callback
/// in crow-rpc).
pub struct CrowRpcPushTarget {
    /// The connection to the watching client (borrowed from the
    /// transport via `Connection::from_handle`).
    pub conn: crow_rpc_ffi::Connection,
    /// The client-side RPC client (for `send`).
    pub rpc: Arc<crow_rpc_ffi::RpcClient>,
    /// The server handle (for `send`).
    pub server: Arc<crow_rpc_ffi::RpcServer>,
    /// Next request ID for push frames.
    next_push_id: AtomicU64,
}

impl CrowRpcPushTarget {
    #[must_use]
    pub fn new(
        conn: crow_rpc_ffi::Connection,
        rpc: Arc<crow_rpc_ffi::RpcClient>,
        server: Arc<crow_rpc_ffi::RpcServer>,
    ) -> Self {
        Self {
            conn,
            rpc,
            server,
            next_push_id: AtomicU64::new(1),
        }
    }

    fn next_id(&self) -> u64 {
        self.next_push_id.fetch_add(1, Ordering::Relaxed)
    }
}

/// One watcher: an outbound push target. Tonic watchers use an mpsc
/// channel; crow-rpc watchers use a `CrowRpcPushTarget` (R117).
pub enum PushTarget {
    Tonic(mpsc::Sender<Result<WatchNotifyResponse, tonic::Status>>),
    CrowRpc(Arc<CrowRpcPushTarget>),
}

/// Byte-level prefix trie node. Each node holds the watchers
/// registered for the prefix that ends at this node (the path from
/// the root to this node is the prefix bytes). Children are keyed by
/// the next byte of the key.
struct TrieNode {
    watchers: Vec<(u64, PushTarget)>,
    children: HashMap<u8, TrieNode>,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            watchers: Vec::new(),
            children: HashMap::new(),
        }
    }
}

/// Prefix → (keys, values) accumulator for `emit`. Keys and values
/// borrow from the caller's `&[BatchOp]` for the duration of `emit`
/// (the trie read lock is held throughout); they are cloned into
/// owned `Vec<u8>` once per prefix when building the `WatchNotify`
/// frame, avoiding the per-(prefix,key) clone the owned accumulator
/// did on every matching node.
type PrefixMap<'a> = HashMap<Vec<u8>, (Vec<&'a [u8]>, Vec<&'a [u8]>)>;

/// Prefix trie: maps registered byte-prefixes to watcher lists.
/// `emit` walks each changed key through the trie (`O(key_len)` per
/// key), collecting watchers at every node whose prefix matches.
/// This replaces the former `O(watchers × keys)` `DashMap` scan.
struct PrefixTrie {
    root: TrieNode,
}

impl PrefixTrie {
    fn new() -> Self {
        Self {
            root: TrieNode::new(),
        }
    }

    /// Walk `key` through the trie. At each node that has watchers
    /// (including the root for the empty prefix), record `(prefix,
    /// key, value)` into `out`. The prefix is the bytes consumed so
    /// far on the path to that node. `key` and `value` are borrowed
    /// (no clone here); they are cloned once per prefix when the
    /// `WatchNotify` frame is built in `emit`.
    fn collect_matches<'a>(&self, key: &'a [u8], value: &'a [u8], out: &mut PrefixMap<'a>) {
        let mut node = &self.root;
        if !node.watchers.is_empty() {
            let entry = out.entry(Vec::new()).or_default();
            entry.0.push(key);
            entry.1.push(value);
        }
        for (i, &byte) in key.iter().enumerate() {
            match node.children.get(&byte) {
                Some(child) => {
                    node = child;
                    if !node.watchers.is_empty() {
                        let prefix = key[..=i].to_vec();
                        let entry = out.entry(prefix).or_default();
                        entry.0.push(key);
                        entry.1.push(value);
                    }
                }
                None => break,
            }
        }
    }

    /// Look up the watcher list for `prefix` (walk to the node at
    /// the end of `prefix`). Returns `None` if the path doesn't
    /// exist or has no watchers.
    fn watchers_for(&self, prefix: &[u8]) -> Option<&[(u64, PushTarget)]> {
        let mut node = &self.root;
        for &byte in prefix {
            node = node.children.get(&byte)?;
        }
        if node.watchers.is_empty() {
            None
        } else {
            Some(&node.watchers)
        }
    }
}

/// Per-group watch registry. Wired into `PxLearner` via
/// `set_watch_registry`; the learner's `apply_entry` calls `emit`
/// after each successful engine apply, gated by `has_watchers`.
pub struct WatchRegistry {
    trie: RwLock<PrefixTrie>,
    next_id: AtomicU64,
    /// Atomic fast-path flag: true iff at least one watcher is
    /// registered. The apply path checks this (one Acquire load)
    /// before touching the trie — zero overhead when no watchers.
    has_watchers: AtomicBool,
    /// Cumulative count of notify frames dropped because a watcher's
    /// channel was full (`try_send` returned `Full`). The client
    /// catches up via the safety-net poller, but a non-zero value
    /// indicates the watcher is consuming slower than the produce
    /// rate.
    dropped_notifies: AtomicU64,
    /// Cumulative count of `try_send` calls that returned `Closed`
    /// (the watcher's receiver was dropped). A non-zero value
    /// indicates a leaked watcher entry — stream-end cleanup should
    /// have removed it; a persistent `Closed` means cleanup missed
    /// it (e.g. a multi-group stream that only cleaned up the last
    /// group).
    closed_watchers: AtomicU64,
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
            trie: RwLock::new(PrefixTrie::new()),
            next_id: AtomicU64::new(1),
            has_watchers: AtomicBool::new(false),
            dropped_notifies: AtomicU64::new(0),
            closed_watchers: AtomicU64::new(0),
        }
    }

    /// Register a tonic watcher for `prefix`. Returns the `watcher_id`
    /// for later removal. Sets `has_watchers = true`.
    pub fn subscribe(
        &self,
        prefix: &[u8],
        tx: mpsc::Sender<Result<WatchNotifyResponse, tonic::Status>>,
    ) -> u64 {
        self.subscribe_with_target(prefix, PushTarget::Tonic(tx))
    }

    /// Register a crow-rpc watcher for `prefix` (R117). Returns the
    /// `watcher_id` for later removal. Sets `has_watchers = true`.
    pub fn subscribe_crow_rpc(&self, prefix: &[u8], target: Arc<CrowRpcPushTarget>) -> u64 {
        self.subscribe_with_target(prefix, PushTarget::CrowRpc(target))
    }

    /// Internal subscribe — shared by tonic + crow-rpc overloads.
    fn subscribe_with_target(&self, prefix: &[u8], target: PushTarget) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut trie = self.trie.write();
        let mut node = &mut trie.root;
        for &byte in prefix {
            node = node.children.entry(byte).or_insert_with(TrieNode::new);
        }
        node.watchers.push((id, target));
        self.has_watchers.store(true, Ordering::Release);
        id
    }

    /// Remove a specific watcher by `(prefix, watcher_id)`. Updates
    /// `has_watchers` if the registry becomes empty.
    pub fn unsubscribe(&self, prefix: &[u8], watcher_id: u64) {
        let should_check = {
            let mut trie = self.trie.write();
            let mut node = &mut trie.root;
            for &byte in prefix {
                match node.children.get_mut(&byte) {
                    Some(child) => node = child,
                    None => return,
                }
            }
            node.watchers.retain(|(id, _)| *id != watcher_id);
            node.watchers.is_empty()
        };
        if should_check {
            self.recompute_has_watchers();
        }
    }

    /// Remove all watchers whose `watcher_id` is in the list (stream-
    /// end cleanup). Updates `has_watchers` only if at least one
    /// watcher was actually removed.
    pub fn remove_all(&self, watcher_ids: &[u64]) {
        if watcher_ids.is_empty() {
            return;
        }
        let id_set: std::collections::HashSet<u64> = watcher_ids.iter().copied().collect();
        let removed = {
            let mut trie = self.trie.write();
            let before = count_watchers(&trie.root);
            remove_all_from_node(&mut trie.root, &id_set);
            let after = count_watchers(&trie.root);
            drop(trie);
            before != after
        };
        if removed {
            self.recompute_has_watchers();
        }
    }

    /// Clear all watchers (leader step-down). Drops all tx senders,
    /// closing client streams. Sets `has_watchers = false`.
    pub fn clear(&self) {
        let mut trie = self.trie.write();
        trie.root = TrieNode::new();
        drop(trie);
        self.has_watchers.store(false, Ordering::Release);
    }

    /// For a set of changed keys (from `Batch::decode`), find matching
    /// prefixes and enqueue notify frames. Called by the coalescer or
    /// directly (debounce=0). Non-blocking: uses `try_send`. Each
    /// notify frame carries both the changed keys and their latest
    /// values (empty bytes for Delete) so the client can act without
    /// a re-read.
    pub fn emit(&self, group_id: u64, slot: u64, changed: &[BatchOp]) {
        let trie = self.trie.read();
        // For each changed key, walk the trie and collect matching
        // (prefix, key, value) triples. Keys and values are borrowed
        // from `changed` (no clone here); the trie read lock is held
        // for the whole emit.
        let mut prefix_map: PrefixMap<'_> = HashMap::new();
        for op in changed {
            let value: &[u8] = match &op.op {
                Op::Put(v) => v.as_ref(),
                Op::Delete => &[],
            };
            trie.collect_matches(&op.key, value, &mut prefix_map);
        }
        // Send one notify per matching prefix. The `WatchNotify` frame
        // is built once per prefix (cloning the borrowed keys/values
        // into owned `Vec<u8>`), then cloned once per watcher on that
        // prefix — the channel owns the response value, so the
        // per-watcher clone is unavoidable, but the per-(prefix,key)
        // collection clones are eliminated.
        for (prefix, (keys, values)) in prefix_map {
            if let Some(watchers) = trie.watchers_for(&prefix) {
                for (_, watcher) in watchers {
                    match watcher {
                        PushTarget::Tonic(tx) => {
                            let notify = WatchNotify {
                                group_id,
                                prefix: prefix.clone(),
                                keys: keys.iter().map(|k| k.to_vec()).collect(),
                                slot,
                                values: values.iter().map(|v| v.to_vec()).collect(),
                            };
                            let resp = WatchNotifyResponse {
                                frame: Some(crate::rpc::watch_notify_response::Frame::Notify(notify)),
                            };
                            match tx.try_send(Ok(resp)) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    self.dropped_notifies.fetch_add(1, Ordering::Relaxed);
                                    tracing::error!(
                                        "critical: watch notify dropped -- watcher channel full, \
                                         client will catch up via safety-net poller"
                                    );
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    self.closed_watchers.fetch_add(1, Ordering::Relaxed);
                                    tracing::warn!(
                                        "watch notify: watcher channel closed -- \
                                         leaked watcher (stream-end cleanup missed it); \
                                         client will catch up via safety-net poller"
                                    );
                                }
                            }
                        }
                        PushTarget::CrowRpc(target) => {
                            self.push_crow_rpc_notify(target, group_id, slot, &prefix, &keys, &values);
                        }
                    }
                }
            }
        }
    }

    /// Build + send a `FBWatchNotify` frame to a crow-rpc watcher.
    /// Fire-and-forget via `RpcClient::send`. On connection errors the
    /// watcher is lazily removed (counter incremented; actual removal
    /// happens on the next emit pass or via the safety-net poller).
    fn push_crow_rpc_notify(
        &self,
        target: &Arc<CrowRpcPushTarget>,
        group_id: u64,
        slot: u64,
        prefix: &[u8],
        keys: &[&[u8]],
        values: &[&[u8]],
    ) {
        let push_id = target.next_id();
        let mut builder = FlatBufferBuilder::new();
        let fb_prefix = builder.create_vector(prefix);
        let key_offsets: Vec<_> = keys
            .iter()
            .map(|k| {
                let data = builder.create_vector(k);
                crow_protocol::kv_client_fb::FBBytes::create(
                    &mut builder,
                    &crow_protocol::kv_client_fb::FBBytesArgs { data: Some(data) },
                )
            })
            .collect();
        let val_offsets: Vec<_> = values
            .iter()
            .map(|v| {
                let data = builder.create_vector(v);
                crow_protocol::kv_client_fb::FBBytes::create(
                    &mut builder,
                    &crow_protocol::kv_client_fb::FBBytesArgs { data: Some(data) },
                )
            })
            .collect();
        let fb_keys = builder.create_vector(&key_offsets);
        let fb_values = builder.create_vector(&val_offsets);
        let args = crow_protocol::kv_client_fb::FBWatchNotifyArgs {
            id: push_id,
            rpc_create_nano: 0,
            group_id,
            prefix: Some(fb_prefix),
            keys: Some(fb_keys),
            slot,
            values: Some(fb_values),
        };
        let fb = crow_protocol::kv_client_fb::FBWatchNotify::create(&mut builder, &args);
        builder.finish(fb, None);
        let control = crow_rpc_ffi::Buffer::from_bytes(builder.finished_data());
        let msg_type = crow_protocol::fb::FBMsgType::EWatchNotify.0 as u16;
        let result = target.rpc.send_to_handle(
            &target.server,
            target.conn.handle().cast::<std::ffi::c_void>(),
            push_id,
            control,
            None,
            msg_type,
            crow_rpc_ffi::noop_completion(),
            std::ptr::null_mut(),
        );
        if let Err(e) = result {
            if matches!(
                e,
                crow_rpc_ffi::RpcError::ConnectionClosed | crow_rpc_ffi::RpcError::ConnectionError
            ) {
                self.closed_watchers.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    "watch notify: crow-rpc push failed ({e}) -- \
                     lazy watcher removal on next emit pass"
                );
            } else {
                self.dropped_notifies.fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    "watch notify: crow-rpc push error ({e}) -- \
                     client will catch up via safety-net poller"
                );
            }
        }
    }

    /// True if at least one watcher is registered. Atomic load — the
    /// apply-path fast path.
    pub fn has_watchers(&self) -> bool {
        self.has_watchers.load(Ordering::Acquire)
    }

    /// Cumulative count of dropped notify frames since registry
    /// creation. Non-zero indicates a slow watcher causing backpressure.
    #[must_use]
    pub fn dropped_notifies(&self) -> u64 {
        self.dropped_notifies.load(Ordering::Relaxed)
    }

    /// Cumulative count of `try_send` calls that returned `Closed`
    /// since registry creation. Non-zero indicates a leaked watcher
    /// entry (stream-end cleanup missed it).
    #[must_use]
    pub fn closed_watchers(&self) -> u64 {
        self.closed_watchers.load(Ordering::Relaxed)
    }

    /// Recompute `has_watchers` by checking if the trie is empty.
    /// Called after removals.
    fn recompute_has_watchers(&self) {
        let empty = {
            let trie = self.trie.read();
            trie.root.watchers.is_empty() && trie.root.children.is_empty()
        };
        self.has_watchers.store(!empty, Ordering::Release);
    }
}

/// Recursively remove watchers with ids in `id_set` from `node` and
/// its children. Prune empty child nodes after removal.
fn remove_all_from_node(node: &mut TrieNode, id_set: &std::collections::HashSet<u64>) {
    node.watchers.retain(|(id, _)| !id_set.contains(id));
    let empty_children: Vec<u8> = node
        .children
        .iter_mut()
        .filter_map(|(byte, child)| {
            remove_all_from_node(child, id_set);
            if child.watchers.is_empty() && child.children.is_empty() {
                Some(*byte)
            } else {
                None
            }
        })
        .collect();
    for byte in empty_children {
        node.children.remove(&byte);
    }
}

/// Total watcher count across `node` and its children.
fn count_watchers(node: &TrieNode) -> usize {
    let mut n = node.watchers.len();
    for child in node.children.values() {
        n += count_watchers(child);
    }
    n
}
