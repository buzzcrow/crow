// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Client for the crow-kv `WatchNotify` bidi stream. Opens a stream
//! to the group leader, subscribes to prefixes, and delivers notify
//! frames via a channel. Automatically reconnects on leader change.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;

use crate::client::CrowkvClient;
use crate::error::Result;
use crow_kv::rpc::kv_service_client::KvServiceClient;
use crow_kv::rpc::watch_notify_request;
use crow_kv::rpc::{watch_notify_response, WatchNotifyRequest};

/// Re-export of the `WatchNotify` frame for callers.
pub use crow_kv::rpc::WatchNotify;

/// A live watch subscription. Dropping it unsubscribes and closes the
/// stream.
pub struct WatchSubscription {
    /// Notify frames for this subscription. Receiver end of an mpsc;
    /// the client reads `WatchNotify` frames here.
    pub notify_rx: mpsc::Receiver<WatchNotify>,
    /// Internal handle to abort the reader task on drop.
    abort: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for WatchSubscription {
    fn drop(&mut self) {
        // Signal the reader task to stop. Dropping the sender also
        // closes the gRPC stream (the client half closes).
        if let Some(abort) = self.abort.take() {
            let _ = abort.send(());
        }
    }
}

/// Client for the crow-kv `WatchNotify` bidi stream.
pub struct WatchNotifyClient {
    kv: Arc<CrowkvClient>,
}

impl WatchNotifyClient {
    /// Create from a shared `CrowkvClient`. Reuses the client's
    /// `ConnectionPool` + `TopologyCache`.
    #[must_use]
    pub fn from_shared(kv: Arc<CrowkvClient>) -> Self {
        Self { kv }
    }

    /// Subscribe to `(store_id, group_id, prefix)`. Opens a bidi
    /// stream to the group leader (discovered via the topology cache),
    /// sends a `WatchSubscribe` frame, and returns a
    /// `WatchSubscription` whose `notify_rx` yields `WatchNotify`
    /// frames.
    ///
    /// On leader-change (stream closes), the reader task automatically
    /// reconnects to the new leader and re-subscribes; the caller's
    /// `notify_rx` stays open across the reconnect. Missed notifies
    /// during the reconnect gap are caught by the caller's
    /// safety-net polling.
    ///
    /// # Errors
    /// Returns `Err` if the leader endpoint cannot be resolved after
    /// a topology refresh.
    pub fn subscribe(&self, store_id: u64, group_id: u64, prefix: &[u8]) -> Result<WatchSubscription> {
        let (notify_tx, notify_rx) = mpsc::channel::<WatchNotify>(64);
        let (abort_tx, abort_rx) = tokio::sync::oneshot::channel::<()>();
        let kv = Arc::clone(&self.kv);
        let prefix = prefix.to_vec();

        tokio::spawn(async move {
            reader_loop(kv, store_id, group_id, prefix, notify_tx, abort_rx).await;
        });

        Ok(WatchSubscription {
            notify_rx,
            abort: Some(abort_tx),
        })
    }
}

/// Maximum reconnect backoff.
const MAX_BACKOFF: Duration = Duration::from_secs(2);

/// Reader loop: resolve leader, open stream, forward notifies,
/// reconnect on error/leader-change. Runs until the abort signal.
async fn reader_loop(
    kv: Arc<CrowkvClient>,
    store_id: u64,
    group_id: u64,
    prefix: Vec<u8>,
    notify_tx: mpsc::Sender<WatchNotify>,
    mut abort_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let mut backoff = Duration::from_millis(50);
    loop {
        // Proactively refresh topology on every (re)connect. A cached
        // leader endpoint may be stale if the leader changed during the
        // disconnect gap; refreshing first avoids connecting to the old
        // leader and getting bounced back via not_leader_hint (or, if
        // the old leader is down, getting stuck in backoff retries).
        if let Err(e) = kv.topology.refresh().await {
            tracing::warn!(error = %e, "watch_notify: topology refresh failed");
            sleep_backoff(&mut backoff).await;
            continue;
        }
        let Some(endpoint) = kv.topology.leader(store_id, group_id) else {
            tracing::warn!(
                store_id,
                group_id,
                "watch_notify: leader still unknown after refresh"
            );
            sleep_backoff(&mut backoff).await;
            continue;
        };

        let channel = match kv.pool.get(&endpoint) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "watch_notify: pool get failed");
                sleep_backoff(&mut backoff).await;
                continue;
            }
        };

        let mut client = KvServiceClient::new(channel);
        let req = WatchNotifyRequest {
            frame: Some(watch_notify_request::Frame::Subscribe(
                crow_kv::rpc::WatchSubscribe {
                    version: 1,
                    group_id,
                    prefix: prefix.clone(),
                },
            )),
        };
        let stream_req = tokio_stream::iter(vec![req]);
        let response = match client.watch_notify(stream_req).await {
            Ok(r) => r.into_inner(),
            Err(e) => {
                tracing::warn!(error = %e, "watch_notify: stream open failed");
                sleep_backoff(&mut backoff).await;
                continue;
            }
        };

        // Reset backoff on successful connection.
        backoff = Duration::from_millis(50);

        // Read the response stream, forwarding notify frames.
        let mut stream = response;
        loop {
            tokio::select! {
                _ = &mut abort_rx => return,
                item = stream.next() => match item {
                    Some(Ok(resp)) => {
                        if let Some(watch_notify_response::Frame::Notify(notify)) = resp.frame {
                            if notify_tx.send(notify).await.is_err() {
                                return;
                            }
                        } else if let Some(watch_notify_response::Frame::Error(err)) = resp.frame {
                            if !err.not_leader_hint.is_empty() {
                                kv.topology.set_leader(store_id, group_id, err.not_leader_hint);
                                break;
                            }
                            tracing::warn!(error = err.error, "watch_notify: server error");
                        }
                    }
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, "watch_notify: stream error, reconnecting");
                        break;
                    }
                    None => {
                        tracing::info!("watch_notify: stream closed, reconnecting");
                        break;
                    }
                }
            }
        }
    }
}

/// Sleep for the current backoff duration, then double it (capped).
async fn sleep_backoff(backoff: &mut Duration) {
    tokio::time::sleep(*backoff).await;
    *backoff = (*backoff * 2).min(MAX_BACKOFF);
}
