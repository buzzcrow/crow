// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Client for the crow-kv `WatchNotify` push. Opens a connection to
//! the group leader, subscribes to prefixes, and delivers notify
//! frames via a channel. Automatically reconnects on leader change.
//!
//! Two transport paths coexist (R117 mixed rollout):
//! - **Tonic** (default): bidi stream via `KvServiceClient::watch_notify`.
//! - **crow-rpc** (when `CrowkvClient::with_rpc_transport` is set):
//!   persistent connection + client-side handler for `FBWatchNotify` /
//!   `FBWatchNotifyError` push frames, fire-and-forget
//!   `FBWatchSubscribe`.

use std::sync::Arc;
use std::time::Duration;

use flatbuffers::FlatBufferBuilder;
use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;

use crate::client::CrowkvClient;
use crate::error::Result;
use crow_kv::rpc::kv_service_client::KvServiceClient;
use crow_kv::rpc::watch_notify_request;
use crow_kv::rpc::{watch_notify_response, WatchNotifyRequest};
use crow_protocol::fb::FBMsgType;
use crow_protocol::fb_wrappers::kv_client::FBWatchNotifyRef;
use crow_protocol::kv_client_fb::{
    FBWatchNotifyError, FBWatchNotifyErrorArgs, FBWatchSubscribe, FBWatchSubscribeArgs,
};
use crow_rpc_ffi::{noop_completion, Buffer, RpcClient, RpcServer};

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

/// Client for the crow-kv `WatchNotify` push.
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

    /// Subscribe to `(store_id, group_id, prefix)`. Opens a connection
    /// to the group leader (discovered via the topology cache), sends a
    /// `WatchSubscribe` frame, and returns a `WatchSubscription` whose
    /// `notify_rx` yields `WatchNotify` frames.
    ///
    /// On leader-change, the reader task automatically reconnects to
    /// the new leader and re-subscribes; the caller's `notify_rx`
    /// stays open across the reconnect. Missed notifies during the
    /// reconnect gap are caught by the caller's safety-net polling.
    ///
    /// # Errors
    /// Returns `Err` if the leader endpoint cannot be resolved after
    /// a topology refresh.
    pub fn subscribe(&self, store_id: u64, group_id: u64, prefix: &[u8]) -> Result<WatchSubscription> {
        let (notify_tx, notify_rx) = mpsc::channel::<WatchNotify>(64);
        let (abort_tx, abort_rx) = tokio::sync::oneshot::channel::<()>();
        let kv = Arc::clone(&self.kv);
        let prefix = prefix.to_vec();

        // Select the transport path based on whether the client has
        // a crow-rpc transport set.
        if kv.rpc_transport().is_some() {
            tokio::spawn(async move {
                crow_rpc_reader_loop(kv, store_id, group_id, prefix, notify_tx, abort_rx).await;
            });
        } else {
            tokio::spawn(async move {
                tonic_reader_loop(kv, store_id, group_id, prefix, notify_tx, abort_rx).await;
            });
        }

        Ok(WatchSubscription {
            notify_rx,
            abort: Some(abort_tx),
        })
    }
}

/// Maximum reconnect backoff.
const MAX_BACKOFF: Duration = Duration::from_secs(2);

/// Connection-alive check interval for the crow-rpc path. crow-rpc
/// does not deliver a connection-close callback, so the reader loop
/// periodically sends a no-op fire-and-forget frame to check if the
/// connection is still alive. If the send fails, the loop reconnects.
const CROW_RPC_LIVENESS_CHECK: Duration = Duration::from_secs(5);

// ── Tonic reader loop (existing path) ────────────────────────────

/// Reader loop: resolve leader, open stream, forward notifies,
/// reconnect on error/leader-change. Runs until the abort signal.
async fn tonic_reader_loop(
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

// ── crow-rpc reader loop (R117 path) ─────────────────────────────

/// crow-rpc reader loop: resolve leader, open connection, register
/// push handlers, send `FBWatchSubscribe`, and periodically check
/// connection liveness. On `FBWatchNotifyError` with
/// `not_leader_hint` or liveness-check failure, reconnect.
#[allow(clippy::too_many_lines)]
async fn crow_rpc_reader_loop(
    kv: Arc<CrowkvClient>,
    store_id: u64,
    group_id: u64,
    prefix: Vec<u8>,
    notify_tx: mpsc::Sender<WatchNotify>,
    mut abort_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let mut backoff = Duration::from_millis(50);
    loop {
        if let Err(e) = kv.topology.refresh().await {
            tracing::warn!(error = %e, "watch_notify(crow-rpc): topology refresh failed");
            sleep_backoff(&mut backoff).await;
            continue;
        }
        let Some(endpoint) = kv.topology.leader(store_id, group_id) else {
            tracing::warn!(
                store_id,
                group_id,
                "watch_notify(crow-rpc): leader still unknown after refresh"
            );
            sleep_backoff(&mut backoff).await;
            continue;
        };

        let Some(transport) = kv.rpc_transport() else {
            // Transport was unset between subscribe and the loop —
            // fall back to the tonic path.
            tracing::warn!("watch_notify(crow-rpc): transport unset, falling back to tonic");
            tonic_reader_loop(kv, store_id, group_id, prefix, notify_tx, abort_rx).await;
            return;
        };

        let conn = match transport.get_conn(&endpoint) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "watch_notify(crow-rpc): connect failed");
                sleep_backoff(&mut backoff).await;
                continue;
            }
        };

        // Register push handlers for `FBWatchNotify` + `FBWatchNotifyError`.
        // The handlers forward frames to the notify channel. A reconnect
        // signal (via the `reconnect_tx` channel) breaks the liveness
        // loop and triggers re-resolution.
        let (reconnect_tx, mut reconnect_rx) = tokio::sync::mpsc::channel::<()>(1);
        let notify_tx_clone = Arc::new(notify_tx.clone());
        let reconnect_tx_clone = reconnect_tx.clone();
        let kv_clone = Arc::clone(&kv);
        register_crow_rpc_handlers(
            transport.rpc(),
            transport.server(),
            &notify_tx_clone,
            reconnect_tx_clone,
            kv_clone,
            store_id,
        );

        // Send `FBWatchSubscribe` as fire-and-forget.
        let sub_id = transport.alloc_id();
        let mut builder = FlatBufferBuilder::new();
        let fb_prefix = builder.create_vector(&prefix);
        let args = FBWatchSubscribeArgs {
            id: sub_id,
            rpc_create_nano: 0,
            version: 1,
            group_id,
            prefix: Some(fb_prefix),
        };
        let fb = FBWatchSubscribe::create(&mut builder, &args);
        builder.finish(fb, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EWatchSubscribe.0 as u16;
        let send_result = transport.rpc().send(
            transport.server(),
            &conn,
            sub_id,
            control,
            None,
            msg_type,
            noop_completion(),
            std::ptr::null_mut(),
        );
        if let Err(e) = send_result {
            tracing::warn!(error = %e, "watch_notify(crow-rpc): subscribe send failed");
            sleep_backoff(&mut backoff).await;
            continue;
        }

        // Reset backoff on successful subscribe.
        backoff = Duration::from_millis(50);
        tracing::info!(
            store_id, group_id, endpoint = %endpoint,
            "watch_notify(crow-rpc): subscribed, waiting for push frames"
        );

        // Liveness loop: wait for reconnect signal or periodic check.
        loop {
            tokio::select! {
                _ = &mut abort_rx => return,
                _ = reconnect_rx.recv() => {
                    tracing::info!("watch_notify(crow-rpc): reconnect signal, re-resolving leader");
                    break;
                }
                () = tokio::time::sleep(CROW_RPC_LIVENESS_CHECK) => {
                    // Periodic liveness check: send a no-op
                    // fire-and-forget frame. If the send fails, the
                    // connection is dead — reconnect.
                    let ping_id = transport.alloc_id();
                    let mut builder = FlatBufferBuilder::new();
                    let args = FBWatchNotifyErrorArgs {
                        id: ping_id,
                        rpc_create_nano: 0,
                        group_id,
                        not_leader_hint: None,
                        error: None,
                    };
                    let fb = FBWatchNotifyError::create(&mut builder, &args);
                    builder.finish(fb, None);
                    let control = Buffer::from_bytes(builder.finished_data());
                    let msg_type = FBMsgType::EWatchNotifyError.0 as u16;
                    let result = transport.rpc().send(
                        transport.server(),
                        &conn,
                        ping_id,
                        control,
                        None,
                        msg_type,
                        noop_completion(),
                        std::ptr::null_mut(),
                    );
                    if let Err(e) = result {
                        tracing::warn!(error = %e, "watch_notify(crow-rpc): liveness check failed, reconnecting");
                        break;
                    }
                }
            }
        }
    }
}

/// Register client-side handlers for `FBWatchNotify` (1119) and
/// `FBWatchNotifyError` (1120) push frames. The handlers parse the
/// flatbuffer and forward to the notify channel / reconnect signal.
fn register_crow_rpc_handlers(
    rpc: &Arc<RpcClient>,
    _server: &Arc<RpcServer>,
    notify_tx: &Arc<mpsc::Sender<WatchNotify>>,
    reconnect_tx: tokio::sync::mpsc::Sender<()>,
    kv: Arc<CrowkvClient>,
    store_id: u64,
) {
    // Handler for `FBWatchNotify` push frames.
    let notify_tx_h = Arc::clone(notify_tx);
    rpc.register_handler(
        FBMsgType::EWatchNotify.0 as u16,
        move |req: crow_rpc_ffi::ClientRequest<'_>| {
            let r = FBWatchNotifyRef::new(req.control);
            if !r.valid() {
                tracing::warn!("watch_notify(crow-rpc): malformed FBWatchNotify frame");
                return;
            }
            let group_id = r.group_id();
            let prefix = r.prefix().unwrap_or(&[]).to_vec();
            let keys: Vec<Vec<u8>> = r
                .keys()
                .map(|v| v.map(<[u8]>::to_vec).collect())
                .unwrap_or_default();
            let values: Vec<Vec<u8>> = r
                .values()
                .map(|v| v.map(<[u8]>::to_vec).collect())
                .unwrap_or_default();
            let slot = r.slot();
            let notify = WatchNotify {
                group_id,
                prefix,
                keys,
                slot,
                values,
            };
            let tx = Arc::clone(&notify_tx_h);
            tokio::spawn(async move {
                let _ = tx.send(notify).await;
            });
        },
    );

    // Handler for `FBWatchNotifyError` push frames.
    let reconnect_tx_h = reconnect_tx;
    rpc.register_handler(
        FBMsgType::EWatchNotifyError.0 as u16,
        move |req: crow_rpc_ffi::ClientRequest<'_>| {
            let Ok(fb) = flatbuffers::root::<FBWatchNotifyError>(req.control) else {
                tracing::warn!("watch_notify(crow-rpc): malformed FBWatchNotifyError frame");
                return;
            };
            let hint = fb.not_leader_hint().unwrap_or("").to_string();
            let error = fb.error().unwrap_or("").to_string();
            if !hint.is_empty() {
                tracing::info!(
                    store_id, hint = %hint,
                    "watch_notify(crow-rpc): not_leader_hint, reconnecting"
                );
                kv.topology.set_leader(store_id, fb.group_id(), hint);
                let tx = reconnect_tx_h.clone();
                tokio::spawn(async move {
                    let _ = tx.send(()).await;
                });
            } else if !error.is_empty() {
                tracing::warn!(error = %error, "watch_notify(crow-rpc): server error frame");
            }
        },
    );
}

/// Sleep for the current backoff duration, then double it (capped).
async fn sleep_backoff(backoff: &mut Duration) {
    tokio::time::sleep(*backoff).await;
    *backoff = (*backoff * 2).min(MAX_BACKOFF);
}
