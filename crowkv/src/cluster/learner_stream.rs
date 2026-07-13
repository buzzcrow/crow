// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Per-peer bidi `LearnerStream` client.
//!
//! Multiplexes `Accept`, `Heartbeat`, and `ChosenNotification` frames over a
//! single long-running gRPC bidi stream per `(group_id, peer_id)` pair so
//! that:
//!
//! 1. Heartbeats cannot reorder ahead of an Accept they logically follow.
//! 2. Connection setup cost is paid once per leadership tenure rather than
//!    per RPC.
//! 3. Per-peer flow control is enforceable with a single bounded mpsc
//!    channel.
//!
//! `Prepare` / `RequestVote` / `PreVote` / `StepDown` remain unary RPCs
//! (one-shot, no ordering requirement).
//!
//! The stream is laid down at first use by [`PxLearnerStream::new`] and lives
//! for the lifetime of the parent `PxRemoteReplica`. On transport failure
//! the background task tears down the connection, fails all pending
//! oneshots with [`PxReplicaError::Internal`] (`"stream reset"`), and
//! reconnects with capped exponential backoff.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::cluster::replica::PxReplicaError;
use crate::common::config::PxElectionConfig;
use crate::rpc::px_service_client::PxServiceClient;
use crate::rpc::{
    learner_stream_request, learner_stream_response, AcceptRequest, AcceptedResponse, ChosenNotification,
    HeartbeatRequest, HeartbeatResponse, LearnerStreamRequest, LearnerStreamResponse,
};

/// Map from outbound `request_id` to the awaiting client `oneshot`.
/// Shared between the send-half (insert) and recv-half (remove + dispatch)
/// of a single connection lifetime.
type PendingMap = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<LearnerStreamReply, PxReplicaError>>>>>;

/// Cap on the reconnect backoff.
const BACKOFF_CAP: Duration = Duration::from_secs(2);

/// Initial reconnect backoff after a transport failure.
const BACKOFF_INITIAL: Duration = Duration::from_millis(50);

/// Reply sent back through an in-flight `oneshot` once the server acks.
#[derive(Debug)]
pub enum LearnerStreamReply {
    Accepted(AcceptedResponse),
    Heartbeat(HeartbeatResponse),
}

/// User-side request to the background task.
struct OutboundCmd {
    frame: LearnerStreamRequest,
    /// Oneshot for the reply. `None` for fire-and-forget frames
    /// (`ChosenNotification`).
    reply_tx: Option<oneshot::Sender<Result<LearnerStreamReply, PxReplicaError>>>,
    /// Correlation key (echoed from the inner `request_id`). Only meaningful
    /// when `reply_tx.is_some()`.
    request_id: u64,
}

/// Per-peer bidi `LearnerStream` client.
///
/// Cheap to clone via `Arc`; the background task is shared.
#[derive(Debug)]
pub struct PxLearnerStream {
    endpoint: String,
    cmd_tx: mpsc::Sender<OutboundCmd>,
    cancel: CancellationToken,
}

impl PxLearnerStream {
    /// Spawn a background task that maintains a bidi stream to `endpoint`
    /// and reconnects on transport failure.
    #[must_use]
    pub fn new(endpoint: String, cfg: &PxElectionConfig) -> Arc<Self> {
        let cancel = CancellationToken::new();
        let (cmd_tx, cmd_rx) = mpsc::channel(cfg.learner_stream_window_frames.max(1));
        let stream = Arc::new(Self {
            endpoint: endpoint.clone(),
            cmd_tx,
            cancel: cancel.clone(),
        });
        let endpoint_for_task = endpoint;
        tokio::spawn(run_learner_stream(endpoint_for_task, cmd_rx, cancel));
        stream
    }

    /// Enqueue an `AcceptRequest` and await the matching `AcceptedResponse`.
    ///
    /// `request_id` on the request is the correlation key; callers must
    /// ensure it is unique within the outstanding window.
    ///
    /// # Errors
    /// Returns [`PxReplicaError::Internal`] if the background task is shut
    /// down, the connection resets while the reply is in flight, or the
    /// correlated reply arrives on the wrong oneshot.
    pub async fn send_accept(&self, req: AcceptRequest) -> Result<AcceptedResponse, PxReplicaError> {
        let request_id = req.request_id;
        let (tx, rx) = oneshot::channel();
        let frame = LearnerStreamRequest {
            frame: Some(learner_stream_request::Frame::Accept(req)),
        };
        self.dispatch(OutboundCmd {
            frame,
            reply_tx: Some(tx),
            request_id,
        })?;
        match rx.await {
            Ok(Ok(LearnerStreamReply::Accepted(r))) => Ok(r),
            Ok(Ok(LearnerStreamReply::Heartbeat(_))) => Err(PxReplicaError::Internal(
                "learner_stream: heartbeat reply on accept oneshot (correlation bug)".to_string(),
            )),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(PxReplicaError::Internal(
                "learner_stream: oneshot dropped".to_string(),
            )),
        }
    }

    /// Enqueue a `HeartbeatRequest` and await the matching `HeartbeatResponse`.
    ///
    /// # Errors
    /// Same conditions as [`Self::send_accept`].
    pub async fn send_heartbeat(&self, req: HeartbeatRequest) -> Result<HeartbeatResponse, PxReplicaError> {
        let request_id = req.request_id;
        let (tx, rx) = oneshot::channel();
        let frame = LearnerStreamRequest {
            frame: Some(learner_stream_request::Frame::Heartbeat(req)),
        };
        self.dispatch(OutboundCmd {
            frame,
            reply_tx: Some(tx),
            request_id,
        })?;
        match rx.await {
            Ok(Ok(LearnerStreamReply::Heartbeat(r))) => Ok(r),
            Ok(Ok(LearnerStreamReply::Accepted(_))) => Err(PxReplicaError::Internal(
                "learner_stream: accepted reply on heartbeat oneshot (correlation bug)".to_string(),
            )),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(PxReplicaError::Internal(
                "learner_stream: oneshot dropped".to_string(),
            )),
        }
    }

    /// Fire-and-forget `ChosenNotification`. No reply is expected.
    ///
    /// # Errors
    /// Returns [`PxReplicaError::Internal`] if the background task has
    /// shut down (the bounded mpsc receiver was dropped).
    pub fn send_chosen(&self, notice: ChosenNotification) -> Result<(), PxReplicaError> {
        let frame = LearnerStreamRequest {
            frame: Some(learner_stream_request::Frame::Chosen(notice)),
        };
        self.dispatch(OutboundCmd {
            frame,
            reply_tx: None,
            request_id: 0,
        })
    }

    fn dispatch(&self, cmd: OutboundCmd) -> Result<(), PxReplicaError> {
        // Flow control: `try_send` (non-blocking) instead of
        // `send` (await). When the per-peer mpsc is full we surface the
        // queue depth as a typed error so the proposer can map it to
        // `PxPaxosError::Busy` (already classified `FailRetryable`)
        // rather than blocking the call site behind backpressure.
        match self.cmd_tx.try_send(cmd) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(PxReplicaError::Internal(format!(
                "learner_stream: outbound queue full at peer {}",
                self.endpoint
            ))),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(PxReplicaError::Internal(format!(
                "learner_stream: peer {} is shut down",
                self.endpoint
            ))),
        }
    }

    /// Endpoint this stream is connected to.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Stop the background task and fail any pending oneshots.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }
}

/// Single background task that owns the bidi stream and runs the
/// reconnect loop. Cancellation aborts all in-flight pending oneshots
/// with `stream reset`.
async fn run_learner_stream(
    endpoint: String,
    mut cmd_rx: mpsc::Receiver<OutboundCmd>,
    cancel: CancellationToken,
) {
    let mut backoff = BACKOFF_INITIAL;

    loop {
        if cancel.is_cancelled() {
            return;
        }

        let connect_url = format!("http://{endpoint}");
        let connect_result = PxServiceClient::connect(connect_url.clone()).await;
        let mut client = match connect_result {
            Ok(c) => c,
            Err(err) => {
                error!(endpoint = %endpoint, error = %err, backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX), "learner_stream: connect failed");
                // Fast-fail any commands queued behind the closed channel.
                // Without this, callers (proposers) would block until the
                // peer comes up, which regresses the unary fast-fail
                // behaviour. The reason string mirrors the gRPC `Unavailable`
                // status the unary path used to surface, so existing error-
                // matching call sites (e.g. `remote_error` test) keep
                // working unchanged.
                let reason = format!("grpc Unavailable: learner_stream connect failed: {err}");
                fail_queued_commands(&mut cmd_rx, &reason);
                if !sleep_or_cancel(backoff, &cancel).await {
                    return;
                }
                backoff = (backoff * 2).min(BACKOFF_CAP);
                continue;
            }
        };

        // Outbound channel feeding the bidi stream's request side. Capacity
        // 1 is fine: cmd_rx already provides flow control to the user-
        // facing API, and we never want the bg task to outrun the wire.
        let (out_tx, out_rx) = mpsc::channel::<LearnerStreamRequest>(1);
        let req_stream = tokio_stream::wrappers::ReceiverStream::new(out_rx);

        let bidi = match client.learner_stream(tonic::Request::new(req_stream)).await {
            Ok(resp) => resp.into_inner(),
            Err(err) => {
                error!(endpoint = %endpoint, error = %err, "learner_stream: failed to open bidi");
                if !sleep_or_cancel(backoff, &cancel).await {
                    return;
                }
                backoff = (backoff * 2).min(BACKOFF_CAP);
                continue;
            }
        };

        debug!(endpoint = %endpoint, "learner_stream: connected");
        backoff = BACKOFF_INITIAL;

        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let pending_for_recv = pending.clone();

        // Spawn the recv-half task: pulls server frames and dispatches to
        // pending oneshots. Exits on stream end / error.
        let recv_cancel = cancel.clone();
        let endpoint_for_recv = endpoint.clone();
        let recv_handle = tokio::spawn(async move {
            let mut server_stream = bidi;
            loop {
                use tokio_stream::StreamExt;
                tokio::select! {
                    biased;
                    () = recv_cancel.cancelled() => return,
                    next = server_stream.next() => {
                        let Some(item) = next else { return };
                        match item {
                            Ok(resp) => dispatch_response(&pending_for_recv, resp),
                            Err(status) => {
                                debug!(endpoint = %endpoint_for_recv, error = %status, "learner_stream recv: tonic error");
                                return;
                            }
                        }
                    }
                }
            }
        });

        // Drive the send-half: pull commands, register pending oneshots,
        // ship the wire frame. Exit conditions:
        //   * cancel fired -> shut down for good.
        //   * cmd_rx returned None -> user dropped the PxLearnerStream; exit.
        //   * out_tx.send failed -> server side closed; reconnect.
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                cmd = cmd_rx.recv() => {
                    let Some(cmd) = cmd else { return; };
                    if let Some(reply_tx) = cmd.reply_tx {
                        pending.lock().insert(cmd.request_id, reply_tx);
                    }
                    if out_tx.send(cmd.frame).await.is_err() {
                        break;
                    }
                }
            }
        }

        // Close the outbound half so the server sees EOF + the recv task
        // unblocks.
        drop(out_tx);
        recv_handle.abort();

        // Fail all pending oneshots so callers don't hang.
        let drained: Vec<_> = std::mem::take(&mut *pending.lock()).into_iter().collect();
        for (_, sender) in drained {
            let _ = sender.send(Err(PxReplicaError::Internal(
                "learner_stream: stream reset".to_string(),
            )));
        }

        if cancel.is_cancelled() {
            return;
        }

        // Loop back to reconnect.
        if !sleep_or_cancel(backoff, &cancel).await {
            return;
        }
        backoff = (backoff * 2).min(BACKOFF_CAP);
    }
}

fn dispatch_response(pending: &PendingMap, resp: LearnerStreamResponse) {
    let Some(frame) = resp.frame else { return };
    let (request_id, reply) = match frame {
        learner_stream_response::Frame::Accepted(r) => (r.request_id, LearnerStreamReply::Accepted(r)),
        learner_stream_response::Frame::Heartbeat(r) => (r.request_id, LearnerStreamReply::Heartbeat(r)),
    };
    if let Some(tx) = pending.lock().remove(&request_id) {
        let _ = tx.send(Ok(reply));
    } else {
        debug!(
            request_id,
            "learner_stream recv: no pending oneshot (late ack or duplicate)"
        );
    }
}

/// Drain any commands queued behind the bg-task channel and fail their
/// awaiting oneshots with the given message. Called from the connect-
/// retry loop so callers do not block on a peer that is down.
fn fail_queued_commands(cmd_rx: &mut mpsc::Receiver<OutboundCmd>, reason: &str) {
    while let Ok(cmd) = cmd_rx.try_recv() {
        if let Some(tx) = cmd.reply_tx {
            let _ = tx.send(Err(PxReplicaError::Internal(reason.to_string())));
        }
    }
}

/// Sleep for `dur` or until `cancel` fires. Returns `false` if cancelled.
async fn sleep_or_cancel(dur: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        () = cancel.cancelled() => false,
        () = tokio::time::sleep(dur) => true,
    }
}
