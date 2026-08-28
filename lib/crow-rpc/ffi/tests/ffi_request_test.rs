// Copyright 2026-present buzzcrow <buzzcrow@126.com>.
// Licensed under the Apache License, Version 2.0.

//! E2E tests for bidirectional request-response (R114):
//! - Server→client request-response via server handler + request_client
//! - Client→server regression (existing path still works)
//! - Client response routing precedence (response routed to pending, not handler)
//! - Client handler dispatch (server sends request to client via handler chain)

use crow_common::RequestIdGen;
use crow_protocol::fb::{
    ConnectionPingRequest, ConnectionPingRequestArgs, ConnectionPingResponse, ConnectionPingResponseArgs,
    FBMsgType, FBRetCode,
};
use crow_rpc_ffi::{Buffer, BufferPool, CallFuture, RpcClient, RpcServer};
use flatbuffers::FlatBufferBuilder;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// ── Client→server regression ──────────────────────────────────────
// Verify the existing client→server path still works after the
// on_response return-bool change and the server dispatch reorder.
#[tokio::test]
async fn client_to_server_regression() {
    let server = RpcServer::new(None);
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0);

    server.start();

    let conn = server.connect("127.0.0.1", port).expect("connect failed");

    let mut fbb = FlatBufferBuilder::new();
    let req = ConnectionPingRequest::create(
        &mut fbb,
        &ConnectionPingRequestArgs {
            id: 12345,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(req, None);
    let fb_bytes = fbb.finished_data();

    let pool = BufferPool::new(256);
    let mut ctrl = pool.alloc_buffer(fb_bytes.len() as u32).expect("alloc control");
    ctrl.write(fb_bytes);

    let caller = RpcClient::new();
    caller.attach(&conn);
    let future = caller
        .call(
            &server,
            &conn,
            12345,
            ctrl,
            None,
            FBMsgType::EConnectionPingRequest.0 as u16,
        )
        .expect("call submit failed");

    let response = tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("timeout waiting for response")
        .expect("callback dropped");

    assert_eq!(response.request_id, 12345, "response id should match");

    server.stop();
}

// ── Server dispatch with request_client (handler-first order) ─────
// Register a handler on the SERVER for a custom msg_type. The client
// sends a request with that msg_type. The server's dispatch finds the
// handler (handler-first order) and dispatches it. The handler submits
// a response. The response goes back to the client's on_frame →
// on_response (matches) → callback invoked.
//
// This verifies the server's dispatch order: handler first, then
// on_response. Without this order, the request frame would be
// intercepted by on_response (since the req_id is in the
// request_client's pending map in a loopback).
#[tokio::test]
async fn server_dispatch_handler_first_order() {
    const CUSTOM_MSG_TYPE: u16 = 300;

    let server = Arc::new(RpcServer::new(None));
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0);

    // Register a handler on the SERVER for the custom msg_type.
    let server_for_handler = server.clone();
    server.register_handler(CUSTOM_MSG_TYPE, move |req| {
        let mut fbb = FlatBufferBuilder::new();
        let resp = ConnectionPingResponse::create(
            &mut fbb,
            &ConnectionPingResponseArgs {
                id: req.request_id,
                rpc_create_nano: req.rpc_create_nano,
                response_create_nano: 0,
                ret: FBRetCode::Success,
            },
        );
        fbb.finish(resp, None);
        let ctrl_bytes = fbb.finished_data().to_vec();
        let resp_msg_type = FBMsgType::EConnectionPingResponse.0 as u16;
        unsafe {
            let _ = server_for_handler.submit_response(
                req.conn_handle,
                &ctrl_bytes,
                None,
                resp_msg_type,
                req.request_id,
            );
        }
    });

    // Wire a request_client into the server (for response routing).
    // In this test, the request_client is the same as the caller —
    // the client's pending map has the req_id. The server's dispatch
    // checks for a handler FIRST, so the request is not intercepted
    // by on_response.
    let caller = RpcClient::new();
    server.set_request_client(&caller);

    server.start();

    let conn = server.connect("127.0.0.1", port).expect("connect failed");
    caller.attach(&conn);

    let id_gen = RequestIdGen::new();
    let req_id = id_gen.next().as_u64();

    let mut fbb = FlatBufferBuilder::new();
    let req = ConnectionPingRequest::create(
        &mut fbb,
        &ConnectionPingRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(req, None);
    let fb_bytes = fbb.finished_data();

    let pool = BufferPool::new(256);
    let mut ctrl = pool.alloc_buffer(fb_bytes.len() as u32).expect("alloc control");
    ctrl.write(fb_bytes);

    let future = caller
        .call(&server, &conn, req_id, ctrl, None, CUSTOM_MSG_TYPE)
        .expect("call submit failed");

    let response = tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("timeout waiting for response")
        .expect("callback dropped");

    assert_eq!(response.request_id, req_id, "response id should match");

    server.stop();
}

// ── Client response routing precedence ────────────────────────────
// Verify that a response frame (matching a pending client-sent request)
// is routed to the pending callback, NOT to the handler dispatch.
// We register a handler for the ping response msg_type, then send a
// ping request. The response should still go to the call() future,
// not the handler.
#[tokio::test]
async fn client_response_routing_precedence() {
    let server = RpcServer::new(None);
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0);

    server.start();

    let conn = server.connect("127.0.0.1", port).expect("connect failed");

    let handler_called = Arc::new(AtomicBool::new(false));
    let handler_called_clone = handler_called.clone();

    let caller = RpcClient::new();
    caller.set_transport(&server);
    // Register a handler for ConnectionPingResponse — but the response
    // should NOT reach it (it should be routed to the pending callback).
    caller.register_handler(FBMsgType::EConnectionPingResponse.0 as u16, move |_req| {
        handler_called_clone.store(true, Ordering::SeqCst);
    });
    caller.attach(&conn);

    let mut fbb = FlatBufferBuilder::new();
    let req = ConnectionPingRequest::create(
        &mut fbb,
        &ConnectionPingRequestArgs {
            id: 555,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(req, None);
    let fb_bytes = fbb.finished_data();

    let pool = BufferPool::new(256);
    let mut ctrl = pool.alloc_buffer(fb_bytes.len() as u32).expect("alloc control");
    ctrl.write(fb_bytes);

    let future = caller
        .call(
            &server,
            &conn,
            555,
            ctrl,
            None,
            FBMsgType::EConnectionPingRequest.0 as u16,
        )
        .expect("call submit failed");

    let response = tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("timeout waiting for response")
        .expect("callback dropped");

    assert_eq!(response.request_id, 555, "response id should match");
    assert!(
        !handler_called.load(Ordering::SeqCst),
        "handler should NOT be called for response frames"
    );

    server.stop();
}

// ── Client handler dispatch (server→client via handler chain) ─────
// Test the client's handler dispatch (dispatch_request) by chaining:
// 1. Register a handler on the SERVER for PING that sends a NOTIFY
//    request back to the client via request_client.call() on the
//    server-side connection (Connection::from_handle).
// 2. Register a handler on the CLIENT for NOTIFY that submits an ack.
// 3. Client sends PING → server's PING handler fires → sends NOTIFY
//    to client → client's NOTIFY handler fires → submits ack →
//    server's on_response matches the NOTIFY req_id.
//
// This tests the full server→client→server roundtrip: the server
// initiates a request to the client, the client handles it and acks.
// R32's Connection::from_handle unblocks the server→client send path
// (R114 originally dropped the NOTIFY buffer here).
#[tokio::test]
async fn client_handler_dispatch_via_server_chain() {
    const PING_MSG_TYPE: u16 = FBMsgType::EConnectionPingRequest.0 as u16;
    const NOTIFY_MSG_TYPE: u16 = 310;
    const NOTIFY_ACK_MSG_TYPE: u16 = FBMsgType::EConnectionPingResponse.0 as u16;

    let server = Arc::new(RpcServer::new(None));
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0);

    // The request_client is used by the server's PING handler to send
    // a NOTIFY request to the client. The ack will be routed back to
    // this client's pending map via the server's dispatch.
    let request_client = Arc::new(RpcClient::new());
    request_client.set_completion_pool_size(64);

    // Wire the request_client into the server for ack routing.
    server.set_request_client(&request_client);

    // Shared state: the NOTIFY CallFuture (awaited after the PING ack)
    // + the NOTIFY req_id (to verify the ack matches) + a flag to
    // verify the client's NOTIFY handler actually fired.
    let notify_future: Arc<Mutex<Option<CallFuture>>> = Arc::new(Mutex::new(None));
    let notify_req_id = Arc::new(AtomicU64::new(0));
    let notify_handler_fired = Arc::new(AtomicBool::new(false));

    // Register a handler on the SERVER for PING. When invoked, it
    // acks the PING, then sends a NOTIFY request back to the client
    // via request_client.call() using Connection::from_handle.
    let server_for_handler = server.clone();
    let request_client_for_handler = request_client.clone();
    let notify_future_for_handler = notify_future.clone();
    let notify_req_id_for_handler = notify_req_id.clone();

    server.register_handler(PING_MSG_TYPE, move |req| {
        // First, ack the original PING request.
        let mut fbb2 = FlatBufferBuilder::new();
        let ping_resp = ConnectionPingResponse::create(
            &mut fbb2,
            &ConnectionPingResponseArgs {
                id: req.request_id,
                rpc_create_nano: req.rpc_create_nano,
                response_create_nano: 0,
                ret: FBRetCode::Success,
            },
        );
        fbb2.finish(ping_resp, None);
        let ping_ack_bytes = fbb2.finished_data().to_vec();
        unsafe {
            let _ = server_for_handler.submit_response(
                req.conn_handle,
                &ping_ack_bytes,
                None,
                NOTIFY_ACK_MSG_TYPE,
                req.request_id,
            );
        }

        // Build a NOTIFY request to send back to the client.
        let id_gen = RequestIdGen::new();
        let nreq_id = id_gen.next().as_u64();
        notify_req_id_for_handler.store(nreq_id, Ordering::SeqCst);

        let mut fbb = FlatBufferBuilder::new();
        let nreq = ConnectionPingRequest::create(
            &mut fbb,
            &ConnectionPingRequestArgs {
                id: nreq_id,
                rpc_create_nano: 0,
            },
        );
        fbb.finish(nreq, None);
        let fb_bytes = fbb.finished_data();

        // Standalone buffer (no pool): the buffer is queued for send and
        // released by the I/O worker after this handler returns, so a
        // pool-allocated buffer would dangle when the local pool drops.
        let ctrl = Buffer::from_vec(fb_bytes.to_vec());

        // Send the NOTIFY request via request_client.call_to_handle()
        // on the server-side connection (raw conn_handle from the
        // handler). The frame goes to the client's on_frame →
        // dispatch_request → NOTIFY handler. The ack will be routed
        // back to request_client's pending map via the server's
        // dispatch.
        if let Ok(fut) = request_client_for_handler.call_to_handle(
            &server_for_handler,
            req.conn_handle,
            nreq_id,
            ctrl,
            None,
            NOTIFY_MSG_TYPE,
        ) {
            *notify_future_for_handler.lock().unwrap() = Some(fut);
        }
    });

    // Register a handler on the CLIENT for NOTIFY.
    let server_for_client_handler = server.clone();
    let notify_handler_fired_for_client = notify_handler_fired.clone();
    let client = RpcClient::new();
    client.set_transport(&server);
    client.register_handler(NOTIFY_MSG_TYPE, move |req| {
        notify_handler_fired_for_client.store(true, Ordering::SeqCst);

        // Ack the NOTIFY request.
        let mut fbb = FlatBufferBuilder::new();
        let resp = ConnectionPingResponse::create(
            &mut fbb,
            &ConnectionPingResponseArgs {
                id: req.request_id,
                rpc_create_nano: req.rpc_create_nano,
                response_create_nano: 0,
                ret: FBRetCode::Success,
            },
        );
        fbb.finish(resp, None);
        let ctrl_bytes = fbb.finished_data().to_vec();
        unsafe {
            let _ = server_for_client_handler.submit_response(
                req.conn_handle,
                &ctrl_bytes,
                None,
                NOTIFY_ACK_MSG_TYPE,
                req.request_id,
            );
        }
    });

    server.start();

    let conn = server.connect("127.0.0.1", port).expect("connect failed");
    client.attach(&conn);

    // Client sends PING → server's PING handler acks + sends NOTIFY →
    // client receives ack.
    let mut fbb = FlatBufferBuilder::new();
    let req = ConnectionPingRequest::create(
        &mut fbb,
        &ConnectionPingRequestArgs {
            id: 777,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(req, None);
    let fb_bytes = fbb.finished_data();

    let pool = BufferPool::new(256);
    let mut ctrl = pool.alloc_buffer(fb_bytes.len() as u32).expect("alloc control");
    ctrl.write(fb_bytes);

    let future = client
        .call(&server, &conn, 777, ctrl, None, PING_MSG_TYPE)
        .expect("call submit failed");

    let response = tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("timeout waiting for ping ack")
        .expect("callback dropped");

    assert_eq!(response.request_id, 777, "ping ack id should match");

    // The server's PING handler also sent a NOTIFY request to the
    // client. Verify the client's NOTIFY handler fired and the ack
    // was routed back to request_client's pending map.
    let notify_fut = notify_future
        .lock()
        .unwrap()
        .take()
        .expect("NOTIFY CallFuture was not stored (handler did not fire?)");

    let notify_response = tokio::time::timeout(std::time::Duration::from_secs(10), notify_fut)
        .await
        .expect("timeout waiting for NOTIFY ack")
        .expect("NOTIFY callback dropped");

    let expected_nreq_id = notify_req_id.load(Ordering::SeqCst);
    assert!(
        expected_nreq_id > 0,
        "NOTIFY req_id should have been stored by the PING handler"
    );
    assert_eq!(
        notify_response.request_id, expected_nreq_id,
        "NOTIFY ack id should match the NOTIFY request id"
    );
    assert!(
        notify_handler_fired.load(Ordering::SeqCst),
        "client NOTIFY handler should have fired"
    );

    server.stop();
}

// ── Server→client timeout (no handler, reaper times out) ──────────
// Test the timeout/error path for server-initiated requests: the
// server sends a request with a msg_type the client has NO handler
// for (and no transport set, so the frame is dropped — no
// UnknownMessage response). The request_client's reaper scans the
// pending slab and fails the entry with Timeout. This covers the
// server_to_client_timeout_no_handler test that was dropped during
// R114's dispatch-order fix. R117's WatchNotify uses fire-and-forget
// send() (no pending entry), so this gap does not affect it — but
// any future server→client call() (request-response) needs this
// timeout path to be correct.
#[tokio::test]
async fn server_to_client_timeout_no_handler() {
    const PING_MSG_TYPE: u16 = FBMsgType::EConnectionPingRequest.0 as u16;
    const NO_HANDLER_MSG_TYPE: u16 = 320;
    const PING_ACK_MSG_TYPE: u16 = FBMsgType::EConnectionPingResponse.0 as u16;

    let server = Arc::new(RpcServer::new(None));
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0);

    // request_client with a short reaper timeout so the test doesn't
    // wait long. The reaper scans every 50ms for entries past their
    // 300ms deadline.
    let request_client = Arc::new(RpcClient::new());
    request_client.set_completion_pool_size(64);
    request_client.start_reaper(300_000_000, 50_000_000);

    server.set_request_client(&request_client);

    // Shared state: the no-handler CallFuture (awaited after PING ack).
    let no_handler_future: Arc<Mutex<Option<CallFuture>>> = Arc::new(Mutex::new(None));

    // Register a handler on the SERVER for PING. When invoked, it
    // acks the PING, then sends a request with NO_HANDLER_MSG_TYPE
    // to the client via request_client.call(). The client has no
    // handler for this msg_type and no transport set → the frame is
    // dropped → no response → the reaper times out the pending entry.
    let server_for_handler = server.clone();
    let request_client_for_handler = request_client.clone();
    let no_handler_future_for_handler = no_handler_future.clone();

    server.register_handler(PING_MSG_TYPE, move |req| {
        // First, ack the original PING request.
        let mut fbb2 = FlatBufferBuilder::new();
        let ping_resp = ConnectionPingResponse::create(
            &mut fbb2,
            &ConnectionPingResponseArgs {
                id: req.request_id,
                rpc_create_nano: req.rpc_create_nano,
                response_create_nano: 0,
                ret: FBRetCode::Success,
            },
        );
        fbb2.finish(ping_resp, None);
        let ping_ack_bytes = fbb2.finished_data().to_vec();
        unsafe {
            let _ = server_for_handler.submit_response(
                req.conn_handle,
                &ping_ack_bytes,
                None,
                PING_ACK_MSG_TYPE,
                req.request_id,
            );
        }

        // Build a request the client has no handler for.
        let id_gen = RequestIdGen::new();
        let nreq_id = id_gen.next().as_u64();

        let mut fbb = FlatBufferBuilder::new();
        let nreq = ConnectionPingRequest::create(
            &mut fbb,
            &ConnectionPingRequestArgs {
                id: nreq_id,
                rpc_create_nano: 0,
            },
        );
        fbb.finish(nreq, None);
        let fb_bytes = fbb.finished_data();

        // Standalone buffer (no pool): the buffer is queued for send and
        // released by the I/O worker after this handler returns, so a
        // pool-allocated buffer would dangle when the local pool drops.
        let ctrl = Buffer::from_vec(fb_bytes.to_vec());

        // Send via request_client.call() on the server-side connection.
        // The client will drop the frame (no handler, no transport) →
        // the reaper will time out this pending entry.
        // Send via request_client.call_to_handle() on the raw
        // server-side connection handle. The client will drop the
        // frame (no handler, no transport) → the reaper will time
        // out this pending entry.
        if let Ok(fut) = request_client_for_handler.call_to_handle(
            &server_for_handler,
            req.conn_handle,
            nreq_id,
            ctrl,
            None,
            NO_HANDLER_MSG_TYPE,
        ) {
            *no_handler_future_for_handler.lock().unwrap() = Some(fut);
        }
    });

    // Client: attached to conn, NO handler for NO_HANDLER_MSG_TYPE,
    // NO set_transport (so unmatched frames are dropped, not responded
    // to with UnknownMessage — which would prevent the timeout).
    let client = RpcClient::new();

    server.start();

    let conn = server.connect("127.0.0.1", port).expect("connect failed");
    client.attach(&conn);

    // Client sends PING → server handler acks + sends no-handler request.
    let mut fbb = FlatBufferBuilder::new();
    let req = ConnectionPingRequest::create(
        &mut fbb,
        &ConnectionPingRequestArgs {
            id: 888,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(req, None);
    let fb_bytes = fbb.finished_data();

    let pool = BufferPool::new(256);
    let mut ctrl = pool.alloc_buffer(fb_bytes.len() as u32).expect("alloc control");
    ctrl.write(fb_bytes);

    let future = client
        .call(&server, &conn, 888, ctrl, None, PING_MSG_TYPE)
        .expect("call submit failed");

    let response = tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("timeout waiting for ping ack")
        .expect("callback dropped");

    assert_eq!(response.request_id, 888, "ping ack id should match");

    // The server's PING handler also sent a no-handler request to the
    // client. The client dropped it (no handler, no transport). The
    // request_client's reaper should time out the pending entry.
    let no_handler_fut = no_handler_future
        .lock()
        .unwrap()
        .take()
        .expect("no-handler CallFuture was not stored (handler did not fire?)");

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), no_handler_fut)
        .await
        .expect("timeout waiting for reaper to fire (reaper did not time out the entry?)");

    assert!(
        result.is_err(),
        "no-handler request should time out, got Ok: {result:?}"
    );
    assert_eq!(
        result.unwrap_err(),
        crow_rpc_ffi::RpcError::Timeout,
        "no-handler request should fail with Timeout (reaper fires, no client response)"
    );

    server.stop();
}
