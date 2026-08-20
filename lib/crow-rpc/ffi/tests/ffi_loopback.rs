// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for the crow-rpc-ffi crate.
//!
//! These tests exercise the full FFI loopback: Rust creates a server,
//! connects a client, sends a ping request, and verifies the response.

use crow_protocol::fb::{ConnectionPingRequest, ConnectionPingRequestArgs, FBMsgType};
use crow_rpc_ffi::{BufferPool, RpcClient, RpcServer};
use flatbuffers::FlatBufferBuilder;

#[test]
fn server_create_listen_start_stop() {
    let server = RpcServer::new(None);
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0, "server should have a valid port");

    server.start();
    // Give the acceptor thread time to start.
    std::thread::sleep(std::time::Duration::from_millis(50));
    server.stop();
}

#[test]
fn buffer_pool_alloc_write_release() {
    let pool = BufferPool::new(16);
    let mut buf = BufferPool::alloc_buffer(&pool, 32).expect("alloc failed");
    buf.write(&[0x42; 32]);
    // buf is dropped here — should not crash.
}

#[test]
fn server_connect_to_peer() {
    let server = RpcServer::new(None);
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0);

    server.start();
    std::thread::sleep(std::time::Duration::from_millis(50));

    // Connect to ourselves (loopback).
    let conn = server.connect("127.0.0.1", port);
    assert!(conn.is_ok(), "connect should succeed");

    server.stop();
}

// ── Full ping loopback via FFI ─────────────────────────────────────
// Build a ConnectionPingRequest flatbuffer on the Rust side, submit it
// through the FFI, and verify the ConnectionPingResponse comes back with
// the matching request id.
#[tokio::test]
async fn ping_loopback() {
    let server = RpcServer::new(None);
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0);

    server.start();
    std::thread::sleep(std::time::Duration::from_millis(50));

    let conn = server.connect("127.0.0.1", port).expect("connect failed");

    // Build a ConnectionPingRequest flatbuffer.
    let mut fbb = FlatBufferBuilder::new();
    let req = ConnectionPingRequest::create(
        &mut fbb,
        &ConnectionPingRequestArgs {
            id: 42,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(req, None);
    let fb_bytes = fbb.finished_data();

    // Allocate a buffer from the server's internal pool and copy the
    // flatbuffer into it. The FFI takes ownership of the buffer.
    let pool = BufferPool::new(256);
    let mut ctrl = pool.alloc_buffer(fb_bytes.len() as u32).expect("alloc control");
    ctrl.write(fb_bytes);

    let caller = RpcClient::new();
    caller.attach(&conn);
    let future = caller
        .call(
            &server,
            &conn,
            ctrl,
            None,
            FBMsgType::EConnectionPingRequest.0 as u16,
        )
        .expect("call submit failed");

    // Await the response (10s timeout).
    let response = tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("timeout waiting for response")
        .expect("callback dropped");

    // With zero-copy parse, control fields are extracted during parse
    // and the control buffer is discarded. request_id is in the Response.
    assert_eq!(response.request_id, 42, "response id should match request id");

    server.stop();
}

// ── Ping loopback with data payload ───────────────────────────────
// Send a ping request with 512-byte data and verify the response.
// The built-in ping handler echoes back a ConnectionPingResponse
// (no data), so we just verify we get a response.
#[tokio::test]
async fn ping_loopback_with_data() {
    let server = RpcServer::new(None);
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0);

    server.start();
    std::thread::sleep(std::time::Duration::from_millis(50));

    let conn = server.connect("127.0.0.1", port).expect("connect failed");

    // Build a ConnectionPingRequest flatbuffer.
    let mut fbb = FlatBufferBuilder::new();
    let req = ConnectionPingRequest::create(
        &mut fbb,
        &ConnectionPingRequestArgs {
            id: 99,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(req, None);
    let fb_bytes = fbb.finished_data();

    let pool = BufferPool::new(256);
    let mut ctrl = pool.alloc_buffer(fb_bytes.len() as u32).expect("alloc control");
    ctrl.write(fb_bytes);

    // 512-byte data payload.
    let payload: Vec<u8> = (0..512).map(|i| (i % 256) as u8).collect();
    let mut data = pool.alloc_buffer(512).expect("alloc data");
    data.write(&payload);

    let caller = RpcClient::new();
    caller.attach(&conn);
    let future = caller
        .call(
            &server,
            &conn,
            ctrl,
            Some(data),
            FBMsgType::EConnectionPingRequest.0 as u16,
        )
        .expect("call submit failed");

    let response = tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("timeout waiting for response")
        .expect("callback dropped");

    // The built-in ping handler returns a ConnectionPingResponse with
    // no data. With zero-copy parse, request_id is in the Response.
    assert_eq!(response.request_id, 99, "response id should match request id");

    server.stop();
}

// ── Echo handler loopback ─────────────────────────────────────────
// Register the built-in echo handler for a custom msg_type, send a
// request with data payload, and verify the response data matches.
#[tokio::test]
async fn echo_handler_loopback() {
    const ECHO_MSG_TYPE: u16 = 100;
    const DATA_SIZE: usize = 512;

    let server = RpcServer::new(None);
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0);

    // Register the built-in echo handler for our custom msg_type.
    server.register_echo_handler(ECHO_MSG_TYPE);

    server.start();
    std::thread::sleep(std::time::Duration::from_millis(50));

    let conn = server.connect("127.0.0.1", port).expect("connect failed");

    // Build a ConnectionPingRequest flatbuffer (the echo handler
    // extracts the request_id from it and echoes it back in the
    // ConnectionPingResponse control buffer).
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

    // 512-byte data payload — the echo handler should return it verbatim.
    let payload: Vec<u8> = (0..DATA_SIZE).map(|i| (i % 256) as u8).collect();
    let mut data = pool.alloc_buffer(DATA_SIZE as u32).expect("alloc data");
    data.write(&payload);

    let caller = RpcClient::new();
    caller.attach(&conn);
    let future = caller
        .call(&server, &conn, ctrl, Some(data), ECHO_MSG_TYPE)
        .expect("call submit failed");

    let response = tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("timeout waiting for response")
        .expect("callback dropped");

    // Verify the echoed request_id (extracted during parse).
    assert_eq!(response.request_id, 777, "response id should match request id");

    // Verify the data buffer matches the request payload (echo).
    assert!(response.data.is_some(), "response should have data");
    let data_buf = response.data.unwrap();
    assert_eq!(
        data_buf.bytes().len(),
        DATA_SIZE,
        "response data length should match"
    );
    assert_eq!(
        data_buf.bytes(),
        &payload[..],
        "response data should match request data"
    );

    server.stop();
}
