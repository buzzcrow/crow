// Copyright 2026-present buzzcrow <buzzcrow@126.com>.
// Licensed under the Apache License, Version 2.0.

//! Integration tests for the custom Rust dispatch handler path
//! (`RpcServer::register_handler`), added for R115 so Rust servers
//! (diskdb, KV consensus, KvService) can register handlers without
//! writing a C++ `HandlerFn`.

use crow_protocol::fb::{
    ConnectionPingRequest, ConnectionPingRequestArgs, ConnectionPingResponse, ConnectionPingResponseArgs,
    FBMsgType, FBRetCode,
};
use crow_rpc_ffi::{BufferPool, RpcClient, RpcServer};
use flatbuffers::FlatBufferBuilder;
use std::sync::Arc;

// Register a custom Rust handler that builds a ConnectionPingResponse
// and submits it via submit_response, then verify a client round-trip.
#[tokio::test]
async fn custom_rust_handler_loopback() {
    const HANDLER_MSG_TYPE: u16 = 200;

    let server = Arc::new(RpcServer::new(None));
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0);

    // Capture a cloned Arc so the handler can submit the response.
    let server_for_handler = server.clone();
    server.register_handler(HANDLER_MSG_TYPE, move |req| {
        // Build a ConnectionPingResponse control buffer echoing the
        // request_id (the framing layer already extracted it).
        let mut fbb = FlatBufferBuilder::new();
        let resp = ConnectionPingResponse::create(
            &mut fbb,
            &ConnectionPingResponseArgs {
                id: req.request_id,
                rpc_create_nano: req.rpc_create_nano,
                ret: FBRetCode::Success,
            },
        );
        fbb.finish(resp, None);
        let ctrl_bytes = fbb.finished_data().to_vec();
        let resp_msg_type = FBMsgType::EConnectionPingResponse.0 as u16;
        // Submit the response from this C++ I/O thread (synchronous
        // submit — no async work needed for the ping echo).
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

    server.start();

    let conn = server.connect("127.0.0.1", port).expect("connect failed");

    // Build a ConnectionPingRequest flatbuffer.
    let mut fbb = FlatBufferBuilder::new();
    let req = ConnectionPingRequest::create(
        &mut fbb,
        &ConnectionPingRequestArgs {
            id: 4242,
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
        .call(&server, &conn, 4242, ctrl, None, HANDLER_MSG_TYPE)
        .expect("call submit failed");

    let response = tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("timeout waiting for response")
        .expect("callback dropped");

    assert_eq!(response.request_id, 4242, "response id should match request id");

    server.stop();
}

// Verify the custom handler receives the data payload for control+data
// requests and can echo it back in the response.
#[tokio::test]
async fn custom_rust_handler_with_data() {
    const HANDLER_MSG_TYPE: u16 = 201;
    const DATA_SIZE: usize = 256;

    let server = Arc::new(RpcServer::new(None));
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0);

    let server_for_handler = server.clone();
    server.register_handler(HANDLER_MSG_TYPE, move |req| {
        // Echo the request data back in the response.
        let mut fbb = FlatBufferBuilder::new();
        let resp = ConnectionPingResponse::create(
            &mut fbb,
            &ConnectionPingResponseArgs {
                id: req.request_id,
                rpc_create_nano: req.rpc_create_nano,
                ret: FBRetCode::Success,
            },
        );
        fbb.finish(resp, None);
        let ctrl_bytes = fbb.finished_data().to_vec();
        let data_bytes = req.data().map(|d| d.to_vec());
        let resp_msg_type = FBMsgType::EConnectionPingResponse.0 as u16;
        unsafe {
            let _ = server_for_handler.submit_response(
                req.conn_handle,
                &ctrl_bytes,
                data_bytes.as_deref(),
                resp_msg_type,
                req.request_id,
            );
        }
    });

    server.start();

    let conn = server.connect("127.0.0.1", port).expect("connect failed");

    let mut fbb = FlatBufferBuilder::new();
    let req = ConnectionPingRequest::create(
        &mut fbb,
        &ConnectionPingRequestArgs {
            id: 9_999,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(req, None);
    let fb_bytes = fbb.finished_data();

    let pool = BufferPool::new(256);
    let mut ctrl = pool.alloc_buffer(fb_bytes.len() as u32).expect("alloc control");
    ctrl.write(fb_bytes);

    let payload: Vec<u8> = (0..DATA_SIZE).map(|i| (i % 256) as u8).collect();
    let mut data = pool.alloc_buffer(DATA_SIZE as u32).expect("alloc data");
    data.write(&payload);

    let caller = RpcClient::new();
    caller.attach(&conn);
    let future = caller
        .call(&server, &conn, 9_999, ctrl, Some(data), HANDLER_MSG_TYPE)
        .expect("call submit failed");

    let response = tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("timeout waiting for response")
        .expect("callback dropped");

    assert_eq!(response.request_id, 9_999, "response id should match");
    assert!(response.data.is_some(), "response should echo data");
    let data_buf = response.data.unwrap();
    assert_eq!(data_buf.bytes(), &payload[..], "echoed data should match");

    server.stop();
}
