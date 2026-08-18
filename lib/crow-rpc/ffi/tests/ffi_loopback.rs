// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for the crow-rpc-ffi crate.
//!
//! These tests exercise the full FFI loopback: Rust creates a server,
//! connects a client, and verifies the server accepts the connection.

use crow_rpc_ffi::{BufferPool, RpcServer};

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
