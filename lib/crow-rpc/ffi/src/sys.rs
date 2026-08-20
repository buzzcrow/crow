// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Raw FFI bindings to the crow-rpc C ABI.

#![allow(non_camel_case_types, non_snake_case, dead_code)]

use std::os::raw::{c_char, c_int, c_void};

pub type crow_rpc_pool_t = *mut crow_rpc_pool_s;
pub type crow_rpc_buffer_t = *mut crow_rpc_buffer_s;
pub type crow_rpc_conn_t = *mut crow_rpc_conn_s;
pub type crow_rpc_client_t = *mut crow_rpc_client_s;
pub type crow_rpc_server_t = *mut crow_rpc_server_s;

#[repr(C)]
pub struct crow_rpc_pool_s {
    _private: [u8; 0],
}
#[repr(C)]
pub struct crow_rpc_buffer_s {
    _private: [u8; 0],
}
#[repr(C)]
pub struct crow_rpc_conn_s {
    _private: [u8; 0],
}
#[repr(C)]
pub struct crow_rpc_client_s {
    _private: [u8; 0],
}
#[repr(C)]
pub struct crow_rpc_server_s {
    _private: [u8; 0],
}

#[repr(C)]
pub struct CrowRpcLatencyStats {
    pub count: u64,
    pub sum_ns: u64,
    pub min_ns: u64,
    pub max_ns: u64,
}

#[repr(C)]
pub struct CrowRpcTransportStats {
    pub read_calls: u64,
    pub writev_calls: u64,
    pub submit_to_writev: CrowRpcLatencyStats,
    pub read_to_dispatch: CrowRpcLatencyStats,
    pub dispatch_to_enq: CrowRpcLatencyStats,
}

#[repr(C)]
pub struct CrowRpcClientCounters {
    pub submit_ok: u64,
    pub submit_fail: u64,
    pub resp_matched: u64,
    pub resp_mismatch: u64,
    pub resp_wrong_id: u64,
    pub resp_dropped: u64,
    pub slab_fallback: u64,
    pub resp_map_matched: u64,
    pub reaped_slab: u64,
    pub reaped_map: u64,
}

pub type crow_rpc_status = i32;

pub const CROW_RPC_OK: crow_rpc_status = 0;
pub const CROW_RPC_ERR_CONN_CLOSED: crow_rpc_status = -1;
pub const CROW_RPC_ERR_TIMEOUT: crow_rpc_status = -2;
pub const CROW_RPC_ERR_SEND_QUEUE: crow_rpc_status = -3;
pub const CROW_RPC_ERR_CONN_ERROR: crow_rpc_status = -4;
pub const CROW_RPC_ERR_REGISTRATION: crow_rpc_status = -5;
pub const CROW_RPC_ERR_ALL_DOWN: crow_rpc_status = -6;
pub const CROW_RPC_ERR_INVALID_ARG: crow_rpc_status = -7;

pub type crow_rpc_on_complete = Option<
    unsafe extern "C" fn(
        request_id: u64,
        control: crow_rpc_buffer_t,
        data: crow_rpc_buffer_t,
        status: crow_rpc_status,
        user_data: *mut c_void,
    ),
>;

// ── Coroutine client (Option 3: C++ coroutine + Rust FFI) ────────
pub type crow_rpc_co_build_request = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        request_id: u64,
        out_control: *mut crow_rpc_buffer_t,
        out_data: *mut crow_rpc_buffer_t,
    ) -> bool,
>;

pub type crow_rpc_co_on_response = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        request_id: u64,
        control: crow_rpc_buffer_t,
        data: crow_rpc_buffer_t,
        status: crow_rpc_status,
        latency_ns: u64,
    ) -> bool,
>;

#[repr(C)]
pub struct CrowRpcCoStats {
    pub total_ops: u64,
    pub total_errors: u64,
    pub total_latency_ns: u64,
    pub min_latency_ns: u64,
    pub max_latency_ns: u64,
}

extern "C" {
    pub fn crow_rpc_buffer_alloc(pool: crow_rpc_pool_t, capacity: u32) -> crow_rpc_buffer_t;
    pub fn crow_rpc_buffer_write(buf: crow_rpc_buffer_t, data: *const u8, len: u32);
    pub fn crow_rpc_buffer_data(buf: crow_rpc_buffer_t) -> *const u8;
    pub fn crow_rpc_buffer_len(buf: crow_rpc_buffer_t) -> u32;
    pub fn crow_rpc_buffer_ref(buf: crow_rpc_buffer_t) -> crow_rpc_buffer_t;
    pub fn crow_rpc_buffer_release(buf: crow_rpc_buffer_t);

    pub fn crow_rpc_pool_create(max_buffers: u32) -> crow_rpc_pool_t;
    pub fn crow_rpc_pool_destroy(pool: crow_rpc_pool_t);

    pub fn crow_rpc_server_create(pool: crow_rpc_pool_t) -> crow_rpc_server_t;
    pub fn crow_rpc_server_create_with_workers(pool: crow_rpc_pool_t, num_workers: u32) -> crow_rpc_server_t;
    pub fn crow_rpc_server_create_with_engines(
        pool: crow_rpc_pool_t,
        io_engines: u32,
        workers_per_engine: u32,
    ) -> crow_rpc_server_t;
    pub fn crow_rpc_server_destroy(server: crow_rpc_server_t);
    pub fn crow_rpc_server_listen(
        server: crow_rpc_server_t,
        addr: *const c_char,
        port: c_int,
    ) -> crow_rpc_status;
    pub fn crow_rpc_server_start(server: crow_rpc_server_t);
    pub fn crow_rpc_server_stop(server: crow_rpc_server_t);
    pub fn crow_rpc_server_port(server: crow_rpc_server_t) -> c_int;

    pub fn crow_rpc_server_transport_stats(server: crow_rpc_server_t, out: *mut CrowRpcTransportStats);

    pub fn crow_rpc_client_get_counters(client: crow_rpc_client_t, out: *mut CrowRpcClientCounters);

    pub fn crow_rpc_client_create() -> crow_rpc_client_t;
    pub fn crow_rpc_client_destroy(client: crow_rpc_client_t);
    pub fn crow_rpc_client_attach(client: crow_rpc_client_t, conn: crow_rpc_conn_t);

    pub fn crow_rpc_client_call(
        client: crow_rpc_client_t,
        server: crow_rpc_server_t,
        conn: crow_rpc_conn_t,
        control: crow_rpc_buffer_t,
        data: crow_rpc_buffer_t,
        msg_type: u16,
        on_complete: crow_rpc_on_complete,
        user_data: *mut c_void,
        out_request_id: *mut u64,
    ) -> crow_rpc_status;

    pub fn crow_rpc_client_set_completion_pool_size(client: crow_rpc_client_t, max_in_flight: u32);

    pub fn crow_rpc_client_start_reaper(client: crow_rpc_client_t, timeout_ns: u64, scan_interval_ns: u64);

    pub fn crow_rpc_client_stop_reaper(client: crow_rpc_client_t);

    pub fn crow_rpc_client_call_callback(
        client: crow_rpc_client_t,
        server: crow_rpc_server_t,
        conn: crow_rpc_conn_t,
        request_id: u64,
        control: crow_rpc_buffer_t,
        data: crow_rpc_buffer_t,
        msg_type: u16,
        on_complete: crow_rpc_on_complete,
        user_data: *mut c_void,
    ) -> crow_rpc_status;

    pub fn crow_rpc_client_call_one_way(
        client: crow_rpc_client_t,
        server: crow_rpc_server_t,
        conn: crow_rpc_conn_t,
        control: crow_rpc_buffer_t,
        data: crow_rpc_buffer_t,
        msg_type: u16,
    ) -> crow_rpc_status;

    pub fn crow_rpc_connect(server: crow_rpc_server_t, addr: *const c_char, port: c_int) -> crow_rpc_conn_t;

    pub fn crow_rpc_server_register_echo_handler(server: crow_rpc_server_t, msg_type: u16);

    pub fn crow_rpc_server_submit_response(
        server: crow_rpc_server_t,
        conn_handle: *mut c_void,
        control: *const u8,
        control_len: u32,
        data: *const u8,
        data_len: u32,
        msg_type: u16,
        request_id: u64,
    ) -> crow_rpc_status;

    // ── Coroutine client (Option 3: C++ coroutine + Rust FFI) ────
    pub fn crow_rpc_co_spawn(
        client: crow_rpc_client_t,
        server: crow_rpc_server_t,
        conns: *const crow_rpc_conn_t,
        num_conns: usize,
        num_coroutines: u32,
        msg_type: u16,
        build_request: crow_rpc_co_build_request,
        on_response: crow_rpc_co_on_response,
        ctx: *mut c_void,
    );

    pub fn crow_rpc_co_get_stats(client: crow_rpc_client_t, out: *mut CrowRpcCoStats);
}
