// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Raw FFI bindings to the crowdb-rpc C ABI.

#![allow(non_camel_case_types, non_snake_case, dead_code)]

use std::os::raw::{c_char, c_int, c_void};

pub type crowdb_rpc_pool_t = *mut crowdb_rpc_pool_s;
pub type crowdb_rpc_buffer_t = *mut crowdb_rpc_buffer_s;
pub type crowdb_rpc_conn_t = *mut crowdb_rpc_conn_s;
pub type crowdb_rpc_client_t = *mut crowdb_rpc_client_s;
pub type crowdb_rpc_server_t = *mut crowdb_rpc_server_s;

#[repr(C)]
pub struct crowdb_rpc_pool_s {
    _private: [u8; 0],
}
#[repr(C)]
pub struct crowdb_rpc_buffer_s {
    _private: [u8; 0],
}
#[repr(C)]
pub struct crowdb_rpc_conn_s {
    _private: [u8; 0],
}
#[repr(C)]
pub struct crowdb_rpc_client_s {
    _private: [u8; 0],
}
#[repr(C)]
pub struct crowdb_rpc_server_s {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Default, Debug, Clone, Copy)]
pub struct CrowdbRpcLatencyStats {
    pub count: u64,
    pub sum_ns: u64,
    pub min_ns: u64,
    pub max_ns: u64,
}

#[repr(C)]
#[derive(Default, Debug, Clone, Copy)]
pub struct CrowdbRpcTransportStats {
    pub submit_to_writev: CrowdbRpcLatencyStats,
    pub send_queue_rejects: u64,
}

#[repr(C)]
#[derive(Default, Debug)]
pub struct CrowdbRpcClientCounters {
    pub submit_fail: u64,
    pub submit_retry: u64,
    pub resp_missed: u64,
    pub reaped: u64,
}

pub type crowdb_rpc_status = i32;

pub const CROWDB_RPC_OK: crowdb_rpc_status = 0;
pub const CROWDB_RPC_ERR_CONN_CLOSED: crowdb_rpc_status = -1;
pub const CROWDB_RPC_ERR_TIMEOUT: crowdb_rpc_status = -2;
pub const CROWDB_RPC_ERR_SEND_QUEUE: crowdb_rpc_status = -3;
pub const CROWDB_RPC_ERR_CONN_ERROR: crowdb_rpc_status = -4;
pub const CROWDB_RPC_ERR_REGISTRATION: crowdb_rpc_status = -5;
pub const CROWDB_RPC_ERR_ALL_DOWN: crowdb_rpc_status = -6;
pub const CROWDB_RPC_ERR_INVALID_ARG: crowdb_rpc_status = -7;

pub type crowdb_rpc_on_complete = Option<
    unsafe extern "C" fn(
        request_id: u64,
        control: crowdb_rpc_buffer_t,
        data: crowdb_rpc_buffer_t,
        status: crowdb_rpc_status,
        user_data: *mut c_void,
    ),
>;

// ── Custom server handler dispatch (R115: Rust server handlers) ──
pub type crowdb_rpc_handler_fn = Option<
    unsafe extern "C" fn(
        request_id: u64,
        rpc_create_nano: u64,
        msg_type: u16,
        control: *const u8,
        control_len: u32,
        data: *const u8,
        data_len: u32,
        conn_handle: *mut c_void,
        frame_handle: *mut c_void,
        user_data: *mut c_void,
    ),
>;

// ── Coroutine client (Option 3: C++ coroutine + Rust FFI) ────────
pub type crowdb_rpc_co_build_request = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        request_id: u64,
        out_control: *mut crowdb_rpc_buffer_t,
        out_data: *mut crowdb_rpc_buffer_t,
    ) -> bool,
>;

pub type crowdb_rpc_co_on_response = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        request_id: u64,
        control: crowdb_rpc_buffer_t,
        data: crowdb_rpc_buffer_t,
        status: crowdb_rpc_status,
        latency_ns: u64,
    ) -> bool,
>;

extern "C" {
    pub fn crowdb_rpc_buffer_alloc(pool: crowdb_rpc_pool_t, capacity: u32) -> crowdb_rpc_buffer_t;
    pub fn crowdb_rpc_buffer_write(buf: crowdb_rpc_buffer_t, data: *const u8, len: u32);
    pub fn crowdb_rpc_buffer_data(buf: crowdb_rpc_buffer_t) -> *const u8;
    pub fn crowdb_rpc_buffer_len(buf: crowdb_rpc_buffer_t) -> u32;
    pub fn crowdb_rpc_buffer_ref(buf: crowdb_rpc_buffer_t) -> crowdb_rpc_buffer_t;
    pub fn crowdb_rpc_buffer_release(buf: crowdb_rpc_buffer_t);
    pub fn crowdb_rpc_buffer_create(data: *const u8, len: u32) -> crowdb_rpc_buffer_t;
    pub fn crowdb_rpc_buffer_create_external(
        data: *const u8,
        len: u32,
        free_cb: Option<extern "C" fn(ctx: *mut std::ffi::c_void)>,
        free_ctx: *mut std::ffi::c_void,
    ) -> crowdb_rpc_buffer_t;

    pub fn crowdb_rpc_pool_create(max_buffers: u32) -> crowdb_rpc_pool_t;
    pub fn crowdb_rpc_pool_destroy(pool: crowdb_rpc_pool_t);

    pub fn crowdb_rpc_server_create(pool: crowdb_rpc_pool_t) -> crowdb_rpc_server_t;
    pub fn crowdb_rpc_server_create_with_workers(
        pool: crowdb_rpc_pool_t,
        num_workers: u32,
    ) -> crowdb_rpc_server_t;
    pub fn crowdb_rpc_server_create_with_engines(
        pool: crowdb_rpc_pool_t,
        io_engines: u32,
        io_workers: u32,
    ) -> crowdb_rpc_server_t;
    pub fn crowdb_rpc_server_destroy(server: crowdb_rpc_server_t);
    pub fn crowdb_rpc_server_clear_handlers(server: crowdb_rpc_server_t);
    pub fn crowdb_rpc_server_listen(
        server: crowdb_rpc_server_t,
        addr: *const c_char,
        port: c_int,
    ) -> crowdb_rpc_status;
    pub fn crowdb_rpc_server_start(server: crowdb_rpc_server_t);
    pub fn crowdb_rpc_server_stop(server: crowdb_rpc_server_t);
    pub fn crowdb_rpc_server_port(server: crowdb_rpc_server_t) -> c_int;
    pub fn crowdb_rpc_server_set_send_queue_capacity(server: crowdb_rpc_server_t, capacity: u32);
    pub fn crowdb_rpc_server_set_tcp_nodelay(server: crowdb_rpc_server_t, enabled: c_int);
    pub fn crowdb_rpc_server_set_quickack(server: crowdb_rpc_server_t, enabled: c_int);
    pub fn crowdb_rpc_server_set_event_write(server: crowdb_rpc_server_t, enabled: c_int);

    pub fn crowdb_rpc_server_transport_stats(server: crowdb_rpc_server_t, out: *mut CrowdbRpcTransportStats);

    pub fn crowdb_rpc_client_get_counters(client: crowdb_rpc_client_t, out: *mut CrowdbRpcClientCounters);

    pub fn crowdb_rpc_client_create() -> crowdb_rpc_client_t;
    pub fn crowdb_rpc_client_destroy(client: crowdb_rpc_client_t);
    pub fn crowdb_rpc_client_attach(client: crowdb_rpc_client_t, conn: crowdb_rpc_conn_t);

    pub fn crowdb_rpc_client_set_completion_pool_size(client: crowdb_rpc_client_t, max_in_flight: u32);

    pub fn crowdb_rpc_client_start_reaper(
        client: crowdb_rpc_client_t,
        timeout_ns: u64,
        scan_interval_ns: u64,
    );

    pub fn crowdb_rpc_client_stop_reaper(client: crowdb_rpc_client_t);

    pub fn crowdb_rpc_client_dump_pending(client: crowdb_rpc_client_t);

    pub fn crowdb_rpc_client_send(
        client: crowdb_rpc_client_t,
        server: crowdb_rpc_server_t,
        conn: crowdb_rpc_conn_t,
        request_id: u64,
        control: crowdb_rpc_buffer_t,
        data: crowdb_rpc_buffer_t,
        msg_type: u16,
        on_complete: crowdb_rpc_on_complete,
        user_data: *mut c_void,
    ) -> crowdb_rpc_status;

    pub fn crowdb_rpc_client_send_conn(
        client: crowdb_rpc_client_t,
        server: crowdb_rpc_server_t,
        conn_handle: *mut c_void,
        request_id: u64,
        control: crowdb_rpc_buffer_t,
        data: crowdb_rpc_buffer_t,
        msg_type: u16,
        on_complete: crowdb_rpc_on_complete,
        user_data: *mut c_void,
    ) -> crowdb_rpc_status;

    pub fn crowdb_rpc_connect(
        server: crowdb_rpc_server_t,
        addr: *const c_char,
        port: c_int,
    ) -> crowdb_rpc_conn_t;

    pub fn crowdb_rpc_conn_destroy(conn: crowdb_rpc_conn_t);

    pub fn crowdb_rpc_server_register_echo_handler(server: crowdb_rpc_server_t, msg_type: u16);

    pub fn crowdb_rpc_frame_release(frame_handle: *mut c_void);

    pub fn crowdb_rpc_server_register_handler(
        server: crowdb_rpc_server_t,
        msg_type: u16,
        callback: crowdb_rpc_handler_fn,
        user_data: *mut c_void,
    );

    pub fn crowdb_rpc_server_submit_response(
        server: crowdb_rpc_server_t,
        conn_handle: *mut c_void,
        control: *const u8,
        control_len: u32,
        data: *const u8,
        data_len: u32,
        msg_type: u16,
        request_id: u64,
    ) -> crowdb_rpc_status;

    pub fn crowdb_rpc_server_submit_response_buffer(
        server: crowdb_rpc_server_t,
        conn_handle: *mut c_void,
        control: crowdb_rpc_buffer_t,
        data: crowdb_rpc_buffer_t,
        msg_type: u16,
        request_id: u64,
    ) -> crowdb_rpc_status;

    // ── Client-side request handler dispatch (R114) ───────────────
    pub fn crowdb_rpc_client_register_handler(
        client: crowdb_rpc_client_t,
        msg_type: u16,
        callback: crowdb_rpc_handler_fn,
        user_data: *mut c_void,
    );
    pub fn crowdb_rpc_client_clear_handlers(client: crowdb_rpc_client_t);

    pub fn crowdb_rpc_client_set_transport(client: crowdb_rpc_client_t, server: crowdb_rpc_server_t);

    // ── Server-side request-response correlation (R114) ───────────
    pub fn crowdb_rpc_server_set_request_client(server: crowdb_rpc_server_t, client: crowdb_rpc_client_t);

    // ── Logging (mirrors crowdb-tree ct_*_logging) ─────────────────
    pub fn crowdb_rpc_init_logging(
        log_dir: *const c_char,
        level: *const c_char,
        max_file_mb: usize,
        max_files: usize,
        file_prefix: *const c_char,
    );
    pub fn crowdb_rpc_flush_logging();
    pub fn crowdb_rpc_add_log_stderr(level: *const c_char);
    pub fn crowdb_rpc_shutdown_logging();
    pub fn crowdb_rpc_metrics_start(
        log_path: *const c_char,
        interval_secs: f64,
        max_file_mb: usize,
        max_files: usize,
        console: c_int,
    );
    pub fn crowdb_rpc_metrics_stop();
    pub fn crowdb_rpc_server_register_conn_count_gauge(server: crowdb_rpc_server_t, name: *const c_char);

    // ── C++ global metrics registry (crowdb-common) ────────────────
    pub fn crowdb_common_metrics_global_flush(
        window_secs: f64,
        timestamp: *const c_char,
        section_label: *const c_char,
        width: usize,
        count_w: usize,
        tps_w: usize,
    ) -> *mut c_char;
    pub fn crowdb_common_metrics_global_max_name_len() -> usize;
    pub fn crowdb_common_metrics_global_free(s: *mut c_char);

    // ── Coroutine client (Option 3: C++ coroutine + Rust FFI) ────
    pub fn crowdb_rpc_co_spawn(
        client: crowdb_rpc_client_t,
        server: crowdb_rpc_server_t,
        conns: *const crowdb_rpc_conn_t,
        num_conns: usize,
        num_coroutines: u32,
        msg_type: u16,
        build_request: crowdb_rpc_co_build_request,
        on_response: crowdb_rpc_co_on_response,
        ctx: *mut c_void,
    );
}
