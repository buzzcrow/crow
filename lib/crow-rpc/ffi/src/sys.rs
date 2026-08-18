// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Raw FFI bindings to the crow-rpc C ABI.

#![allow(non_camel_case_types, non_snake_case, dead_code)]

use std::os::raw::{c_char, c_int, c_void};

pub type crow_rpc_pool_t = *mut crow_rpc_pool_s;
pub type crow_rpc_buffer_t = *mut crow_rpc_buffer_s;
pub type crow_rpc_conn_t = *mut crow_rpc_conn_s;
pub type crow_rpc_caller_t = *mut crow_rpc_caller_s;
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
pub struct crow_rpc_caller_s {
    _private: [u8; 0],
}
#[repr(C)]
pub struct crow_rpc_server_s {
    _private: [u8; 0],
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

extern "C" {
    pub fn crow_rpc_buffer_alloc(pool: crow_rpc_pool_t, capacity: u32) -> crow_rpc_buffer_t;
    pub fn crow_rpc_buffer_write(buf: crow_rpc_buffer_t, data: *const u8, len: u32);
    pub fn crow_rpc_buffer_ref(buf: crow_rpc_buffer_t) -> crow_rpc_buffer_t;
    pub fn crow_rpc_buffer_release(buf: crow_rpc_buffer_t);

    pub fn crow_rpc_pool_create(max_buffers: u32) -> crow_rpc_pool_t;
    pub fn crow_rpc_pool_destroy(pool: crow_rpc_pool_t);

    pub fn crow_rpc_server_create(pool: crow_rpc_pool_t) -> crow_rpc_server_t;
    pub fn crow_rpc_server_destroy(server: crow_rpc_server_t);
    pub fn crow_rpc_server_listen(
        server: crow_rpc_server_t,
        addr: *const c_char,
        port: c_int,
    ) -> crow_rpc_status;
    pub fn crow_rpc_server_start(server: crow_rpc_server_t);
    pub fn crow_rpc_server_stop(server: crow_rpc_server_t);
    pub fn crow_rpc_server_port(server: crow_rpc_server_t) -> c_int;

    pub fn crow_rpc_caller_create() -> crow_rpc_caller_t;
    pub fn crow_rpc_caller_destroy(caller: crow_rpc_caller_t);

    pub fn crow_rpc_caller_call(
        caller: crow_rpc_caller_t,
        server: crow_rpc_server_t,
        conn: crow_rpc_conn_t,
        control: crow_rpc_buffer_t,
        data: crow_rpc_buffer_t,
        msg_type: u16,
        on_complete: crow_rpc_on_complete,
        user_data: *mut c_void,
        out_request_id: *mut u64,
    ) -> crow_rpc_status;

    pub fn crow_rpc_caller_call_one_way(
        caller: crow_rpc_caller_t,
        server: crow_rpc_server_t,
        conn: crow_rpc_conn_t,
        control: crow_rpc_buffer_t,
        data: crow_rpc_buffer_t,
        msg_type: u16,
    ) -> crow_rpc_status;

    pub fn crow_rpc_connect(server: crow_rpc_server_t, addr: *const c_char, port: c_int) -> crow_rpc_conn_t;
}
