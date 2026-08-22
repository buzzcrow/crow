// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! C ABI wrapper around `HardwareClient` and `ServiceRegistryClient`
//! for C++ consumers (primarily `crow-diskio`).
//!
//! The crate manages a dedicated tokio runtime on a background thread.
//! All async operations are exposed via a callback pattern: the C++
//! caller provides a function pointer + `user_data`; when the async op
//! completes, the callback is invoked with a status code and a JSON
//! result string (or null on error).
//!
//! Complex types (`DiskValue`, `DiskdbOwnerEntry`, etc.) are serialized
//! to JSON for transport across the C ABI boundary.

#![allow(clippy::missing_safety_doc, clippy::needless_pass_by_value)]
#![allow(non_camel_case_types, non_snake_case, static_mut_refs)]
#![allow(unsafe_code)]

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::sync::Arc;

use crate::{ClientConfig, CrowkvClient, HardwareClient, ServiceRegistryClient};
use crow_protocol::common::DiskGroupUsageSummary;

// ── Opaque handle types ───────────────────────────────────────────

pub type crow_hw_client_t = *mut c_void;
pub type crow_svc_client_t = *mut c_void;

// ── Callback types ────────────────────────────────────────────────

/// Callback for async operations that return a JSON string.
/// `status`: 0 = OK, negative = error.
/// `result_json`: null-terminated JSON string (null on error). The
/// caller must NOT free this pointer — it is valid only during the
/// callback.
/// `user_data`: opaque pointer passed by the caller.
pub type crow_kv_on_complete =
    extern "C" fn(status: c_int, result_json: *const c_char, user_data: *mut c_void);

// Wrapper to send raw pointers across thread boundaries.
struct SendPtr(usize);
struct SendCallback(usize);

// ── Tokio runtime management ──────────────────────────────────────

struct FfiRuntime {
    rt: tokio::runtime::Runtime,
}

impl FfiRuntime {
    fn new() -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to create tokio runtime");
        Self { rt }
    }
}

static mut G_RUNTIME: Option<Arc<FfiRuntime>> = None;

fn runtime() -> Arc<FfiRuntime> {
    unsafe {
        if G_RUNTIME.is_none() {
            G_RUNTIME = Some(Arc::new(FfiRuntime::new()));
        }
        let opt_ref = &*std::ptr::addr_of!(G_RUNTIME);
        Arc::clone(opt_ref.as_ref().unwrap())
    }
}

// ── Helper: spawn an async op and invoke callback ─────────────────

fn spawn_op<F, Fut>(callback: crow_kv_on_complete, user_data: *mut c_void, f: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<String, String>> + Send + 'static,
{
    let cb = SendCallback(callback as usize);
    let ud = SendPtr(user_data as usize);
    let rt = runtime();
    rt.rt.spawn(async move {
        let result = f().await;
        let cb_fn: crow_kv_on_complete = unsafe { std::mem::transmute(cb.0) };
        let ud_ptr = ud.0 as *mut c_void;
        match result {
            Ok(json) => {
                let c_str = CString::new(json).unwrap_or_else(|_| CString::new("{}").unwrap());
                cb_fn(0, c_str.as_ptr(), ud_ptr);
            }
            Err(err) => {
                cb_fn(-1, ptr::null(), ud_ptr);
                eprintln!("crow-kv-client (ffi): op failed: {err}");
            }
        }
    });
}

// ── HardwareClient FFI ────────────────────────────────────────────

/// Create a `HardwareClient` from kv-server management seeds.
/// `seeds`: array of null-terminated C strings (e.g. "<http://127.0.0.1:9910>").
/// `num_seeds`: number of seeds.
/// Returns an opaque handle, or null on error.
#[no_mangle]
pub unsafe extern "C" fn crow_hw_client_create(
    seeds: *const *const c_char,
    num_seeds: usize,
) -> crow_hw_client_t {
    if seeds.is_null() {
        return ptr::null_mut();
    }
    let seed_vec: Vec<String> = (0..num_seeds)
        .filter_map(|i| {
            let s = *seeds.add(i);
            if s.is_null() {
                None
            } else {
                CStr::from_ptr(s).to_str().ok().map(String::from)
            }
        })
        .collect();
    if seed_vec.is_empty() {
        return ptr::null_mut();
    }
    let kv = CrowkvClient::new(ClientConfig::new(seed_vec));
    let hw = HardwareClient::from_shared(Arc::new(kv));
    Box::into_raw(Box::new(hw)) as crow_hw_client_t
}

/// Destroy a `HardwareClient` handle.
#[no_mangle]
pub unsafe extern "C" fn crow_hw_client_destroy(client: crow_hw_client_t) {
    if !client.is_null() {
        drop(Box::from_raw(client.cast::<HardwareClient>()));
    }
}

/// List disks in a disk-group. Returns a JSON array of
/// `{"disk_id": {...}, "value": {...}}` objects.
#[no_mangle]
pub unsafe extern "C" fn crow_hw_list_disks_in_group(
    client: crow_hw_client_t,
    rack_id: u64,
    node_id: u64,
    dg_id: u64,
    callback: crow_kv_on_complete,
    user_data: *mut c_void,
) {
    if client.is_null() {
        callback(-1, ptr::null(), user_data);
        return;
    }
    let hw = (*(client as *const HardwareClient)).clone();
    spawn_op(callback, user_data, move || async move {
        let disks = hw
            .list_disks_in_group(rack_id, node_id, dg_id)
            .await
            .map_err(|e| e.to_string())?;
        let json_arr: Vec<serde_json::Value> = disks
            .into_iter()
            .map(|(did, val)| {
                serde_json::json!({
                    "disk_id": {"high": did.high, "low": did.low},
                    "value": val,
                })
            })
            .collect();
        Ok(serde_json::to_string(&json_arr).unwrap_or_else(|_| "[]".to_string()))
    });
}

/// List all diskdb ownership entries. Returns a JSON array of
/// `DiskdbOwnerEntry` objects.
#[no_mangle]
pub unsafe extern "C" fn crow_hw_list_owners(
    client: crow_hw_client_t,
    callback: crow_kv_on_complete,
    user_data: *mut c_void,
) {
    if client.is_null() {
        callback(-1, ptr::null(), user_data);
        return;
    }
    let hw = (*(client as *const HardwareClient)).clone();
    spawn_op(callback, user_data, move || async move {
        let owners = hw.list_owners().await.map_err(|e| e.to_string())?;
        Ok(serde_json::to_string(&owners).unwrap_or_else(|_| "[]".to_string()))
    });
}

/// List all KV-group bind entries. Returns a JSON array of
/// `KVGroupBindEntry` objects.
#[no_mangle]
pub unsafe extern "C" fn crow_hw_list_binds(
    client: crow_hw_client_t,
    callback: crow_kv_on_complete,
    user_data: *mut c_void,
) {
    if client.is_null() {
        callback(-1, ptr::null(), user_data);
        return;
    }
    let hw = (*(client as *const HardwareClient)).clone();
    spawn_op(callback, user_data, move || async move {
        let binds = hw.list_binds().await.map_err(|e| e.to_string())?;
        Ok(serde_json::to_string(&binds).unwrap_or_else(|_| "[]".to_string()))
    });
}

// ── ServiceRegistryClient FFI ─────────────────────────────────────

/// Create a `ServiceRegistryClient` from kv-server management seeds.
#[no_mangle]
pub unsafe extern "C" fn crow_svc_client_create(
    seeds: *const *const c_char,
    num_seeds: usize,
) -> crow_svc_client_t {
    if seeds.is_null() {
        return ptr::null_mut();
    }
    let seed_vec: Vec<String> = (0..num_seeds)
        .filter_map(|i| {
            let s = *seeds.add(i);
            if s.is_null() {
                None
            } else {
                CStr::from_ptr(s).to_str().ok().map(String::from)
            }
        })
        .collect();
    if seed_vec.is_empty() {
        return ptr::null_mut();
    }
    let kv = CrowkvClient::new(ClientConfig::new(seed_vec));
    let svc = ServiceRegistryClient::from_shared(Arc::new(kv));
    Box::into_raw(Box::new(svc)) as crow_svc_client_t
}

/// Destroy a `ServiceRegistryClient` handle.
#[no_mangle]
pub unsafe extern "C" fn crow_svc_client_destroy(client: crow_svc_client_t) {
    if !client.is_null() {
        drop(Box::from_raw(client.cast::<ServiceRegistryClient>()));
    }
}

/// Heartbeat a diskio instance. `owned_dg_ids_json` is a JSON array
/// of u64 disk-group IDs. `group_usages_json` is a JSON array of
/// `DiskGroupUsageSummary` objects (can be empty "[]").
#[no_mangle]
pub unsafe extern "C" fn crow_svc_heartbeat_diskio(
    client: crow_svc_client_t,
    instance_id: u64,
    grpc_endpoint: *const c_char,
    owned_dg_ids_json: *const c_char,
    group_usages_json: *const c_char,
    callback: crow_kv_on_complete,
    user_data: *mut c_void,
) {
    if client.is_null() || grpc_endpoint.is_null() {
        callback(-1, ptr::null(), user_data);
        return;
    }
    let svc = (*(client as *const ServiceRegistryClient)).clone();
    let endpoint = if let Ok(s) = CStr::from_ptr(grpc_endpoint).to_str() {
        s.to_string()
    } else {
        callback(-1, ptr::null(), user_data);
        return;
    };
    let dg_ids: Vec<u64> = if owned_dg_ids_json.is_null() {
        Vec::new()
    } else {
        match CStr::from_ptr(owned_dg_ids_json).to_str() {
            Ok(s) => serde_json::from_str(s).unwrap_or_default(),
            Err(_) => Vec::new(),
        }
    };
    let usages: Vec<DiskGroupUsageSummary> = if group_usages_json.is_null() {
        Vec::new()
    } else {
        match CStr::from_ptr(group_usages_json).to_str() {
            Ok(s) => serde_json::from_str(s).unwrap_or_default(),
            Err(_) => Vec::new(),
        }
    };
    spawn_op(callback, user_data, move || async move {
        svc.heartbeat_diskdb(instance_id, &endpoint, &dg_ids, &usages)
            .await
            .map_err(|e| e.to_string())?;
        Ok("{}".to_string())
    });
}

// ── Runtime lifecycle ─────────────────────────────────────────────

/// Shut down the FFI tokio runtime. Call this before process exit
/// (after destroying all client handles).
#[no_mangle]
pub unsafe extern "C" fn crow_kv_ffi_shutdown() {
    let opt = std::ptr::read(std::ptr::addr_of!(G_RUNTIME));
    std::ptr::write(std::ptr::addr_of_mut!(G_RUNTIME), None);
    if let Some(rt) = opt {
        drop(rt);
    }
}
