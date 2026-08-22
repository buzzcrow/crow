// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// C ABI for crow-kv-client (ffi feature): exposes HardwareClient and
// ServiceRegistryClient operations to C++ consumers (crow-diskio).
//
// Async operations use a callback pattern: the caller provides a
// function pointer + user_data. When the op completes, the callback
// is invoked with a status code (0 = OK, negative = error) and a
// JSON result string (null on error). The JSON string is valid only
// during the callback — copy it if needed.
//
// Complex types (DiskValue, DiskdbOwnerEntry, etc.) are serialized
// to JSON for transport across the ABI boundary.
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

// ── Opaque handles ────────────────────────────────────────────────

typedef void *crow_hw_client_t;
typedef void *crow_svc_client_t;

// ── Callback type ─────────────────────────────────────────────────

typedef void (*crow_kv_on_complete)(int status, const char *result_json, void *user_data);

// ── HardwareClient ────────────────────────────────────────────────

// Create a HardwareClient from kv-server management seeds.
// seeds: array of null-terminated C strings (e.g. "http://127.0.0.1:9910").
// Returns NULL on error.
crow_hw_client_t crow_hw_client_create(const char *const *seeds, size_t num_seeds);

// Destroy a HardwareClient handle.
void crow_hw_client_destroy(crow_hw_client_t client);

// List disks in a disk-group. Callback receives a JSON array of
// {"disk_id": {"high": u64, "low": u64}, "value": {...DiskValue...}}.
void crow_hw_list_disks_in_group(crow_hw_client_t client, uint64_t rack_id, uint64_t node_id,
                                 uint64_t dg_id, crow_kv_on_complete callback, void *user_data);

// List all diskdb ownership entries. Callback receives a JSON array
// of DiskdbOwnerEntry objects.
void crow_hw_list_owners(crow_hw_client_t client, crow_kv_on_complete callback, void *user_data);

// List all KV-group bind entries. Callback receives a JSON array
// of KVGroupBindEntry objects.
void crow_hw_list_binds(crow_hw_client_t client, crow_kv_on_complete callback, void *user_data);

// ── ServiceRegistryClient ─────────────────────────────────────────

// Create a ServiceRegistryClient from kv-server management seeds.
crow_svc_client_t crow_svc_client_create(const char *const *seeds, size_t num_seeds);

// Destroy a ServiceRegistryClient handle.
void crow_svc_client_destroy(crow_svc_client_t client);

// Heartbeat a diskio instance.
// owned_dg_ids_json: JSON array of u64 disk-group IDs (e.g. "[1,2,3]").
// group_usages_json: JSON array of DiskGroupUsageSummary objects (can be "[]").
void crow_svc_heartbeat_diskio(crow_svc_client_t client, uint64_t instance_id,
                               const char *grpc_endpoint, const char *owned_dg_ids_json,
                               const char *group_usages_json, crow_kv_on_complete callback,
                               void *user_data);

// ── Runtime lifecycle ─────────────────────────────────────────────

// Shut down the FFI tokio runtime. Call before process exit.
void crow_kv_ffi_shutdown();

#ifdef __cplusplus
} // extern "C"
#endif
