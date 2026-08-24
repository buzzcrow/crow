// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Tests for `crow-protocol::ports` — default port allocation,
// stride rules, and non-overlap across service types.

use crow_protocol::ports::ServicePort;
use crow_protocol::{
    CHUNKDB_GRPC_BASE, CHUNKDB_HTTP_BASE, CHUNKDB_RPC_BASE, DISKDB_GRPC_BASE, DISKDB_HTTP_BASE,
    KV_SERVER_GRPC_BASE, KV_SERVER_MGMT_BASE, WEB_BASE,
};

// ── base constants match enum ──────────────────────────────────

#[test]
fn base_constants_match_enum_base() {
    assert_eq!(ServicePort::KvServerMgmt.base(), KV_SERVER_MGMT_BASE);
    assert_eq!(ServicePort::KvServerGrpc.base(), KV_SERVER_GRPC_BASE);
    assert_eq!(ServicePort::DiskdbGrpc.base(), DISKDB_GRPC_BASE);
    assert_eq!(ServicePort::DiskdbHttp.base(), DISKDB_HTTP_BASE);
    assert_eq!(ServicePort::ChunkdbGrpc.base(), CHUNKDB_GRPC_BASE);
    assert_eq!(ServicePort::ChunkdbHttp.base(), CHUNKDB_HTTP_BASE);
    assert_eq!(ServicePort::ChunkdbRpc.base(), CHUNKDB_RPC_BASE);
    assert_eq!(ServicePort::Web.base(), WEB_BASE);
}

// ── known defaults ─────────────────────────────────────────────

#[test]
fn known_base_ports() {
    assert_eq!(KV_SERVER_MGMT_BASE, 9910);
    assert_eq!(KV_SERVER_GRPC_BASE, 28001);
    assert_eq!(DISKDB_GRPC_BASE, 9941);
    assert_eq!(DISKDB_HTTP_BASE, 9942);
    assert_eq!(CHUNKDB_GRPC_BASE, 9971);
    assert_eq!(CHUNKDB_HTTP_BASE, 9972);
    assert_eq!(CHUNKDB_RPC_BASE, 9961);
    assert_eq!(WEB_BASE, 9920);
}

// ── stride ─────────────────────────────────────────────────────

#[test]
fn single_port_services_have_stride_one() {
    assert_eq!(ServicePort::KvServerMgmt.stride(), 1);
    assert_eq!(ServicePort::KvServerGrpc.stride(), 1);
    assert_eq!(ServicePort::Web.stride(), 1);
    assert_eq!(ServicePort::ChunkdbRpc.stride(), 1);
}

#[test]
fn diskdb_paired_ports_have_stride_two() {
    assert_eq!(ServicePort::DiskdbGrpc.stride(), 2);
    assert_eq!(ServicePort::DiskdbHttp.stride(), 2);
    assert_eq!(ServicePort::ChunkdbGrpc.stride(), 2);
    assert_eq!(ServicePort::ChunkdbHttp.stride(), 2);
}

// ── port(instance) computation ─────────────────────────────────

#[test]
fn port_instance_zero_is_base() {
    for svc in [
        ServicePort::KvServerMgmt,
        ServicePort::KvServerGrpc,
        ServicePort::DiskdbGrpc,
        ServicePort::DiskdbHttp,
        ServicePort::ChunkdbGrpc,
        ServicePort::ChunkdbHttp,
        ServicePort::ChunkdbRpc,
        ServicePort::Web,
    ] {
        assert_eq!(svc.port(0), svc.base());
    }
}

#[test]
fn diskdb_paired_ports_stay_adjacent_across_instances() {
    for i in 0..10_u16 {
        let grpc = ServicePort::DiskdbGrpc.port(i);
        let http = ServicePort::DiskdbHttp.port(i);
        assert_eq!(http, grpc + 1, "instance {i}: http must be grpc + 1");
    }
}

#[test]
fn kv_server_rpc_port_increments_by_one() {
    assert_eq!(ServicePort::KvServerGrpc.port(0), 28001);
    assert_eq!(ServicePort::KvServerGrpc.port(1), 28002);
    assert_eq!(ServicePort::KvServerGrpc.port(199), 28200);
}

#[test]
fn chunkdb_rpc_port_increments_by_one() {
    assert_eq!(ServicePort::ChunkdbRpc.port(0), 9961);
    assert_eq!(ServicePort::ChunkdbRpc.port(1), 9962);
    assert_eq!(ServicePort::ChunkdbRpc.port(9), 9970);
}

#[test]
fn chunkdb_rpc_does_not_overlap_grpc() {
    // RPC: 9961-9970, gRPC: 9971-9990 (stride 2, 10 instances).
    let rpc: std::collections::HashSet<u16> = (0..10_u16).map(|i| ServicePort::ChunkdbRpc.port(i)).collect();
    let grpc: std::collections::HashSet<u16> =
        (0..10_u16).map(|i| ServicePort::ChunkdbGrpc.port(i)).collect();
    assert!(rpc.is_disjoint(&grpc), "chunkdb rpc and grpc must not overlap");
}

// ── non-overlap across service types ───────────────────────────

#[test]
fn port_ranges_do_not_overlap() {
    // Each service type's first 10 instances.
    let kv_mgmt: Vec<u16> = (0..10).map(|i| ServicePort::KvServerMgmt.port(i)).collect();
    let web: Vec<u16> = (0..10).map(|i| ServicePort::Web.port(i)).collect();
    let diskdb_grpc: Vec<u16> = (0..10).map(|i| ServicePort::DiskdbGrpc.port(i)).collect();
    let diskdb_http: Vec<u16> = (0..10).map(|i| ServicePort::DiskdbHttp.port(i)).collect();

    // diskdb grpc and http are intentionally adjacent (paired), so
    // they overlap with each other by design — check them as a union.
    let diskdb_all: std::collections::HashSet<u16> =
        diskdb_grpc.iter().chain(diskdb_http.iter()).copied().collect();

    let kv_mgmt_set: std::collections::HashSet<u16> = kv_mgmt.iter().copied().collect();
    let web_set: std::collections::HashSet<u16> = web.iter().copied().collect();

    assert!(
        kv_mgmt_set.is_disjoint(&web_set),
        "kv-server mgmt and web ports must not overlap"
    );
    assert!(
        kv_mgmt_set.is_disjoint(&diskdb_all),
        "kv-server mgmt and diskdb ports must not overlap"
    );
    assert!(
        web_set.is_disjoint(&diskdb_all),
        "web and diskdb ports must not overlap"
    );
}
