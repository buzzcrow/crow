// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Tests for `crowdb-protocol::ports` — default port allocation,
//! stride rules, and non-overlap across service types.

use crowdb_protocol::ports::ServicePort;
use crowdb_protocol::{
    CHUNKDB_HTTP_BASE, CHUNKDB_LISTEN_BASE, CHUNKDB_RPC_BASE, DISKDB_HTTP_BASE, DISKDB_LISTEN_BASE,
    DISKDB_RPC_BASE, DISKIO_RPC_BASE, KV_SERVER_LISTEN_BASE, KV_SERVER_MGMT_BASE, WEB_BASE,
};

// ── base constants match enum ──────────────────────────────────

#[test]
fn base_constants_match_enum_base() {
    assert_eq!(ServicePort::KvServerMgmt.base(), KV_SERVER_MGMT_BASE);
    assert_eq!(ServicePort::KvServerListen.base(), KV_SERVER_LISTEN_BASE);
    assert_eq!(ServicePort::DiskdbListen.base(), DISKDB_LISTEN_BASE);
    assert_eq!(ServicePort::DiskdbHttp.base(), DISKDB_HTTP_BASE);
    assert_eq!(ServicePort::DiskdbRpc.base(), DISKDB_RPC_BASE);
    assert_eq!(ServicePort::ChunkdbListen.base(), CHUNKDB_LISTEN_BASE);
    assert_eq!(ServicePort::ChunkdbHttp.base(), CHUNKDB_HTTP_BASE);
    assert_eq!(ServicePort::ChunkdbRpc.base(), CHUNKDB_RPC_BASE);
    assert_eq!(ServicePort::DiskioRpc.base(), DISKIO_RPC_BASE);
    assert_eq!(ServicePort::Web.base(), WEB_BASE);
}

// ── known defaults (new port map, all >10000) ──────────────────

#[test]
fn known_base_ports() {
    assert_eq!(KV_SERVER_MGMT_BASE, 10000);
    assert_eq!(KV_SERVER_LISTEN_BASE, 10100);
    assert_eq!(DISKDB_LISTEN_BASE, 11000);
    assert_eq!(DISKDB_HTTP_BASE, 11100);
    assert_eq!(DISKDB_RPC_BASE, 11200);
    assert_eq!(CHUNKDB_LISTEN_BASE, 12000);
    assert_eq!(CHUNKDB_HTTP_BASE, 12100);
    assert_eq!(CHUNKDB_RPC_BASE, 12200);
    assert_eq!(DISKIO_RPC_BASE, 13000);
    assert_eq!(WEB_BASE, 14000);
}

// ── stride (all stride 1 — no paired-port logic) ───────────────

#[test]
fn all_services_have_stride_one() {
    for svc in [
        ServicePort::KvServerMgmt,
        ServicePort::KvServerListen,
        ServicePort::DiskdbListen,
        ServicePort::DiskdbHttp,
        ServicePort::DiskdbRpc,
        ServicePort::ChunkdbListen,
        ServicePort::ChunkdbHttp,
        ServicePort::ChunkdbRpc,
        ServicePort::DiskioRpc,
        ServicePort::Web,
    ] {
        assert_eq!(svc.stride(), 1, "{svc:?} must have stride 1");
    }
}

// ── port(instance) computation ─────────────────────────────────

#[test]
fn port_instance_zero_is_base() {
    for svc in [
        ServicePort::KvServerMgmt,
        ServicePort::KvServerListen,
        ServicePort::DiskdbListen,
        ServicePort::DiskdbHttp,
        ServicePort::DiskdbRpc,
        ServicePort::ChunkdbListen,
        ServicePort::ChunkdbHttp,
        ServicePort::ChunkdbRpc,
        ServicePort::DiskioRpc,
        ServicePort::Web,
    ] {
        assert_eq!(svc.port(0), svc.base());
    }
}

#[test]
fn port_increments_by_one() {
    assert_eq!(ServicePort::KvServerMgmt.port(0), 10000);
    assert_eq!(ServicePort::KvServerMgmt.port(1), 10001);
    assert_eq!(ServicePort::KvServerMgmt.port(99), 10099);

    assert_eq!(ServicePort::KvServerListen.port(0), 10100);
    assert_eq!(ServicePort::KvServerListen.port(1), 10101);
    assert_eq!(ServicePort::KvServerListen.port(99), 10199);

    assert_eq!(ServicePort::DiskdbRpc.port(0), 11200);
    assert_eq!(ServicePort::DiskdbRpc.port(1), 11201);
    assert_eq!(ServicePort::DiskdbRpc.port(99), 11299);

    assert_eq!(ServicePort::DiskioRpc.port(0), 13000);
    assert_eq!(ServicePort::DiskioRpc.port(1), 13001);
    assert_eq!(ServicePort::DiskioRpc.port(99), 13099);
}

// ── non-overlap across service types ───────────────────────────

#[test]
fn port_ranges_do_not_overlap() {
    // Each service type's first 100 instances (full sub-range).
    let all_services = [
        ServicePort::KvServerMgmt,
        ServicePort::KvServerListen,
        ServicePort::DiskdbListen,
        ServicePort::DiskdbHttp,
        ServicePort::DiskdbRpc,
        ServicePort::ChunkdbListen,
        ServicePort::ChunkdbHttp,
        ServicePort::ChunkdbRpc,
        ServicePort::DiskioRpc,
        ServicePort::Web,
    ];

    let mut seen: std::collections::HashSet<u16> = std::collections::HashSet::new();
    for svc in all_services {
        let ports: std::collections::HashSet<u16> = (0..100_u16).map(|i| svc.port(i)).collect();
        assert!(
            seen.is_disjoint(&ports),
            "{svc:?} ports overlap with a prior service type",
        );
        seen.extend(ports);
    }
}

// ── range_size ─────────────────────────────────────────────────

#[test]
fn range_size_is_100_for_all_services() {
    for svc in [
        ServicePort::KvServerMgmt,
        ServicePort::KvServerListen,
        ServicePort::DiskdbListen,
        ServicePort::DiskdbHttp,
        ServicePort::DiskdbRpc,
        ServicePort::ChunkdbListen,
        ServicePort::ChunkdbHttp,
        ServicePort::ChunkdbRpc,
        ServicePort::DiskioRpc,
        ServicePort::Web,
    ] {
        assert_eq!(svc.range_size(), 100, "{svc:?} range_size must be 100");
    }
}
