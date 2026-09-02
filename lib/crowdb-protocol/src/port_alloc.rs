// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Flock-coordinated port allocator with bind probes.
//!
//! The single place that picks ports for tests and cluster bootstrap.
//! Uses a claim file under a workspace root, serialized via `flock`.
//! Each port is bind-probed before being handed out, so `TIME_WAIT`
//! sockets and non-coordinated processes are skipped. No port 0 is
//! ever used.
//!
//! See `doc/working/design-cluster-unify-port-usage.md` §8.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};

use fs2::FileExt;

use crate::ports::ServicePort;

/// Default sub-directory name under the workspace root for the claim
/// file.
const CLAIM_DIR: &str = ".crowdb-port-alloc";

/// Claim file name.
const CLAIM_FILE: &str = "claims";

/// Configuration for the port allocator.
#[derive(Debug, Clone)]
pub struct PortAllocConfig {
    /// Workspace root directory. The claim file lives at
    /// `<root>/.crowdb-port-alloc/claims`.
    pub root: PathBuf,
    /// Port offset for multi-session isolation. Each service's base
    /// port is shifted by `offset`: `ServicePort::port(instance) +
    /// offset`. Default: 0.
    pub offset: u16,
}

impl PortAllocConfig {
    /// Create a config with the given root and offset 0.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            offset: 0,
        }
    }

    /// Set the port offset.
    #[must_use]
    pub const fn with_offset(mut self, offset: u16) -> Self {
        self.offset = offset;
        self
    }

    fn claim_path(&self) -> PathBuf {
        self.root.join(CLAIM_DIR).join(CLAIM_FILE)
    }
}

impl Default for PortAllocConfig {
    fn default() -> Self {
        Self::new(std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
    }
}

/// Errors returned by the port allocator.
#[derive(Debug, thiserror::Error)]
pub enum PortAllocError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("no free port in range {base}..{end} for {service:?} (all claimed or bound)")]
    Exhausted {
        service: ServicePort,
        base: u16,
        end: u16,
    },
    #[error("port {port} is already claimed")]
    AlreadyClaimed { port: u16 },
    #[error("offset {offset} + base {base} + instance {instance} overflows u16")]
    Overflow { offset: u16, base: u16, instance: u16 },
}

/// Open (or create) the claim file and acquire an exclusive lock.
fn lock_claim_file(path: &Path) -> Result<File, PortAllocError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    file.lock_exclusive()?;
    Ok(file)
}

/// Read the set of claimed ports from the claim file.
fn read_claims(file: &mut File) -> HashSet<u16> {
    let mut content = String::new();
    if file.read_to_string(&mut content).is_err() {
        return HashSet::new();
    }
    content
        .lines()
        .filter_map(|line| line.trim().parse::<u16>().ok())
        .collect()
}

/// Write the full claim set back to the claim file (one port per line).
fn write_claims(file: &mut File, claims: &HashSet<u16>) -> Result<(), PortAllocError> {
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    let mut sorted: Vec<u16> = claims.iter().copied().collect();
    sorted.sort_unstable();
    let mut buf = String::new();
    for port in sorted {
        buf.push_str(&port.to_string());
        buf.push('\n');
    }
    file.write_all(buf.as_bytes())?;
    Ok(())
}

/// Bind-probe `127.0.0.1:port`. Returns `true` if the port is free
/// (bind succeeds), `false` otherwise. The probe listener is dropped
/// immediately.
fn is_port_free(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_ok()
}

/// Compute the candidate port for a service + instance + offset.
fn candidate_port(service: ServicePort, instance: u16, offset: u16) -> Result<u16, PortAllocError> {
    let base = service.base();
    let port = base
        .checked_add(offset)
        .and_then(|p| p.checked_add(instance))
        .ok_or(PortAllocError::Overflow {
            offset,
            base,
            instance,
        })?;
    Ok(port)
}

/// Allocate a single port for the given service type + instance.
///
/// Probes the system (bind probe) for a free port in the service's
/// range, not already in the claim file. Writes the port to the claim
/// file under flock. If the candidate port for `instance` is claimed
/// or bound, tries `instance+1`, `instance+2`, etc. up to the range
/// limit.
///
/// # Errors
/// Returns `PortAllocError::Exhausted` if all ports in the service
/// range are claimed or bound. Returns `PortAllocError::Io` on
/// filesystem errors. Returns `PortAllocError::Overflow` if the
/// computed port overflows `u16`.
pub fn alloc_port(service: ServicePort, instance: u16, cfg: &PortAllocConfig) -> Result<u16, PortAllocError> {
    let path = cfg.claim_path();
    let mut file = lock_claim_file(&path)?;
    alloc_port_locked(service, instance, cfg.offset, &mut file)
}

/// Inner allocation logic assuming the claim file is already locked.
fn alloc_port_locked(
    service: ServicePort,
    start_instance: u16,
    offset: u16,
    file: &mut File,
) -> Result<u16, PortAllocError> {
    let mut claims = read_claims(file);
    let range_size = service.range_size();

    for i in start_instance..range_size {
        let port = candidate_port(service, i, offset)?;
        if claims.contains(&port) {
            continue;
        }
        if !is_port_free(port) {
            continue;
        }
        claims.insert(port);
        write_claims(file, &claims)?;
        return Ok(port);
    }

    let base = service.base() + offset;
    let end = base + range_size;
    Err(PortAllocError::Exhausted { service, base, end })
}

/// Allocate `count` consecutive ports for a service type starting at
/// `instance`. All ports are in the same service range and are
/// pairwise consecutive. The entire range is probed and claimed
/// atomically under one flock.
///
/// # Errors
/// Returns `PortAllocError::Exhausted` if no contiguous run of `count`
/// free ports exists in the service range. Returns `PortAllocError::Io`
/// on filesystem errors. Returns `PortAllocError::Overflow` if a
/// computed port overflows `u16`.
pub fn alloc_port_range(
    service: ServicePort,
    start_instance: u16,
    count: u16,
    cfg: &PortAllocConfig,
) -> Result<Vec<u16>, PortAllocError> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let path = cfg.claim_path();
    let mut file = lock_claim_file(&path)?;
    alloc_port_range_locked(service, start_instance, count, cfg.offset, &mut file)
}

/// Inner range allocation logic assuming the claim file is already
/// locked.
fn alloc_port_range_locked(
    service: ServicePort,
    start_instance: u16,
    count: u16,
    offset: u16,
    file: &mut File,
) -> Result<Vec<u16>, PortAllocError> {
    let mut claims = read_claims(file);
    let range_size = service.range_size();

    for start in start_instance..=(range_size.saturating_sub(count)) {
        let ports: Result<Vec<u16>, PortAllocError> = (0..count)
            .map(|j| candidate_port(service, start + j, offset))
            .collect();
        let ports = ports?;

        let all_free = ports.iter().all(|p| !claims.contains(p) && is_port_free(*p));

        if all_free {
            for p in &ports {
                claims.insert(*p);
            }
            write_claims(file, &claims)?;
            return Ok(ports);
        }
    }

    let base = service.base() + offset;
    let end = base + range_size;
    Err(PortAllocError::Exhausted { service, base, end })
}

/// Mark a port as "tried-and-failed" in the claim file so the next
/// probe skips it. Called by the test harness when a server bind fails
/// (TOCTOU mitigation). The port is added to the claim set — it will
/// not be re-allocated until `reset_claims` is called.
///
/// # Errors
/// Returns `PortAllocError::Io` on filesystem errors.
pub fn mark_failed(port: u16, cfg: &PortAllocConfig) -> Result<(), PortAllocError> {
    let path = cfg.claim_path();
    let mut file = lock_claim_file(&path)?;
    let mut claims = read_claims(&mut file);
    claims.insert(port);
    write_claims(&mut file, &claims)?;
    Ok(())
}

/// Reset the claim file (delete it). Called by test shells between
/// runs to avoid exhaustion.
///
/// # Errors
/// Returns `PortAllocError::Io` on filesystem errors.
pub fn reset_claims(cfg: &PortAllocConfig) -> Result<(), PortAllocError> {
    let path = cfg.claim_path();
    if path.exists() {
        fs::remove_file(&path)?;
    }
    Ok(())
}

/// Read the current set of claimed ports without allocating. Useful
/// for diagnostics.
///
/// # Errors
/// Returns `PortAllocError::Io` on filesystem errors.
#[allow(clippy::incompatible_msrv)]
pub fn read_claimed_ports(cfg: &PortAllocConfig) -> Result<HashSet<u16>, PortAllocError> {
    let path = cfg.claim_path();
    if !path.exists() {
        return Ok(HashSet::new());
    }
    let mut file = File::open(&path)?;
    file.lock_shared()?;
    let claims = read_claims(&mut file);
    Ok(claims)
}

// ── Test convenience functions ───────────────────────────────────

use std::sync::OnceLock;

static TEST_CFG: OnceLock<PortAllocConfig> = OnceLock::new();

/// Per-process test claim-file root: `$TMPDIR/crowdb-port-alloc-test-{pid}`.
/// All test convenience calls share this claim file so parallel tests
/// within one binary don't collide. The flock serializes access.
fn test_cfg() -> &'static PortAllocConfig {
    TEST_CFG.get_or_init(|| {
        let pid = std::process::id();
        let root = std::env::temp_dir().join(format!("crowdb-port-alloc-test-{pid}"));
        let _ = fs::create_dir_all(&root);
        PortAllocConfig::new(root)
    })
}

/// Allocate a single port for `service` from the per-process test
/// claim file. Panics on exhaustion (mirrors `unique_test_port`).
///
/// # Panics
/// Panics if the port allocator returns an error (all ports claimed,
/// filesystem error, or overflow).
#[must_use]
pub fn alloc_test_port(service: ServicePort) -> u16 {
    alloc_port(service, 0, test_cfg()).unwrap_or_else(|e| panic!("alloc_test_port({service:?}) failed: {e}"))
}

/// Allocate `count` consecutive ports for `service` from the
/// per-process test claim file. Panics on exhaustion (mirrors
/// `unique_test_port_range`).
///
/// # Panics
/// Panics if the port allocator returns an error (no contiguous run
/// found, filesystem error, or overflow).
#[must_use]
pub fn alloc_test_port_range(service: ServicePort, count: u16) -> Vec<u16> {
    alloc_port_range(service, 0, count, test_cfg())
        .unwrap_or_else(|e| panic!("alloc_test_port_range({service:?}, {count}) failed: {e}"))
}

/// Reset the per-process test claim file. Call between test suites to
/// avoid exhaustion.
pub fn reset_test_claims() {
    let _ = reset_claims(test_cfg());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn unique_cfg() -> PortAllocConfig {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("crowdb-port-alloc-test-{id}"));
        let _ = fs::remove_dir_all(&dir);
        PortAllocConfig::new(&dir)
    }

    fn cleanup(cfg: &PortAllocConfig) {
        let _ = fs::remove_dir_all(&cfg.root);
    }

    #[test]
    fn alloc_single_port() {
        let cfg = unique_cfg();
        let port = alloc_port(ServicePort::KvServerMgmt, 0, &cfg).unwrap();
        assert_eq!(port, 10000);
        let claimed = read_claimed_ports(&cfg).unwrap();
        assert!(claimed.contains(&port));
        cleanup(&cfg);
    }

    #[test]
    fn alloc_second_instance() {
        let cfg = unique_cfg();
        let p0 = alloc_port(ServicePort::KvServerMgmt, 0, &cfg).unwrap();
        let p1 = alloc_port(ServicePort::KvServerMgmt, 1, &cfg).unwrap();
        assert_eq!(p0, 10000);
        assert_eq!(p1, 10001);
        cleanup(&cfg);
    }

    #[test]
    fn alloc_skips_claimed() {
        let cfg = unique_cfg();
        let p0 = alloc_port(ServicePort::KvServerMgmt, 0, &cfg).unwrap();
        assert_eq!(p0, 10000);
        // Allocating instance 0 again should skip to instance 1.
        let p1 = alloc_port(ServicePort::KvServerMgmt, 0, &cfg).unwrap();
        assert_eq!(p1, 10001);
        cleanup(&cfg);
    }

    #[test]
    fn alloc_range() {
        let cfg = unique_cfg();
        let ports = alloc_port_range(ServicePort::KvServerListen, 0, 3, &cfg).unwrap();
        assert_eq!(ports.len(), 3);
        assert_eq!(ports[0], 10100);
        assert_eq!(ports[1], 10101);
        assert_eq!(ports[2], 10102);
        let claimed = read_claimed_ports(&cfg).unwrap();
        for p in &ports {
            assert!(claimed.contains(p));
        }
        cleanup(&cfg);
    }

    #[test]
    fn alloc_range_zero_count() {
        let cfg = unique_cfg();
        let ports = alloc_port_range(ServicePort::KvServerListen, 0, 0, &cfg).unwrap();
        assert!(ports.is_empty());
        cleanup(&cfg);
    }

    #[test]
    fn mark_failed_skips_port() {
        let cfg = unique_cfg();
        mark_failed(10000, &cfg).unwrap();
        let port = alloc_port(ServicePort::KvServerMgmt, 0, &cfg).unwrap();
        assert_ne!(port, 10000, "failed port must be skipped");
        cleanup(&cfg);
    }

    #[test]
    fn reset_claims_clears() {
        let cfg = unique_cfg();
        alloc_port(ServicePort::KvServerMgmt, 0, &cfg).unwrap();
        let claimed = read_claimed_ports(&cfg).unwrap();
        assert!(!claimed.is_empty());
        reset_claims(&cfg).unwrap();
        let claimed = read_claimed_ports(&cfg).unwrap();
        assert!(claimed.is_empty());
        cleanup(&cfg);
    }

    #[test]
    fn offset_shifts_base() {
        let cfg = unique_cfg().with_offset(50);
        let port = alloc_port(ServicePort::KvServerMgmt, 0, &cfg).unwrap();
        assert_eq!(port, 10050);
        cleanup(&cfg);
    }

    #[test]
    fn different_services_do_not_collide() {
        let cfg = unique_cfg();
        let kv_mgmt = alloc_port(ServicePort::KvServerMgmt, 0, &cfg).unwrap();
        let kv_listen = alloc_port(ServicePort::KvServerListen, 0, &cfg).unwrap();
        let diskdb = alloc_port(ServicePort::DiskdbRpc, 0, &cfg).unwrap();
        assert_ne!(kv_mgmt, kv_listen);
        assert_ne!(kv_mgmt, diskdb);
        assert_ne!(kv_listen, diskdb);
        cleanup(&cfg);
    }
}
