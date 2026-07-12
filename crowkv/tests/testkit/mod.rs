//! Shared deterministic test harness for `crowkv` integration tests.
//!
//! Real content lands incrementally with each phase:
//! - P1 M2: `TestTimer`, `TestRouter`, `TestNode`.
//! - P2 M0: `SimDisk` (async I/O simulated backend).

pub mod cluster;
pub mod logging;
pub mod net_lock;
pub mod simdisk;
pub mod timer;
