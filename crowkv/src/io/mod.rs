//! `CrowKV` async I/O facade.
//!
//! Skeleton module. Real content lands in P2 M0 with `AsyncFile` public API,
//! `io_uring` backend (`tokio-uring`) on Linux >= 5.11, `tokio::fs` +
//! `spawn_blocking` fallback, and a `SimDisk` simulated backend for unit tests.
//! Key work: `AsyncFile` API, `io_uring` integration, fallback mode, `SimDisk`.
