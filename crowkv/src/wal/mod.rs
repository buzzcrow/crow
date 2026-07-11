//! `CrowKV` write-ahead log.
//!
//! Skeleton module. Real content lands in P2 M1+ with multi-disk segments,
//! batched fsync, ack contract, replay, and GC.
//! Depends on `crate::io` (for `AsyncFile`) and `crate::store` (for `PxLogEntry`).
//! Key work: segment management, fsync batching, replay, garbage collection.
