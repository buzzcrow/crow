//! WAL integration tests.
//!
//! `SimDisk` tests: `#[tokio::test(flavor = "current_thread", start_paused = true)]`
//! Fallback tests: real filesystem via `tempfile`, `#[tokio::test]`

#[path = "wal/record_tests.rs"]
mod record_tests;

#[path = "wal/segment_tests.rs"]
mod segment_tests;

#[path = "wal/manager_tests.rs"]
mod manager_tests;

#[path = "wal/replay_tests.rs"]
mod replay_tests;

#[path = "wal/gc_tests.rs"]
mod gc_tests;

#[path = "wal/pipeline_backend_tests.rs"]
mod pipeline_backend_tests;

#[path = "wal/block_backend_tests.rs"]
mod block_backend_tests;
