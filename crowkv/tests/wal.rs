//! WAL integration tests.
//!
//! `SimDisk` tests: `#[tokio::test(flavor = "current_thread", start_paused = true)]`
//! Fallback tests: real filesystem via `tempfile`, `#[tokio::test]`

#[path = "wal/record_tests.rs"]
mod record_tests;

#[path = "wal/segment_tests.rs"]
mod segment_tests;

#[path = "wal/wal_engine_tests.rs"]
mod wal_engine_tests;

#[path = "wal/replay_tests.rs"]
mod replay_tests;

#[path = "wal/gc_tests.rs"]
mod gc_tests;

#[path = "wal/pipeline_backend_tests.rs"]
mod pipeline_backend_tests;

#[path = "wal/block_backend_tests.rs"]
mod block_backend_tests;

#[path = "wal/file_restore_tests.rs"]
mod file_restore_tests;

#[path = "wal/index_tests.rs"]
mod index_tests;

#[path = "wal/io_backend_tests.rs"]
mod io_backend_tests;

#[path = "wal/file_backend_tests.rs"]
mod file_backend_tests;

#[path = "wal/pipeline_writer_tests.rs"]
mod pipeline_writer_tests;
