// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! WAL integration tests.
//!
//! `SimDisk` tests: `#[tokio::test(flavor = "current_thread", start_paused = true)]`
//! Fallback tests: real filesystem via `tempfile`, `#[tokio::test]`

#[path = "wal_test/record_test.rs"]
mod record_test;

#[path = "wal_test/segment_test.rs"]
mod segment_test;

#[path = "wal_test/wal_engine_test.rs"]
mod wal_engine_test;

#[path = "wal_test/replay_test.rs"]
mod replay_test;

#[path = "wal_test/gc_test.rs"]
mod gc_test;

#[path = "wal_test/pipeline_backend_test.rs"]
mod pipeline_backend_test;

#[path = "wal_test/block_backend_test.rs"]
mod block_backend_test;

#[path = "wal_test/file_restore_test.rs"]
mod file_restore_test;

#[path = "wal_test/index_test.rs"]
mod index_test;

#[path = "wal_test/io_backend_test.rs"]
mod io_backend_test;

#[path = "wal_test/file_backend_test.rs"]
mod file_backend_test;

#[path = "wal_test/pipeline_writer_test.rs"]
mod pipeline_writer_test;
