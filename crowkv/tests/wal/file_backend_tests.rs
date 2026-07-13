// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `file_backend` tests via the public `IoBackend::File` / `WalFile` API.
//!
//! The `file_backend` module itself is private (`mod file_backend`), but its
//! behaviour is fully exercised through the public `IoBackend::File` variant
//! and `WalFile` methods. These tests cover the real-filesystem fallback
//! path (open/append/flush/fsync/truncate) independent of `BlockDevice`.

use crowkv::wal::{IoBackend, OpenOptions};

#[tokio::test]
async fn file_backend_write_then_reopen_preserves_data() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("persist.wal");
    let backend = IoBackend::File;

    // Write data.
    {
        let mut f = backend
            .open(&path, OpenOptions::create_new_rw())
            .await
            .expect("open");
        f.write_at(b"persistent data", 0).await.expect("write");
        f.fdatasync().await.expect("fsync");
    }

    // Reopen and read — data must survive.
    {
        let mut f = backend
            .open(&path, OpenOptions::read_only())
            .await
            .expect("reopen");
        let mut buf = vec![0u8; 15];
        f.read_exact_at(&mut buf, 0).await.expect("read");
        assert_eq!(&buf, b"persistent data");
    }
}

#[tokio::test]
async fn file_backend_append_at_offset_extends_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("extend.wal");
    let backend = IoBackend::File;

    let mut f = backend
        .open(&path, OpenOptions::create_new_rw())
        .await
        .expect("open");

    // Write at offset 0, then at offset 100 — file should extend.
    f.write_at(b"start", 0).await.expect("write 0");
    f.write_at(b"end", 100).await.expect("write 100");
    f.fdatasync().await.expect("sync");

    let len = f.len().await.expect("len");
    assert_eq!(len, 103, "file extends to cover offset 100 + 3 bytes");

    // Gap between offset 5 and 100 reads as zeros.
    let mut gap = vec![0u8; 95];
    f.read_exact_at(&mut gap, 5).await.expect("read gap");
    assert!(gap.iter().all(|&b| b == 0), "gap is zero-filled");
}

#[tokio::test]
async fn file_backend_fsync_succeeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fsync.wal");
    let backend = IoBackend::File;

    let mut f = backend
        .open(&path, OpenOptions::create_new_rw())
        .await
        .expect("open");
    f.write_at(b"synced", 0).await.expect("write");
    f.fsync().await.expect("fsync should succeed");
}

#[tokio::test]
async fn file_backend_read_at_returns_bytes_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("partial.wal");
    let backend = IoBackend::File;

    let mut f = backend
        .open(&path, OpenOptions::create_new_rw())
        .await
        .expect("open");
    f.write_at(b"hello world", 0).await.expect("write");
    f.fdatasync().await.expect("sync");

    // Read 5 bytes from offset 6.
    let mut buf = vec![0u8; 5];
    let n = f.read_at(&mut buf, 6).await.expect("read_at");
    assert_eq!(n, 5);
    assert_eq!(&buf, b"world");
}

#[tokio::test]
async fn file_backend_read_exact_at_at_eof_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("eof.wal");
    let backend = IoBackend::File;

    let mut f = backend
        .open(&path, OpenOptions::create_new_rw())
        .await
        .expect("open");
    f.write_at(b"short", 0).await.expect("write");

    // Try to read 10 bytes but only 5 exist.
    let mut buf = vec![0u8; 10];
    let result = f.read_exact_at(&mut buf, 0).await;
    assert!(result.is_err(), "read_exact past EOF should error");
}

#[tokio::test]
async fn file_backend_truncate_to_zero_clears_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("zero.wal");
    let backend = IoBackend::File;

    let mut f = backend
        .open(&path, OpenOptions::create_new_rw())
        .await
        .expect("open");
    f.write_at(b"12345678", 0).await.expect("write");
    f.fdatasync().await.expect("sync");
    assert_eq!(f.len().await.expect("len"), 8);

    f.truncate(0).await.expect("truncate to 0");
    assert_eq!(f.len().await.expect("len after truncate"), 0);
}

#[tokio::test]
async fn file_backend_overwrite_at_same_offset() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("overwrite.wal");
    let backend = IoBackend::File;

    let mut f = backend
        .open(&path, OpenOptions::create_new_rw())
        .await
        .expect("open");
    f.write_at(b"AAAA", 0).await.expect("write A");
    f.write_at(b"BBBB", 0).await.expect("overwrite B");
    f.fdatasync().await.expect("sync");

    let mut buf = vec![0u8; 4];
    f.read_exact_at(&mut buf, 0).await.expect("read");
    assert_eq!(&buf, b"BBBB", "second write overwrites first");
}

#[tokio::test]
async fn file_backend_create_new_rejects_existing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("exists.wal");
    let backend = IoBackend::File;

    // First create succeeds.
    let mut f = backend
        .open(&path, OpenOptions::create_new_rw())
        .await
        .expect("first create");
    f.write_at(b"x", 0).await.expect("write");
    drop(f);

    // Second create_new should fail (file already exists).
    let result = backend.open(&path, OpenOptions::create_new_rw()).await;
    assert!(result.is_err(), "create_new on existing file should fail");
}

#[tokio::test]
async fn file_backend_create_rw_appends_to_existing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("append.wal");
    let backend = IoBackend::File;

    // Create and write.
    {
        let mut f = backend
            .open(&path, OpenOptions::create_new_rw())
            .await
            .expect("create");
        f.write_at(b"first", 0).await.expect("write");
        f.fdatasync().await.expect("sync");
    }

    // Reopen with create (not create_new) — should succeed, not truncate.
    {
        let mut f = backend
            .open(&path, OpenOptions::create_rw())
            .await
            .expect("reopen create");
        let mut buf = vec![0u8; 5];
        f.read_exact_at(&mut buf, 0).await.expect("read");
        assert_eq!(&buf, b"first", "data preserved on reopen with create");
    }
}
