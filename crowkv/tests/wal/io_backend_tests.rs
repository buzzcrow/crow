// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `IoBackend` tests for the `File` fallback path.
//!
//! Exercises `detect()`, `open()`, `rename()`, `unlink()`, `read_dir()`,
//! `create_dir_all()`, and `exists()` on a real temp directory.

use std::sync::Arc;

use crowkv::wal::{IoBackend, OpenOptions};

#[tokio::test]
async fn detect_returns_file_backend() {
    let backend = IoBackend::detect();
    assert!(matches!(backend, IoBackend::File));
}

#[tokio::test]
async fn open_create_rw_succeeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("test.wal");
    let backend = IoBackend::File;

    let mut file = backend
        .open(&path, OpenOptions::create_new_rw())
        .await
        .expect("open create_new_rw");

    // Write some data at offset 0.
    let data = b"hello wal";
    let n = file.write_at(data, 0).await.expect("write_at");
    assert_eq!(n, data.len());

    // Read it back.
    let mut buf = vec![0u8; data.len()];
    file.read_exact_at(&mut buf, 0).await.expect("read_exact_at");
    assert_eq!(&buf, data);
}

#[tokio::test]
async fn open_read_only_can_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("ro.wal");

    // Create the file first with read-write.
    let backend = IoBackend::File;
    let mut file = backend
        .open(&path, OpenOptions::create_new_rw())
        .await
        .expect("create");
    file.write_at(b"data", 0).await.expect("write");
    drop(file);

    // Reopen read-only and read.
    let mut ro = backend
        .open(&path, OpenOptions::read_only())
        .await
        .expect("open ro");

    let mut buf = vec![0u8; 4];
    ro.read_exact_at(&mut buf, 0).await.expect("read");
    assert_eq!(&buf, b"data");
}

#[tokio::test]
async fn vectored_write_at_writes_all_buffers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("vwrite.wal");
    let backend = IoBackend::File;

    let mut file = backend
        .open(&path, OpenOptions::create_new_rw())
        .await
        .expect("open");

    let buf1 = b"hello ";
    let buf2 = b"world!";
    let slices = [std::io::IoSlice::new(buf1), std::io::IoSlice::new(buf2)];
    let n = file.write_vectored_at(&slices, 0).await.expect("vectored write");
    assert_eq!(n, buf1.len() + buf2.len());

    // Read back the full content.
    let mut buf = vec![0u8; buf1.len() + buf2.len()];
    file.read_exact_at(&mut buf, 0).await.expect("read");
    assert_eq!(&buf, b"hello world!");
}

#[tokio::test]
async fn fdatasync_succeeds_after_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("sync.wal");
    let backend = IoBackend::File;

    let mut file = backend
        .open(&path, OpenOptions::create_new_rw())
        .await
        .expect("open");
    file.write_at(b"durable", 0).await.expect("write");
    file.fdatasync().await.expect("fdatasync should succeed");
}

#[tokio::test]
async fn len_reports_file_size() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("len.wal");
    let backend = IoBackend::File;

    let mut file = backend
        .open(&path, OpenOptions::create_new_rw())
        .await
        .expect("open");
    file.write_at(b"12345678", 0).await.expect("write");
    file.fdatasync().await.expect("sync");

    let len = file.len().await.expect("len");
    assert_eq!(len, 8);
}

#[tokio::test]
async fn truncate_shrinks_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("trunc.wal");
    let backend = IoBackend::File;

    let mut file = backend
        .open(&path, OpenOptions::create_new_rw())
        .await
        .expect("open");
    file.write_at(b"12345678", 0).await.expect("write");
    file.fdatasync().await.expect("sync");
    assert_eq!(file.len().await.expect("len"), 8);

    file.truncate(4).await.expect("truncate");
    assert_eq!(file.len().await.expect("len after truncate"), 4);
}

#[tokio::test]
async fn rename_moves_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let from = dir.path().join("a.wal");
    let to = dir.path().join("b.wal");
    let backend = IoBackend::File;

    // Create and write to 'from'.
    {
        let mut f = backend
            .open(&from, OpenOptions::create_new_rw())
            .await
            .expect("open from");
        f.write_at(b"moved", 0).await.expect("write");
        f.fdatasync().await.expect("sync");
    }

    backend.rename(&from, &to).await.expect("rename");
    assert!(!backend.exists(&from).await, "old path gone");
    assert!(backend.exists(&to).await, "new path exists");

    // Read back from new path.
    let mut f = backend
        .open(&to, OpenOptions::read_only())
        .await
        .expect("open to");
    let mut buf = vec![0u8; 5];
    f.read_exact_at(&mut buf, 0).await.expect("read");
    assert_eq!(&buf, b"moved");
}

#[tokio::test]
async fn unlink_removes_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("del.wal");
    let backend = IoBackend::File;

    {
        let mut f = backend
            .open(&path, OpenOptions::create_new_rw())
            .await
            .expect("open");
        f.write_at(b"x", 0).await.expect("write");
    }

    assert!(backend.exists(&path).await);
    backend.unlink(&path).await.expect("unlink");
    assert!(!backend.exists(&path).await, "file removed");
}

#[tokio::test]
async fn create_dir_all_creates_nested_dirs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let nested = dir.path().join("a/b/c");
    let backend = IoBackend::File;

    backend.create_dir_all(&nested).await.expect("create_dir_all");
    assert!(backend.exists(&nested).await, "nested dir created");
}

#[tokio::test]
async fn read_dir_lists_entries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = IoBackend::File;

    // Create a few files.
    for name in &["x.wal", "y.wal", "z.wal"] {
        let path = dir.path().join(name);
        let mut f = backend
            .open(&path, OpenOptions::create_new_rw())
            .await
            .expect("open");
        f.write_at(b"data", 0).await.expect("write");
    }

    let entries = backend.read_dir(dir.path()).await.expect("read_dir");
    assert_eq!(entries.len(), 3, "three files listed");
}

#[tokio::test]
async fn exists_returns_false_for_missing_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = IoBackend::File;
    assert!(!backend.exists(dir.path().join("nope.wal")).await);
}

#[tokio::test]
async fn arc_backend_shares_across_tasks() {
    // Verify that Arc<IoBackend> can be cloned and used from multiple
    // "tasks" (here just sequential calls on the same Arc).
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = Arc::new(IoBackend::File);

    let path1 = dir.path().join("a.wal");
    let path2 = dir.path().join("b.wal");

    let b1 = backend.clone();
    let b2 = backend.clone();

    let mut f1 = b1
        .open(&path1, OpenOptions::create_new_rw())
        .await
        .expect("open 1");
    let mut f2 = b2
        .open(&path2, OpenOptions::create_new_rw())
        .await
        .expect("open 2");

    f1.write_at(b"aaa", 0).await.expect("write 1");
    f2.write_at(b"bbb", 0).await.expect("write 2");

    assert!(backend.exists(&path1).await);
    assert!(backend.exists(&path2).await);
}
