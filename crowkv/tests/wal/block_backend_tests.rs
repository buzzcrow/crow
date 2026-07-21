// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Tests for the simulated `BlockDevice` backend's two write implementations:
//! byte-addressable (unaligned) vs block-aligned read-modify-write.

use crowkv::wal::pipeline_backend::WalBlockAlignment;
use crowkv::wal::{IoBackend, MemBlockDevice, OpenOptions};
use std::path::Path;

async fn open_segment(backend: &IoBackend, path: &str) -> crowkv::wal::WalFile {
    backend
        .open(Path::new(path), OpenOptions::create_rw())
        .await
        .expect("open segment")
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn unaligned_device_writes_payload_directly_without_amplification() {
    let device = MemBlockDevice::new();
    assert_eq!(device.alignment(), WalBlockAlignment::Unaligned);
    let backend = IoBackend::MemBlock(device.clone());

    let mut file = open_segment(&backend, "/dev/mem/seg-0000001.ck").await;
    file.write_at(b"hello", 10).await.unwrap();

    let mut buf = [0u8; 5];
    file.read_exact_at(&mut buf, 10).await.unwrap();
    assert_eq!(&buf, b"hello");

    assert_eq!(device.write_count(), 1);
    assert_eq!(device.logical_bytes_written(), 5);
    assert_eq!(
        device.physical_bytes_written(),
        5,
        "no amplification for byte-addressable media"
    );
    assert_eq!(device.rmw_count(), 0);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn aligned_device_partial_write_does_read_modify_write() {
    let device = MemBlockDevice::with_alignment(WalBlockAlignment::default_aligned());
    assert_eq!(
        device.alignment(),
        WalBlockAlignment::Aligned { io_unit_bytes: 4096 }
    );
    let backend = IoBackend::MemBlock(device.clone());

    let mut file = open_segment(&backend, "/dev/nvme0/seg-0000001.ck").await;
    // 200-byte payload at offset 100: not block-aligned, requires RMW of the
    // enclosing [0, 4096) block.
    let payload: Vec<u8> = (0..200u32).map(|i| (i % 251) as u8).collect();
    file.write_at(&payload, 100).await.unwrap();

    let mut buf = vec![0u8; 200];
    file.read_exact_at(&mut buf, 100).await.unwrap();
    assert_eq!(buf, payload, "payload round-trips through RMW");

    assert_eq!(device.logical_bytes_written(), 200);
    assert_eq!(
        device.physical_bytes_written(),
        4096,
        "widened to one aligned block"
    );
    assert_eq!(device.rmw_count(), 1);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn unaligned_device_counts_writes_and_durable_flushes() {
    let device = MemBlockDevice::new();
    let backend = IoBackend::MemBlock(device.clone());

    let mut file = open_segment(&backend, "/dev/mem/seg-0000002.ck").await;
    file.write_at(b"ab", 0).await.unwrap();
    file.write_at(b"cd", 2).await.unwrap();
    file.fdatasync().await.unwrap();
    file.fdatasync().await.unwrap();

    assert_eq!(device.write_count(), 2);
    assert_eq!(device.fdatasync_count(), 2);
    assert_eq!(device.logical_bytes_written(), 4);
    assert_eq!(device.physical_bytes_written(), 4);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn aligned_device_rmw_preserves_neighbouring_bytes_in_same_block() {
    let device = MemBlockDevice::with_alignment(WalBlockAlignment::default_aligned());
    let backend = IoBackend::MemBlock(device.clone());

    let mut file = open_segment(&backend, "/dev/nvme0/seg-0000001.ck").await;
    // Two sub-block writes into the same 4 KiB block. The second must not erase
    // the first (the read part of read-modify-write).
    file.write_at(b"AAAA", 0).await.unwrap();
    file.write_at(b"B", 10).await.unwrap();

    let mut head = [0u8; 4];
    file.read_exact_at(&mut head, 0).await.unwrap();
    assert_eq!(&head, b"AAAA");
    let mut mid = [0u8; 1];
    file.read_exact_at(&mut mid, 10).await.unwrap();
    assert_eq!(&mid, b"B");

    // Both writes are partial blocks → two RMWs; each rewrites the 4 KiB block.
    assert_eq!(device.write_count(), 2);
    assert_eq!(device.rmw_count(), 2);
    assert_eq!(device.physical_bytes_written(), 4096 * 2);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn aligned_device_block_aligned_write_avoids_amplification() {
    let device = MemBlockDevice::with_alignment(WalBlockAlignment::default_aligned());
    let backend = IoBackend::MemBlock(device.clone());

    let mut file = open_segment(&backend, "/dev/nvme0/seg-0000001.ck").await;
    let block = vec![7u8; 4096];
    // Offset and length both aligned to the 4 KiB I/O unit → no RMW.
    file.write_at(&block, 4096).await.unwrap();

    assert_eq!(device.logical_bytes_written(), 4096);
    assert_eq!(device.physical_bytes_written(), 4096, "exact block, no widening");
    assert_eq!(device.rmw_count(), 0);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn aligned_device_supports_custom_io_unit() {
    let device = MemBlockDevice::with_alignment(WalBlockAlignment::Aligned { io_unit_bytes: 512 });
    let backend = IoBackend::MemBlock(device.clone());

    let mut file = open_segment(&backend, "/dev/scm0/seg-0000001.ck").await;
    file.write_at(&[1u8; 100], 0).await.unwrap();

    // 100 bytes at offset 0 widens to one 512-byte sector.
    assert_eq!(device.physical_bytes_written(), 512);
    assert_eq!(device.rmw_count(), 1);
}
