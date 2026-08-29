// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv::wal::pipeline_backend::{WalBlockAlignment, WalBlockPipelineBackend, WalPipelineBackend};

#[test]
fn mem_block_has_no_alignment_requirement() {
    let backend = WalPipelineBackend::mem_block();
    match backend {
        WalPipelineBackend::MemBlock(_) => {}
        other => panic!("expected MemBlock, got {other:?}"),
    }
}

#[test]
fn aligned_block_write_detects_read_modify_write() {
    let backend = WalBlockPipelineBackend::new("nvme0n1", WalBlockAlignment::Aligned { io_unit_bytes: 4096 });

    let plan = backend.plan_write(100, 200);
    assert_eq!(plan.aligned_offset, 0);
    assert_eq!(plan.aligned_len, 4096);
    assert_eq!(plan.payload_offset_within_aligned, 100);
    assert!(plan.requires_read_modify_write);
}

#[test]
fn aligned_block_write_that_matches_boundary_is_direct() {
    let backend = WalBlockPipelineBackend::new("nvme0n1", WalBlockAlignment::Aligned { io_unit_bytes: 4096 });

    let plan = backend.plan_write(4096, 4096);
    assert_eq!(plan.aligned_offset, 4096);
    assert_eq!(plan.aligned_len, 4096);
    assert_eq!(plan.payload_offset_within_aligned, 0);
    assert!(!plan.requires_read_modify_write);
}

#[test]
fn unaligned_block_write_has_no_alignment_penalty() {
    let backend = WalBlockPipelineBackend::new("scm0", WalBlockAlignment::Unaligned);

    let plan = backend.plan_write(123, 77);
    assert_eq!(plan.aligned_offset, 123);
    assert_eq!(plan.aligned_len, 77);
    assert_eq!(plan.payload_offset_within_aligned, 0);
    assert!(!plan.requires_read_modify_write);
}

#[test]
fn aligned_block_supports_non_default_unit_sizes() {
    let backend = WalBlockPipelineBackend::new(
        "scm0",
        WalBlockAlignment::Aligned {
            io_unit_bytes: 16 * 1024,
        },
    );

    let plan = backend.plan_write(8 * 1024, 1024);
    assert_eq!(plan.aligned_offset, 0);
    assert_eq!(plan.aligned_len, 16 * 1024);
    assert_eq!(plan.payload_offset_within_aligned, 8 * 1024);
    assert!(plan.requires_read_modify_write);
}
