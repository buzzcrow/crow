// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Segment file format tests (W3) — `SimDisk` backend.

use bytes::Bytes;
use crowdb_kv::paxos::roles::PxBallot;
use crowdb_kv::wal::pipeline_backend::WalBlockAlignment;
use crowdb_kv::wal::record::{RecordType, WALRecord, WalRecordFormat};
use crowdb_kv::wal::segment::{SegmentReader, WalSegment, SEG_HEADER_LEN};
use crowdb_kv::wal::{IoBackend, MemBlockDevice};
use std::path::PathBuf;

fn sim_backend() -> IoBackend {
    IoBackend::MemBlock(MemBlockDevice::new())
}

/// 4 KiB-aligned block device (SSD/NVMe model). Every physical write is widened
/// to the enclosing 4 KiB unit, so a sealed segment ends in zero padding out to
/// the block boundary — the B1 scenario the reader must tolerate.
fn aligned_backend() -> IoBackend {
    IoBackend::MemBlock(MemBlockDevice::with_alignment(
        WalBlockAlignment::default_aligned(),
    ))
}

fn accepted_record(group: u64, slot: u64, payload_len: usize) -> WALRecord {
    let payload: Vec<u8> = (0..payload_len)
        .map(|i| u8::try_from(i % 251).expect("i % 251 < u8::MAX"))
        .collect();
    WALRecord {
        record_type: RecordType::Accepted,
        group_id: group,
        term: 1,
        slot,
        ballot: PxBallot::new(0, 1),
        payload: Bytes::from(payload),
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn create_segment_writes_header() {
    let backend = sim_backend();
    let dir = PathBuf::from("/wal/group1");
    let seg = WalSegment::create(&backend, &dir, 1, 100).await.unwrap();
    assert_eq!(seg.segment_id, 1);
    assert_eq!(seg.group_id, 100);
    assert_eq!(seg.len(), SEG_HEADER_LEN as u64);
    assert!(!seg.is_sealed());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn append_and_seal() {
    let backend = sim_backend();
    let dir = PathBuf::from("/wal/group1");
    let mut seg = WalSegment::create(&backend, &dir, 1, 100).await.unwrap();

    let r1 = WALRecord::from_promised(100, 1, 10, PxBallot::new(0, 1));
    let off1 = seg.append(&r1).await.unwrap();
    assert_eq!(off1, SEG_HEADER_LEN as u64);
    assert_eq!(seg.record_count, 1);

    let r2 = WALRecord {
        record_type: RecordType::Accepted,
        group_id: 100,
        term: 1,
        slot: 20,
        ballot: PxBallot::new(0, 1),
        payload: Bytes::from_static(b"test payload"),
    };
    let off2 = seg.append(&r2).await.unwrap();
    assert!(off2 > off1);
    assert_eq!(seg.record_count, 2);
    assert_eq!(seg.min_slot, 10);
    assert_eq!(seg.max_slot, 20);

    seg.seal().await.unwrap();
    assert!(seg.is_sealed());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn segment_reader_reads_text_line_records() {
    let backend = sim_backend();
    let dir = PathBuf::from("/wal/group-text");
    let mut seg = WalSegment::create_with_format(&backend, &dir, 6, 201, WalRecordFormat::TextLine)
        .await
        .unwrap();

    let r1 = WALRecord::from_promised(201, 1, 10, PxBallot::new(0, 1));
    let r2 = accepted_record(201, 20, 7);
    let off1 = seg.append(&r1).await.unwrap();
    let off2 = seg.append(&r2).await.unwrap();
    assert!(off2 > off1);
    seg.seal().await.unwrap();

    let mut reader = SegmentReader::open(&backend, seg.path()).await.unwrap();
    let (decoded1, decoded_off1) = reader.next_record().await.unwrap().unwrap();
    let (decoded2, decoded_off2) = reader.next_record().await.unwrap().unwrap();
    assert_eq!(decoded_off1, off1);
    assert_eq!(decoded_off2, off2);
    assert_eq!(decoded1, r1);
    assert_eq!(decoded2, r2);
    assert!(reader.next_record().await.unwrap().is_none());

    let footer = reader.read_footer().await.unwrap().unwrap();
    assert_eq!(footer.min_slot, 10);
    assert_eq!(footer.max_slot, 20);
    assert_eq!(footer.record_count, 2);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn segment_reader_reads_records() {
    let backend = sim_backend();
    let dir = PathBuf::from("/wal/group2");
    let mut seg = WalSegment::create(&backend, &dir, 5, 200).await.unwrap();

    for i in 1..=3 {
        let r = WALRecord::from_promised(200, 1, i * 10, PxBallot::new(0, 1));
        seg.append(&r).await.unwrap();
    }
    seg.seal().await.unwrap();

    // Now read it back.
    let mut reader = SegmentReader::open(&backend, seg.path()).await.unwrap();
    assert_eq!(reader.header.segment_id, 5);
    assert_eq!(reader.header.group_id, 200);

    let mut count = 0;
    while let Ok(Some((rec, _offset))) = reader.next_record().await {
        count += 1;
        assert_eq!(rec.record_type, RecordType::Promised);
        assert_eq!(rec.group_id, 200);
    }
    assert_eq!(count, 3);

    // Footer should be readable.
    let footer = reader.read_footer().await.unwrap().unwrap();
    assert_eq!(footer.min_slot, 10);
    assert_eq!(footer.max_slot, 30);
    assert_eq!(footer.record_count, 3);
}

/// B1: on a 4 KiB-aligned device a sealed segment is padded to the block
/// boundary, so the footer no longer sits at the physical end of the file. The
/// reader must still recover every record (stopping at the footer magic, not
/// running into padding) and locate the footer past the trailing zero padding.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn aligned_sealed_segment_recovers_records_and_footer_past_padding() {
    let backend = aligned_backend();
    let dir = PathBuf::from("/dev/nvme0/group7");
    let mut seg = WalSegment::create(&backend, &dir, 9, 7).await.unwrap();

    // Append enough payload to span several 4 KiB blocks so the footer lands
    // well past the first block and the tail padding is non-trivial.
    let count = 40u64;
    for slot in 1..=count {
        let r = accepted_record(7, slot, 200);
        seg.append(&r).await.unwrap();
    }
    seg.seal().await.unwrap();
    assert!(seg.is_sealed());

    let mut reader = SegmentReader::open(&backend, seg.path()).await.unwrap();
    assert_eq!(reader.header.segment_id, 9);
    assert_eq!(reader.header.group_id, 7);

    let mut recovered = 0u64;
    while let Some((rec, _off)) = reader.next_record().await.unwrap() {
        recovered += 1;
        assert_eq!(rec.record_type, RecordType::Accepted);
        assert_eq!(rec.slot, recovered);
        assert_eq!(rec.payload.len(), 200);
    }
    assert_eq!(recovered, count, "every record recovered past padding");

    // Footer is recoverable even though it is not at the physical file end.
    let footer = reader.read_footer().await.unwrap().unwrap();
    assert_eq!(footer.min_slot, 1);
    assert_eq!(footer.max_slot, count);
    assert_eq!(footer.record_count, u32::try_from(count).unwrap());
}

/// B1: an *unsealed* aligned segment ends in zero padding after the last
/// record (the final append was widened to the block boundary). The reader
/// must stop cleanly at the zero `frame_len` padding marker — recovering all
/// records without reporting truncation — and report no footer.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn aligned_unsealed_segment_stops_at_padding_without_truncation() {
    let backend = aligned_backend();
    let dir = PathBuf::from("/dev/nvme0/group8");
    let mut seg = WalSegment::create(&backend, &dir, 3, 8).await.unwrap();

    let count = 12u64;
    for slot in 1..=count {
        let r = accepted_record(8, slot, 100);
        seg.append(&r).await.unwrap();
    }
    // Deliberately NOT sealed — the last write left trailing zero padding.

    let mut reader = SegmentReader::open(&backend, seg.path()).await.unwrap();
    let mut recovered = 0u64;
    loop {
        match reader.next_record().await {
            Ok(Some((rec, _off))) => {
                recovered += 1;
                assert_eq!(rec.slot, recovered);
            }
            Ok(None) => break,
            Err((err, off)) => panic!("unexpected reader error {err} at offset {off}"),
        }
    }
    assert_eq!(recovered, count, "all records recovered; padding is clean EOF");

    // No footer on an unsealed segment, even with trailing padding present.
    assert!(reader.read_footer().await.unwrap().is_none());
}
