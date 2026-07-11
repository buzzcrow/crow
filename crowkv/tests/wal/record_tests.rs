//! `WALRecord` codec tests (W2).

use bytes::Bytes;
use crowkv::paxos::roles::{PxBallot, PxLogEntry, PxLogEntryKind};
use crowkv::wal::record::{RecordError, RecordType, WALRecord, MIN_RECORD_SIZE};

#[test]
fn encode_decode_roundtrip_accepted() {
    let entry = PxLogEntry {
        slot: 42,
        ballot: PxBallot::new(3, 7),
        term: 10,
        kind: PxLogEntryKind::Write,
        payload: Bytes::from_static(b"hello world"),
        client_id: Some(99),
        seq: Some(5),
    };
    let record = WALRecord::from_accepted(1, &entry);
    assert_eq!(record.record_type, RecordType::Accepted);
    assert_eq!(record.group_id, 1);
    assert_eq!(record.slot, 42);
    assert_eq!(record.ballot, PxBallot::new(3, 7));
    assert_eq!(record.term, 10);

    let encoded = record.encode();
    assert!(encoded.len() >= MIN_RECORD_SIZE);

    let (decoded, consumed) = WALRecord::decode(&encoded).unwrap();
    assert_eq!(consumed, encoded.len());
    assert_eq!(decoded.record_type, RecordType::Accepted);
    assert_eq!(decoded.group_id, 1);
    assert_eq!(decoded.slot, 42);
    assert_eq!(decoded.ballot, PxBallot::new(3, 7));
    assert_eq!(decoded.term, 10);

    // Verify accepted payload roundtrip.
    let recovered = decoded.to_log_entry().unwrap();
    assert_eq!(recovered.slot, 42);
    assert_eq!(recovered.ballot, PxBallot::new(3, 7));
    assert_eq!(recovered.term, 10);
    assert_eq!(recovered.kind, PxLogEntryKind::Write);
    assert_eq!(recovered.payload, Bytes::from_static(b"hello world"));
    assert_eq!(recovered.client_id, Some(99));
    assert_eq!(recovered.seq, Some(5));
}

#[test]
fn encode_decode_roundtrip_promised() {
    let record = WALRecord::from_promised(5, 20, 100, PxBallot::new(1, 3));
    assert_eq!(record.record_type, RecordType::Promised);
    assert!(record.payload.is_empty());

    let encoded = record.encode();
    let (decoded, consumed) = WALRecord::decode(&encoded).unwrap();
    assert_eq!(consumed, encoded.len());
    assert_eq!(decoded.record_type, RecordType::Promised);
    assert_eq!(decoded.group_id, 5);
    assert_eq!(decoded.term, 20);
    assert_eq!(decoded.slot, 100);
    assert_eq!(decoded.ballot, PxBallot::new(1, 3));
}

#[test]
fn encode_decode_roundtrip_vote_granted() {
    let record = WALRecord::from_vote_granted(2, 15, 42);
    assert_eq!(record.record_type, RecordType::VoteGranted);
    assert_eq!(record.slot, 0);

    let encoded = record.encode();
    let (decoded, _) = WALRecord::decode(&encoded).unwrap();
    assert_eq!(decoded.record_type, RecordType::VoteGranted);
    assert_eq!(decoded.group_id, 2);
    assert_eq!(decoded.term, 15);
    assert_eq!(decoded.voted_for_id(), Some(42));
}

#[test]
fn decode_truncated_returns_error() {
    let record = WALRecord::from_promised(1, 1, 1, PxBallot::new(0, 0));
    let encoded = record.encode();
    let truncated = &encoded[..encoded.len() - 5];
    assert!(matches!(
        WALRecord::decode(truncated),
        Err(RecordError::Truncated)
    ));
}

#[test]
fn decode_corrupted_crc_returns_bad_crc() {
    let record = WALRecord::from_promised(1, 1, 1, PxBallot::new(0, 0));
    let mut encoded = record.encode();
    // Corrupt a byte in the body (after frame_len, before crc).
    encoded[8] ^= 0xFF;
    match WALRecord::decode(&encoded) {
        Err(RecordError::BadCrc { .. }) => {}
        other => panic!("expected BadCrc, got {other:?}"),
    }
}

#[test]
fn noop_entry_roundtrip() {
    let entry = PxLogEntry {
        slot: 1,
        ballot: PxBallot::new(0, 1),
        term: 1,
        kind: PxLogEntryKind::NoOp,
        payload: Bytes::new(),
        client_id: None,
        seq: None,
    };
    let record = WALRecord::from_accepted(1, &entry);
    let encoded = record.encode();
    let (decoded, _) = WALRecord::decode(&encoded).unwrap();
    let recovered = decoded.to_log_entry().unwrap();
    assert_eq!(recovered.kind, PxLogEntryKind::NoOp);
    assert!(recovered.payload.is_empty());
    assert!(recovered.client_id.is_none());
    assert!(recovered.seq.is_none());
}
