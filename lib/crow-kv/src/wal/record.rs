// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! WAL record codec — **FREEZE GATE** (P2 W2).
//!
//! After this module lands the byte layout and `version` are frozen.
//! Future format changes bump `version`; no field reorder / removal.
//!
//! ## On-disk byte layout (version 1)
//!
//! ```text
//! [frame_len   : u32 LE]  — bytes from magic through crc32c (inclusive)
//! [magic       : u32 LE]  — 0x4352_4F57 ("CROW")
//! [version     : u16 LE]  — 1
//! [record_type : u8    ]  — RecordType discriminant
//! [group_id    : u64 LE]
//! [term        : u64 LE]
//! [slot        : u64 LE]  — 0 sentinel for n/a (e.g. DedupCheckpoint)
//! [ballot_round: u64 LE]
//! [ballot_lid  : u64 LE]  — ballot leader_id
//! [payload_len : u32 LE]
//! [payload     : [u8; payload_len]]
//! [crc32c      : u32 LE]  — CRC of [magic..end_of_payload]
//! ```
//!
//! `frame_len = HEADER_BODY_LEN + payload_len + 4(crc)`
//! where `HEADER_BODY_LEN = 4+2+1+8+8+8+8+8+4 = 51`.

use bytes::Bytes;
use std::fmt::Write;

use crate::paxos::roles::{PxBallot, PxLogEntry, SlotIndex};
use crate::paxos::{PxGroupId, PxTerm};

/// Magic number: ASCII "CROW" in little-endian.
pub const WAL_MAGIC: u32 = 0x4352_4F57;

/// Current frozen record version.
pub const WAL_VERSION: u16 = 1;

/// Size of the header body (magic through `payload_len`), excluding `frame_len` prefix.
pub const HEADER_BODY_LEN: usize = 4 + 2 + 1 + 8 + 8 + 8 + 8 + 8 + 4; // 51

/// Minimum encoded record size: `frame_len(4)` + `header_body(51)` + crc(4) = 59.
pub const MIN_RECORD_SIZE: usize = 4 + HEADER_BODY_LEN + 4;

/// Encoding format used for WAL records inside a segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum WalRecordFormat {
    Auto,
    Binary,
    TextLine,
}

/// Discriminant for [`RecordType`]. Stable; append-only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    Promised = 0,
    Accepted = 1,
    VoteGranted = 2,
}

impl RecordType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Promised),
            1 => Some(Self::Accepted),
            2 => Some(Self::VoteGranted),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Promised => "Promised",
            Self::Accepted => "Accepted",
            Self::VoteGranted => "VoteGranted",
        }
    }

    fn from_name(value: &str) -> Result<Self, RecordError> {
        match value {
            "Promised" => Ok(Self::Promised),
            "Accepted" => Ok(Self::Accepted),
            "VoteGranted" => Ok(Self::VoteGranted),
            _ => Err(RecordError::BadText(format!("unknown record type: {value}"))),
        }
    }
}

/// Typed decode error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordError {
    Truncated,
    BadCrc { expected: u32, got: u32 },
    BadMagic(u32),
    BadVersion(u16),
    BadRecordType(u8),
    BadText(String),
}

impl std::fmt::Display for RecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(f, "truncated record"),
            Self::BadCrc { expected, got } => {
                write!(f, "CRC mismatch: expected {expected:#x}, got {got:#x}")
            }
            Self::BadMagic(m) => write!(f, "bad magic: {m:#x}"),
            Self::BadVersion(v) => write!(f, "bad version: {v}"),
            Self::BadRecordType(t) => write!(f, "bad record type: {t}"),
            Self::BadText(msg) => write!(f, "bad text record: {msg}"),
        }
    }
}

impl std::error::Error for RecordError {}

/// One WAL record (in-memory representation).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WALRecord {
    pub record_type: RecordType,
    pub group_id: PxGroupId,
    pub term: PxTerm,
    pub slot: SlotIndex,
    pub ballot: PxBallot,
    pub payload: Bytes,
}

/// Zero-copy binary frame for a single WAL record.
///
/// The fixed-size header pieces are stored inline; the payload is borrowed via
/// `Bytes`, so no data copy is needed when the frame is built or cloned.
#[derive(Clone, Debug)]
pub struct RecordFrame {
    frame_len: [u8; 4],
    header: [u8; HEADER_BODY_LEN],
    payload: Bytes,
    crc: [u8; 4],
}

impl RecordFrame {
    /// Total on-disk size of the framed record (`frame_len` + header + payload + crc).
    #[must_use]
    pub fn total_len(&self) -> usize {
        4 + HEADER_BODY_LEN + self.payload.len() + 4
    }

    /// Append this record's slices to `slices` so the whole batch can be written
    /// with a single vectored write.
    pub fn append_io_slices<'a>(&'a self, slices: &mut Vec<std::io::IoSlice<'a>>) {
        slices.push(std::io::IoSlice::new(&self.frame_len));
        slices.push(std::io::IoSlice::new(&self.header));
        slices.push(std::io::IoSlice::new(&self.payload));
        slices.push(std::io::IoSlice::new(&self.crc));
    }
}

/// Write the fixed-size header body (everything after `frame_len` and before
/// the payload) into `buf`.
fn write_header_body(buf: &mut [u8; HEADER_BODY_LEN], record: &WALRecord, payload_len: usize) {
    let mut off = 0;
    buf[off..off + 4].copy_from_slice(&WAL_MAGIC.to_le_bytes());
    off += 4;
    buf[off..off + 2].copy_from_slice(&WAL_VERSION.to_le_bytes());
    off += 2;
    buf[off] = record.record_type as u8;
    off += 1;
    buf[off..off + 8].copy_from_slice(&record.group_id.to_le_bytes());
    off += 8;
    buf[off..off + 8].copy_from_slice(&record.term.to_le_bytes());
    off += 8;
    buf[off..off + 8].copy_from_slice(&record.slot.to_le_bytes());
    off += 8;
    buf[off..off + 8].copy_from_slice(&record.ballot.round.to_le_bytes());
    off += 8;
    buf[off..off + 8].copy_from_slice(&record.ballot.leader_id.to_le_bytes());
    off += 8;
    buf[off..off + 4].copy_from_slice(
        &u32::try_from(payload_len)
            .expect("payload_len exceeds u32")
            .to_le_bytes(),
    );
    off += 4;
    debug_assert_eq!(off, HEADER_BODY_LEN);
}

impl WALRecord {
    pub const TEXT_PREFIX: &'static str = "CROW_WAL_TEXT";

    /// Zero-copy binary frame. Holds the fixed-size pieces and borrows the
    /// payload via `Bytes`. The payload is not copied; cloning the frame only
    /// bumps the ref-count.
    ///
    /// # Panics
    /// Panics if `frame_len` or `payload_len` exceeds `u32::MAX`.
    #[must_use]
    pub fn encode_frame(&self) -> RecordFrame {
        let payload_len = self.payload.len();
        let frame_len_value = HEADER_BODY_LEN + payload_len + 4; // +4 for crc
        let mut frame_len = [0u8; 4];
        frame_len.copy_from_slice(
            &u32::try_from(frame_len_value)
                .expect("frame_len exceeds u32")
                .to_le_bytes(),
        );

        let mut header = [0u8; HEADER_BODY_LEN];
        write_header_body(&mut header, self, payload_len);

        let crc = crow_tree_ffi::crc32c_update(crow_tree_ffi::crc32c(&header), &self.payload);
        let mut crc_bytes = [0u8; 4];
        crc_bytes.copy_from_slice(&crc.to_le_bytes());

        RecordFrame {
            frame_len,
            header,
            payload: self.payload.clone(),
            crc: crc_bytes,
        }
    }

    /// Encode to the on-disk byte layout. Returns the complete framed record.
    ///
    /// This is kept for tests and legacy callers. The hot path uses
    /// [`Self::encode_frame`] to avoid copying the payload.
    ///
    /// # Panics
    /// Panics if `frame_len` or `payload_len` exceeds `u32::MAX`.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let frame = self.encode_frame();
        let mut buf = Vec::with_capacity(frame.total_len());
        buf.extend_from_slice(&frame.frame_len);
        buf.extend_from_slice(&frame.header);
        buf.extend_from_slice(&frame.payload);
        buf.extend_from_slice(&frame.crc);
        buf
    }

    /// Decode one record from `data`. Returns `(record, bytes_consumed)`.
    ///
    /// `data` must start at a `frame_len` prefix. On success, `bytes_consumed`
    /// is the total record size including the `frame_len` prefix.
    ///
    /// # Errors
    /// Returns `RecordError` if the data is truncated, has bad CRC, bad magic,
    /// bad version, or bad record type.
    pub fn decode(data: &[u8]) -> Result<(Self, usize), RecordError> {
        if data.len() < MIN_RECORD_SIZE {
            return Err(RecordError::Truncated);
        }
        let frame_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let total = 4 + frame_len;
        if data.len() < total {
            return Err(RecordError::Truncated);
        }
        let body = &data[4..total];

        // Verify CRC (covers everything except the last 4 bytes which are the CRC itself).
        let crc_stored = u32::from_le_bytes([
            body[frame_len - 4],
            body[frame_len - 3],
            body[frame_len - 2],
            body[frame_len - 1],
        ]);
        let crc_computed = crow_tree_ffi::crc32c(&body[..frame_len - 4]);
        if crc_stored != crc_computed {
            return Err(RecordError::BadCrc {
                expected: crc_computed,
                got: crc_stored,
            });
        }

        let mut off = 0usize;
        let magic = read_u32(body, &mut off);
        if magic != WAL_MAGIC {
            return Err(RecordError::BadMagic(magic));
        }
        let version = read_u16(body, &mut off);
        if version != WAL_VERSION {
            return Err(RecordError::BadVersion(version));
        }
        let rt_raw = body[off];
        off += 1;
        let record_type = RecordType::from_u8(rt_raw).ok_or(RecordError::BadRecordType(rt_raw))?;
        let group_id = read_u64(body, &mut off);
        let term = read_u64(body, &mut off);
        let slot = read_u64(body, &mut off);
        let ballot_round = read_u64(body, &mut off);
        let ballot_leader = read_u64(body, &mut off);
        let payload_len = read_u32(body, &mut off) as usize;

        if off + payload_len + 4 > body.len() {
            return Err(RecordError::Truncated);
        }
        let payload = Bytes::copy_from_slice(&body[off..off + payload_len]);

        Ok((
            Self {
                record_type,
                group_id,
                term,
                slot,
                ballot: PxBallot::new(ballot_round, ballot_leader),
                payload,
            },
            total,
        ))
    }

    /// Encode to a self-checking UTF-8 line format.
    #[must_use]
    pub fn encode_text_line(&self) -> String {
        let mut line = format!(
            "{} v={} type={} group_id={} term={} slot={} ballot_round={} ballot_leader_id={} payload_hex={}",
            Self::TEXT_PREFIX,
            WAL_VERSION,
            self.record_type.as_str(),
            self.group_id,
            self.term,
            self.slot,
            self.ballot.round,
            self.ballot.leader_id,
            encode_hex(&self.payload)
        );
        let crc = crow_tree_ffi::crc32c(line.as_bytes());
        let _ = writeln!(line, " crc32c={crc:08x}");
        line
    }

    /// Decode one UTF-8 text WAL record line.
    ///
    /// # Errors
    /// Returns `RecordError` if the line is malformed, has a bad CRC, uses an
    /// unsupported version, or contains an unknown record type.
    pub fn decode_text_line(line: &str) -> Result<Self, RecordError> {
        let line = line.trim_end_matches(['\r', '\n']);
        let (body, crc_hex) = line
            .rsplit_once(" crc32c=")
            .ok_or_else(|| RecordError::BadText("missing crc32c field".to_string()))?;
        let got = parse_hex_u32(crc_hex)?;
        let expected = crow_tree_ffi::crc32c(body.as_bytes());
        if got != expected {
            return Err(RecordError::BadCrc { expected, got });
        }

        let mut fields = body.split(' ');
        let prefix = fields
            .next()
            .ok_or_else(|| RecordError::BadText("missing prefix".to_string()))?;
        if prefix != Self::TEXT_PREFIX {
            return Err(RecordError::BadText(format!("bad prefix: {prefix}")));
        }

        let mut version = None;
        let mut record_type = None;
        let mut group_id = None;
        let mut term = None;
        let mut slot = None;
        let mut ballot_round = None;
        let mut ballot_leader_id = None;
        let mut payload = None;

        for field in fields {
            let (key, value) = field
                .split_once('=')
                .ok_or_else(|| RecordError::BadText(format!("malformed field: {field}")))?;
            match key {
                "v" => version = Some(parse_u16(value, key)?),
                "type" => record_type = Some(RecordType::from_name(value)?),
                "group_id" => group_id = Some(parse_u64(value, key)?),
                "term" => term = Some(parse_u64(value, key)?),
                "slot" => slot = Some(parse_u64(value, key)?),
                "ballot_round" => ballot_round = Some(parse_u64(value, key)?),
                "ballot_leader_id" => ballot_leader_id = Some(parse_u64(value, key)?),
                "payload_hex" => payload = Some(decode_hex(value)?),
                _ => return Err(RecordError::BadText(format!("unknown field: {key}"))),
            }
        }

        let version = version.ok_or_else(|| RecordError::BadText("missing v field".to_string()))?;
        if version != WAL_VERSION {
            return Err(RecordError::BadVersion(version));
        }

        Ok(Self {
            record_type: record_type.ok_or_else(|| RecordError::BadText("missing type field".to_string()))?,
            group_id: group_id.ok_or_else(|| RecordError::BadText("missing group_id field".to_string()))?,
            term: term.ok_or_else(|| RecordError::BadText("missing term field".to_string()))?,
            slot: slot.ok_or_else(|| RecordError::BadText("missing slot field".to_string()))?,
            ballot: PxBallot::new(
                ballot_round.ok_or_else(|| RecordError::BadText("missing ballot_round field".to_string()))?,
                ballot_leader_id
                    .ok_or_else(|| RecordError::BadText("missing ballot_leader_id field".to_string()))?,
            ),
            payload: Bytes::from(
                payload.ok_or_else(|| RecordError::BadText("missing payload_hex field".to_string()))?,
            ),
        })
    }

    // ── Accepted payload helpers ───────────────────────────

    /// Encode a [`PxLogEntry`] into a `WALRecord` with `RecordType::Accepted`.
    #[must_use]
    pub fn from_accepted(group_id: PxGroupId, entry: &PxLogEntry) -> Self {
        Self {
            record_type: RecordType::Accepted,
            group_id,
            term: entry.term,
            slot: entry.slot,
            ballot: entry.ballot,
            payload: encode_accepted_payload(entry),
        }
    }

    /// Encode a `Promised` record (no payload).
    #[must_use]
    pub fn from_promised(group_id: PxGroupId, term: PxTerm, slot: SlotIndex, ballot: PxBallot) -> Self {
        Self {
            record_type: RecordType::Promised,
            group_id,
            term,
            slot,
            ballot,
            payload: Bytes::new(),
        }
    }

    /// Encode a `VoteGranted` record. Payload carries `voted_for_id`.
    #[must_use]
    pub fn from_vote_granted(group_id: PxGroupId, term: PxTerm, voted_for_id: u64) -> Self {
        Self {
            record_type: RecordType::VoteGranted,
            group_id,
            term,
            slot: 0,
            ballot: PxBallot::new(0, 0),
            payload: Bytes::copy_from_slice(&voted_for_id.to_le_bytes()),
        }
    }

    /// Decode the `Accepted` payload back to a [`PxLogEntry`].
    ///
    /// The WAL header fields (slot, ballot, term) are merged with the payload
    /// bytes.
    #[must_use]
    pub fn to_log_entry(&self) -> Option<PxLogEntry> {
        if self.record_type != RecordType::Accepted {
            return None;
        }
        Some(decode_accepted_payload(self))
    }

    /// For `VoteGranted` records, extract the `voted_for` node id.
    #[must_use]
    pub fn voted_for_id(&self) -> Option<u64> {
        if self.record_type != RecordType::VoteGranted {
            return None;
        }
        if self.payload.len() < 8 {
            return None;
        }
        Some(u64::from_le_bytes([
            self.payload[0],
            self.payload[1],
            self.payload[2],
            self.payload[3],
            self.payload[4],
            self.payload[5],
            self.payload[6],
            self.payload[7],
        ]))
    }
}

// ── Accepted payload (de)serialization ──────────────────────

/// Accepted payload layout:
/// ```text
/// [inner : rest]  — PxLogEntry.payload bytes
/// ```
fn encode_accepted_payload(entry: &PxLogEntry) -> Bytes {
    entry.payload.clone()
}

fn decode_accepted_payload(rec: &WALRecord) -> PxLogEntry {
    PxLogEntry {
        slot: rec.slot,
        ballot: rec.ballot,
        term: rec.term,
        payload: rec.payload.clone(),
    }
}

// ── Primitive read helpers ──────────────────────────────────

fn parse_u16(value: &str, field: &str) -> Result<u16, RecordError> {
    value
        .parse::<u16>()
        .map_err(|e| RecordError::BadText(format!("invalid {field}: {e}")))
}

fn parse_u64(value: &str, field: &str) -> Result<u64, RecordError> {
    value
        .parse::<u64>()
        .map_err(|e| RecordError::BadText(format!("invalid {field}: {e}")))
}

pub(super) fn parse_hex_u32(value: &str) -> Result<u32, RecordError> {
    u32::from_str_radix(value, 16).map_err(|e| RecordError::BadText(format!("invalid crc32c: {e}")))
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(nibble_to_hex(byte >> 4));
        out.push(nibble_to_hex(byte & 0x0f));
    }
    out
}

fn decode_hex(value: &str) -> Result<Vec<u8>, RecordError> {
    let bytes = value.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(RecordError::BadText("payload_hex has odd length".to_string()));
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for chunk in bytes.chunks_exact(2) {
        let high = hex_to_nibble(chunk[0])?;
        let low = hex_to_nibble(chunk[1])?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn nibble_to_hex(value: u8) -> char {
    match value {
        0..=9 => char::from(b'0' + value),
        10..=15 => char::from(b'a' + (value - 10)),
        _ => unreachable!("nibble out of range"),
    }
}

fn hex_to_nibble(value: u8) -> Result<u8, RecordError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(RecordError::BadText(format!(
            "invalid hex digit: {}",
            char::from(value)
        ))),
    }
}

fn read_u16(buf: &[u8], off: &mut usize) -> u16 {
    let v = u16::from_le_bytes([buf[*off], buf[*off + 1]]);
    *off += 2;
    v
}

fn read_u32(buf: &[u8], off: &mut usize) -> u32 {
    let v = u32::from_le_bytes([buf[*off], buf[*off + 1], buf[*off + 2], buf[*off + 3]]);
    *off += 4;
    v
}

fn read_u64(buf: &[u8], off: &mut usize) -> u64 {
    let v = u64::from_le_bytes([
        buf[*off],
        buf[*off + 1],
        buf[*off + 2],
        buf[*off + 3],
        buf[*off + 4],
        buf[*off + 5],
        buf[*off + 6],
        buf[*off + 7],
    ]);
    *off += 8;
    v
}
