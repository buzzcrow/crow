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

use crate::paxos::roles::{PxBallot, PxLogEntry, PxLogEntryKind, SlotIndex};
use crate::paxos::{PxGroupId, PxTerm};

/// Magic number: ASCII "CROW" in little-endian.
pub const WAL_MAGIC: u32 = 0x4352_4F57;

/// Current frozen record version.
pub const WAL_VERSION: u16 = 1;

/// Size of the header body (magic through `payload_len`), excluding `frame_len` prefix.
pub const HEADER_BODY_LEN: usize = 4 + 2 + 1 + 8 + 8 + 8 + 8 + 8 + 4; // 51

/// Minimum encoded record size: `frame_len(4)` + `header_body(51)` + crc(4) = 59.
pub const MIN_RECORD_SIZE: usize = 4 + HEADER_BODY_LEN + 4;

/// Discriminant for [`RecordType`]. Stable; append-only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    Promised = 0,
    Accepted = 1,
    ConfigChange = 2,
    DedupCheckpoint = 3,
    SnapshotMarker = 4,
    VoteGranted = 5,
}

impl RecordType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Promised),
            1 => Some(Self::Accepted),
            2 => Some(Self::ConfigChange),
            3 => Some(Self::DedupCheckpoint),
            4 => Some(Self::SnapshotMarker),
            5 => Some(Self::VoteGranted),
            _ => None,
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

impl WALRecord {
    /// Encode to the on-disk byte layout. Returns the complete framed record.
    ///
    /// # Panics
    /// Panics if `frame_len` or `payload_len` exceeds `u32::MAX`.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let payload_len = self.payload.len();
        let frame_len = HEADER_BODY_LEN + payload_len + 4; // +4 for crc
        let total = 4 + frame_len; // +4 for frame_len prefix
        let mut buf = Vec::with_capacity(total);

        // frame_len
        buf.extend_from_slice(
            &u32::try_from(frame_len)
                .expect("frame_len exceeds u32")
                .to_le_bytes(),
        );
        // --- CRC region starts here ---
        let crc_start = buf.len();
        // magic
        buf.extend_from_slice(&WAL_MAGIC.to_le_bytes());
        // version
        buf.extend_from_slice(&WAL_VERSION.to_le_bytes());
        // record_type
        buf.push(self.record_type as u8);
        // group_id
        buf.extend_from_slice(&self.group_id.to_le_bytes());
        // term
        buf.extend_from_slice(&self.term.to_le_bytes());
        // slot
        buf.extend_from_slice(&self.slot.to_le_bytes());
        // ballot round
        buf.extend_from_slice(&self.ballot.round.to_le_bytes());
        // ballot leader_id
        buf.extend_from_slice(&self.ballot.leader_id.to_le_bytes());
        // payload_len
        buf.extend_from_slice(
            &u32::try_from(payload_len)
                .expect("payload_len exceeds u32")
                .to_le_bytes(),
        );
        // payload
        buf.extend_from_slice(&self.payload);
        // --- CRC region ends here ---
        let crc = crc32c::crc32c(&buf[crc_start..]);
        buf.extend_from_slice(&crc.to_le_bytes());

        debug_assert_eq!(buf.len(), total);
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
        let crc_computed = crc32c::crc32c(&body[..frame_len - 4]);
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

    // ── Accepted payload helpers ───────────────────────────

    /// Encode a [`PxLogEntry`] into a `WALRecord` with `RecordType::Accepted`.
    #[must_use]
    pub fn from_accepted(group_id: PxGroupId, entry: &PxLogEntry) -> Self {
        let payload = encode_accepted_payload(entry);
        Self {
            record_type: RecordType::Accepted,
            group_id,
            term: entry.term,
            slot: entry.slot,
            ballot: entry.ballot,
            payload: Bytes::from(payload),
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
    /// fields (kind, `client_id`, seq, inner payload).
    #[must_use]
    pub fn to_log_entry(&self) -> Option<PxLogEntry> {
        if self.record_type != RecordType::Accepted {
            return None;
        }
        decode_accepted_payload(self)
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
/// [kind      : u8    ]  — PxLogEntryKind discriminant
/// [client_id : u64 LE]  — 0 = None
/// [seq       : u64 LE]  — meaningful only when client_id != 0
/// [inner     : rest  ]  — PxLogEntry.payload bytes
/// ```
fn encode_accepted_payload(entry: &PxLogEntry) -> Vec<u8> {
    let kind_byte = match entry.kind {
        PxLogEntryKind::Write => 0u8,
        PxLogEntryKind::NoOp => 1,
        PxLogEntryKind::ConfigChange => 2,
        PxLogEntryKind::DedupCheckpoint => 3,
    };
    let client_id = entry.client_id.unwrap_or(0);
    let seq = entry.seq.unwrap_or(0);
    let mut buf = Vec::with_capacity(1 + 8 + 8 + entry.payload.len());
    buf.push(kind_byte);
    buf.extend_from_slice(&client_id.to_le_bytes());
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.extend_from_slice(&entry.payload);
    buf
}

fn decode_accepted_payload(rec: &WALRecord) -> Option<PxLogEntry> {
    let p = &rec.payload;
    if p.len() < 17 {
        return None;
    }
    let kind = match p[0] {
        0 => PxLogEntryKind::Write,
        1 => PxLogEntryKind::NoOp,
        2 => PxLogEntryKind::ConfigChange,
        3 => PxLogEntryKind::DedupCheckpoint,
        _ => return None,
    };
    let client_id_raw = u64::from_le_bytes(p[1..9].try_into().ok()?);
    let seq_raw = u64::from_le_bytes(p[9..17].try_into().ok()?);
    let client_id = if client_id_raw == 0 {
        None
    } else {
        Some(client_id_raw)
    };
    let seq = if client_id.is_none() { None } else { Some(seq_raw) };
    let inner_payload = Bytes::copy_from_slice(&p[17..]);

    Some(PxLogEntry {
        slot: rec.slot,
        ballot: rec.ballot,
        term: rec.term,
        kind,
        payload: inner_payload,
        client_id,
        seq,
    })
}

// ── Primitive read helpers ──────────────────────────────────

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
