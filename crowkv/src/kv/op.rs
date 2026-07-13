// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

/// A single mutation within a batch: set a value or delete (tombstone) a key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Put(Vec<u8>),
    Delete,
}

/// One `(key, op)` mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchOp {
    pub key: Vec<u8>,
    pub op: Op,
}

/// An ordered list of mutations that all share one slot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Batch {
    pub ops: Vec<BatchOp>,
}

/// Per-key single-version cell: a live value or a tombstone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cell {
    Value(Vec<u8>),
    Tombstone,
}

/// A logical difference between two engines at one key. `None` means the key
/// is absent on that side.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineDiff {
    pub key: Vec<u8>,
    pub left: Option<(u64, Cell)>,
    pub right: Option<(u64, Cell)>,
}

impl Batch {
    /// Decode the minimal KV payload emitted by
    /// `PxKvStore::encode_kv_payload` / `encode_kv_batch_items`.
    ///
    /// Wire format:
    ///   [`op_count`: u8]
    ///   for each op:
    ///     [`kind`: u8]  0 = Put, 1 = Delete
    ///     [`key_len`: u32 LE][key bytes]
    ///     [`value_len`: u32 LE][value bytes]  (`value_len` = 0 for Delete)
    ///
    /// An empty payload (e.g. a `NoOp` repair entry) decodes to an empty
    /// batch. Malformed/truncated input stops decoding at the bad record.
    #[must_use]
    pub fn decode(payload: &[u8]) -> Self {
        let mut ops = Vec::new();
        if payload.is_empty() {
            return Self { ops };
        }
        let op_count = payload[0] as usize;
        let mut offset = 1usize;
        for _ in 0..op_count {
            if offset >= payload.len() {
                break;
            }
            let kind = payload[offset];
            offset += 1;

            let key_len = read_u32_le(payload, offset) as usize;
            offset += 4;
            let key = payload.get(offset..offset + key_len).unwrap_or(&[]).to_vec();
            offset += key_len;

            let value_len = read_u32_le(payload, offset) as usize;
            offset += 4;
            let value = payload.get(offset..offset + value_len).unwrap_or(&[]).to_vec();
            offset += value_len;

            let op = if kind == 0 { Op::Put(value) } else { Op::Delete };
            ops.push(BatchOp { key, op });
        }
        Self { ops }
    }
}

fn read_u32_le(buf: &[u8], offset: usize) -> u32 {
    let b = buf.get(offset..offset + 4).unwrap_or(&[0; 4]);
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}
