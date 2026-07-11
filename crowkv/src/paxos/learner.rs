use dashmap::DashMap;

use crate::paxos::roles::{Learner, PxLogEntry};

/// In-memory state-machine backed by a `DashMap`.
///
/// `learn` is called once a log entry has been chosen (i.e. accepted by a
/// quorum).  The payload is the minimal binary format emitted by
/// `PxReplica::encode_kv_payload`.
pub struct PxLearner {
    store: DashMap<Vec<u8>, Vec<u8>>,
}

impl Default for PxLearner {
    fn default() -> Self {
        Self {
            store: DashMap::new(),
        }
    }
}

impl PxLearner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn store(&self) -> &DashMap<Vec<u8>, Vec<u8>> {
        &self.store
    }

    /// Decode `payload` and apply each operation to the store.
    ///
    /// Wire format (per `PxReplica::encode_kv_payload`):
    ///   [op_count: u8]
    ///   for each op:
    ///     [kind: u8]  0=Put, 1=Delete
    ///     [key_len: u32 LE]
    ///     [key bytes]
    ///     [value_len: u32 LE]  (0 for Delete)
    ///     [value bytes]
    fn apply_payload(&self, payload: &[u8]) {
        if payload.is_empty() {
            return;
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
            let key = payload
                .get(offset..offset + key_len)
                .unwrap_or(&[])
                .to_vec();
            offset += key_len;

            let value_len = read_u32_le(payload, offset) as usize;
            offset += 4;
            let value = payload
                .get(offset..offset + value_len)
                .unwrap_or(&[])
                .to_vec();
            offset += value_len;

            if kind == 0 {
                self.store.insert(key, value);
            } else {
                self.store.remove(&key);
            }
        }
    }
}

fn read_u32_le(buf: &[u8], offset: usize) -> u32 {
    let b = buf.get(offset..offset + 4).unwrap_or(&[0; 4]);
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

impl Learner for PxLearner {
    fn learn(&self, entry: PxLogEntry) {
        self.apply_payload(&entry.payload);
    }
}
