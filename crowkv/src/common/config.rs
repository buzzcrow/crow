/// Paxos retry configuration (static, global).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PaxosConfig {
    pub max_paxos_retries: usize,
    pub max_slot_retries: usize,
    pub retry_base_backoff_ms: u64,
}

impl PaxosConfig {
    pub const DEFAULT: Self = Self {
        max_paxos_retries: 3,
        max_slot_retries: 3,
        retry_base_backoff_ms: 5,
    };
}
