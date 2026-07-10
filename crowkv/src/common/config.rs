use std::net::SocketAddr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KvConfig {
    pub max_paxos_retries: usize,
    pub max_slot_retries: usize,
    pub retry_base_backoff_ms: u64,
    pub listen_addr: SocketAddr,
}

impl Default for KvConfig {
    fn default() -> Self {
        Self::new(SocketAddr::from(([127, 0, 0, 1], 0)))
    }
}

impl KvConfig {
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self {
            max_paxos_retries: 3,
            max_slot_retries: 3,
            retry_base_backoff_ms: 5,
            listen_addr,
        }
    }
}
