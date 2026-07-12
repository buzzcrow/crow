use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::OnceLock;

static PORT_BASE: OnceLock<u16> = OnceLock::new();
static PORT_NEXT: AtomicU16 = AtomicU16::new(0);

pub fn unique_test_port() -> u16 {
    let base = *PORT_BASE.get_or_init(|| {
        let pid = u64::from(std::process::id());
        let exe_hash = std::env::current_exe().ok().map_or(0, |path| {
            path.to_string_lossy()
                .bytes()
                .fold(0u64, |acc, b| acc.wrapping_mul(131).wrapping_add(u64::from(b)))
        });
        20_000 + ((pid ^ exe_hash) % 20_000) as u16
    });
    base + PORT_NEXT.fetch_add(1, Ordering::Relaxed)
}
