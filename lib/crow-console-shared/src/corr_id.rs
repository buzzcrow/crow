// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Correlation-id propagation for console-issued operations.
//!
//! Every console request (CLI invocation, web handler call) runs inside
//! a `tokio` task-local that carries an opaque 16-hex-char correlation
//! id. Outbound HTTP/gRPC clients attach this id as the
//! `x-crow-kv-corr-id` header so a single user action shows up linked
//! across console + every upstream `crow-kv-server` it touched.
//!
//! Key work: task-local storage with `scope`/`scope_in`, generators
//! (`new`, `current_or_new`), and the canonical header constant.
//!
//! Format: 16 hex characters, derived from the system clock + pid +
//! a monotonic counter. Good enough for log correlation; not
//! cryptographically unique.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// HTTP header name carrying the correlation id between console
/// components and upstream `crow-kv-server`. Lowercase per RFC 7230;
/// `crow-kv-server` will recognise it once the tracing middleware lands.
pub const HEADER: &str = "x-crow-kv-corr-id";

tokio::task_local! {
    /// Per-task correlation id. Reading via `try_with` returns `None`
    /// outside a `scope` block.
    static CORR_ID: String;
}

/// Borrow the current task-local correlation id, if any.
#[must_use]
pub fn current() -> Option<String> {
    CORR_ID.try_with(Clone::clone).ok()
}

/// Return the current correlation id, generating a fresh one if no
/// task-local is set. Useful for libraries that may be called from
/// either a console handler (corr-id set) or a unit test (corr-id
/// unset).
#[must_use]
pub fn current_or_new() -> String {
    current().unwrap_or_else(generate)
}

/// Generate a fresh correlation id. 16 lowercase hex characters.
///
/// Layout: `tttttttt-pppp-ssss`, concatenated:
///   - `tttttttt` low 32 bits of `Instant`-since-epoch nanoseconds,
///   - `pppp` low 16 bits of the pid,
///   - `ssss` low 16 bits of a monotonic counter.
///
/// This is not a UUID and is not collision-resistant across crashes;
/// it is "good enough" to link a single user action across logs.
#[must_use]
pub fn generate() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    )
    .unwrap_or(u64::MAX);
    let pid = u64::from(std::process::id());
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let t = (nanos & 0xFFFF_FFFF) as u32;
    let p = (pid & 0xFFFF) as u16;
    let s = (seq & 0xFFFF) as u16;
    format!("{t:08x}{p:04x}{s:04x}")
}

/// Run `fut` with `corr_id` bound as the current task-local id. Nested
/// `scope` calls shadow the parent id for the duration of the future.
pub async fn scope<F, T>(corr_id: String, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    CORR_ID.scope(corr_id, fut).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn current_returns_none_outside_scope() {
        assert!(current().is_none());
    }

    #[tokio::test]
    async fn scope_sets_and_clears_corr_id() {
        let id = generate();
        let got = scope(id.clone(), async { current().unwrap() }).await;
        assert_eq!(got, id);
        assert!(current().is_none(), "corr-id should not leak past scope");
    }

    #[test]
    fn generated_id_is_16_lowercase_hex_chars() {
        let id = generate();
        assert_eq!(id.len(), 16, "expected 16 chars, got {id:?}");
        assert!(
            id.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "non-lowercase-hex in {id:?}"
        );
    }

    #[test]
    fn distinct_ids_within_one_call_stack() {
        let a = generate();
        let b = generate();
        assert_ne!(a, b, "sequence counter should advance");
    }
}
