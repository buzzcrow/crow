// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Fast-path-or-real-future result for a [`super::KVEngine`] operation that is
/// usually synchronous (in-memory hit / no I/O) but occasionally needs to
/// wait on real I/O (a crowdb-tree demand-load miss, or a write that triggers a
/// flush) once the `io_uring` reactor lands.
///
/// `Ready` costs nothing beyond the enum tag + inline value — no allocation,
/// no `Pin<Box<..>>>` — so a [`super::KVEngine`] that never needs real I/O
/// ([`super::InMemKV`], or [`super::CrowdbTreeEngine`] on every in-memory/
/// resident hit, which is *every* case today since no reactor exists yet)
/// never pays anything for being "async-capable". Only the genuine I/O path
/// boxes a future.
///
/// The shape exists because `async fn` in a trait is not `dyn`-compatible,
/// so a custom future enum is the zero-cost alternative.
#[must_use = "a KVFuture does nothing unless polled/awaited or unwrapped via into_ready()"]
pub enum KVFuture<T> {
    /// `Some` until first polled; `take()`n on completion so polling an
    /// already-completed `Ready` again panics loudly (matches the standard
    /// "polling after Ready" contract violation instead of silently
    /// returning a bogus value).
    Ready(Option<T>),
    Pending(Pin<Box<dyn Future<Output = T> + Send>>),
}

impl<T> KVFuture<T> {
    /// Construct the zero-allocation fast-path variant. Preferred over
    /// spelling `KVFuture::Ready(Some(v))` directly at call sites.
    pub fn ready(v: T) -> Self {
        KVFuture::Ready(Some(v))
    }

    /// Unwrap a `KVFuture` that is known, by construction, to never be
    /// `Pending` — true for every [`super::KVEngine`] call site in this
    /// codebase today, since no impl constructs `Pending` yet (no reactor
    /// exists). Panics on `Pending` so the day a real `Pending` future shows
    /// up here, it's a loud, unmissable signal that this call site now needs
    /// converting to real `async`/`.await` instead of a silent wrong answer
    /// or a hang. The deferred caller-side conversion this panic is meant to trigger.
    ///
    /// # Panics
    /// Panics if `self` is [`KVFuture::Pending`].
    pub fn into_ready(self) -> T {
        match self {
            KVFuture::Ready(v) => v.expect("KVFuture::Ready already taken"),
            KVFuture::Pending(_) => {
                panic!(
                    "KVFuture::into_ready called on a Pending future -- \
                     this call site needs to become async now"
                )
            }
        }
    }
}

impl<T: Unpin> Future for KVFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        match self.get_mut() {
            KVFuture::Ready(v) => Poll::Ready(v.take().expect("KVFuture::Ready polled after completion")),
            KVFuture::Pending(fut) => fut.as_mut().poll(cx),
        }
    }
}
