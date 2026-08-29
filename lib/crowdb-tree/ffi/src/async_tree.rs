// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::error::CtError;
use crate::reactor::{
    drive_ct_future, try_poll_ct_future, try_poll_ct_future_pinned, FutureGuard, FutureKind, PinnedValue,
};
use crate::scan::{decode_scan, ScanEntry};
use crate::sys;
use crate::tree::Crowdbtree;
use crate::Options;

/// Async facade. `get`/`flush`/`snapshot`/`scan` drive the engine's io_uring
/// reactor directly via [`drive_ct_future`] -- no thread pool hop. The
/// remaining methods have no async C API twin yet and are called via the
/// synchronous `Crowdbtree` handle (`handle()`). Cheap to clone (shares
/// one `Arc<Crowdbtree>`).
#[derive(Clone, Debug)]
pub struct AsyncCrowdbtree {
    inner: Arc<Crowdbtree>,
}

impl AsyncCrowdbtree {
    pub fn open(opt: &Options) -> Result<Self, CtError> {
        Ok(Self {
            inner: Arc::new(Crowdbtree::open(opt)?),
        })
    }

    pub fn from_sync(tree: Crowdbtree) -> Self {
        Self {
            inner: Arc::new(tree),
        }
    }

    pub fn handle(&self) -> Arc<Crowdbtree> {
        Arc::clone(&self.inner)
    }

    /// Drives the engine's io_uring reactor directly (Phase
    /// 3) -- no `spawn_blocking`, since flush never touches the page
    /// store (only the in-memory L1), this always resolves on the very
    /// first poll.
    pub async fn flush(&self) -> Result<(), CtError> {
        let fut = unsafe { sys::ct_flush_async(self.inner.as_ptr()) };
        drive_ct_future(FutureGuard(fut), &self.inner, FutureKind::Flush).await?;
        Ok(())
    }

    /// Drives the engine's io_uring reactor directly (Phase
    /// 3) -- no `spawn_blocking`; the write phase always waits on the
    /// reactor, unlike `flush`/the fast `get` path.
    pub async fn snapshot(&self) -> Result<u64, CtError> {
        let fut = unsafe { sys::ct_snapshot_async(self.inner.as_ptr()) };
        Ok(
            drive_ct_future(FutureGuard(fut), &self.inner, FutureKind::Snapshot)
                .await?
                .slot,
        )
    }

    /// Drives the engine's io_uring reactor directly (Phase
    /// 3) -- no `spawn_blocking`. Resolves on the very first poll for a
    /// resident hit (`get_view`'s existing fast path, `#5 B3`); only a
    /// genuine demand-load miss waits on the reactor.
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<(u64, Vec<u8>)>, CtError> {
        let fut = unsafe { sys::ct_get_async(self.inner.as_ptr(), key.as_ptr(), key.len()) };
        let out = drive_ct_future(FutureGuard(fut), &self.inner, FutureKind::Get).await?;
        Ok(out.found.then_some((out.slot, out.value)))
    }

    /// Like [`Self::get`], but never allocates or boxes anything for the
    /// fast (resident hit/miss) path -- only the genuine demand-load-miss
    /// path (`GetOutcome::Pending`) does. Lets a caller with its own
    /// fast-path/slow-path return type (`crowdb_kv`'s `KVFuture`,     /// #11 Phase 6) mirror `ct_get_async`'s own C++-layer split one layer
    /// up, instead of collapsing it back into a uniform `async fn` (which
    /// would force boxing on every call, fast path included -- exactly what
    /// `KVFuture::Ready` exists to avoid).
    pub fn try_get(&self, key: &[u8]) -> GetOutcome {
        let fut = unsafe { sys::ct_get_async(self.inner.as_ptr(), key.as_ptr(), key.len()) };
        let mut guard = FutureGuard(fut);
        if let Some(result) = try_poll_ct_future(&mut guard, FutureKind::Get) {
            return GetOutcome::Ready(result.map(|out| out.found.then_some((out.slot, out.value))));
        }
        let tree = self.inner.clone();
        GetOutcome::Pending(Box::pin(async move {
            let out = drive_ct_future(guard, &tree, FutureKind::Get).await?;
            Ok(out.found.then_some((out.slot, out.value)))
        }))
    }

    /// Like [`Self::try_get`] but the fast path returns a [`PinnedValue`]
    /// borrowing directly from the C++ engine's internal buffer (no
    /// `copy_buf` allocation). The slow path is identical to
    /// [`Self::try_get`]'s (`PinnedGetOutcome::Pending` resolves to an
    /// owned `Vec<u8>`).
    pub fn try_get_pinned(&self, key: &[u8]) -> PinnedGetOutcome {
        let fut = unsafe { sys::ct_get_async(self.inner.as_ptr(), key.as_ptr(), key.len()) };
        let mut guard = FutureGuard(fut);
        if let Some(result) = try_poll_ct_future_pinned(&mut guard) {
            return PinnedGetOutcome::Ready(result);
        }
        let tree = self.inner.clone();
        PinnedGetOutcome::Pending(Box::pin(async move {
            let out = drive_ct_future(guard, &tree, FutureKind::Get).await?;
            Ok(out.found.then_some((out.slot, out.value)))
        }))
    }

    /// Async twin of [`Crowdbtree::scan`] (follow-up). Drives
    /// the reactor directly like `get`/`flush`/`snapshot` -- resolves on the
    /// first poll whenever every leaf in range is already resident
    /// (`scan`'s own fast path), only waiting on the reactor for a
    /// genuine cold leaf (or the initial root->leaf descent). See
    /// `Crowdbtree::scan_async`'s doc comment (crowdb-tree.h) for why a miss
    /// retries the whole scan rather than resuming a cursor.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan(
        &self,
        prefix: Vec<u8>,
        start_after: Vec<u8>,
        end_key: Vec<u8>,
        limit: usize,
        byte_budget: usize,
        keys_only: bool,
        deadline_ms: u64,
    ) -> Result<(Vec<ScanEntry>, bool), CtError> {
        let fut = unsafe {
            sys::ct_scan_async(
                self.inner.as_ptr(),
                prefix.as_ptr(),
                prefix.len(),
                start_after.as_ptr(),
                start_after.len(),
                end_key.as_ptr(),
                end_key.len(),
                limit,
                byte_budget,
                if keys_only { 1 } else { 0 },
                deadline_ms,
            )
        };
        let out = drive_ct_future(FutureGuard(fut), &self.inner, FutureKind::Scan).await?;
        let entries = decode_scan(out.value, out.slot as usize)?;
        Ok((entries, out.found))
    }

    /// Like [`Self::try_get`], but for [`Self::scan`]: never allocates or
    /// boxes anything for the fast (all-leaves-resident) path -- only a
    /// genuine cold-leaf miss (`ScanOutcome::Pending`) does. Same
    /// motivation as `try_get`'s doc comment: lets a caller with its own
    /// fast-path/slow-path return type mirror `ct_scan_async`'s own
    /// C++-layer split one layer up instead of forcing a box on every call.
    #[allow(clippy::too_many_arguments)]
    pub fn try_scan(
        &self,
        prefix: Vec<u8>,
        start_after: Vec<u8>,
        end_key: Vec<u8>,
        limit: usize,
        byte_budget: usize,
        keys_only: bool,
        deadline_ms: u64,
    ) -> ScanOutcome {
        let fut = unsafe {
            sys::ct_scan_async(
                self.inner.as_ptr(),
                prefix.as_ptr(),
                prefix.len(),
                start_after.as_ptr(),
                start_after.len(),
                end_key.as_ptr(),
                end_key.len(),
                limit,
                byte_budget,
                if keys_only { 1 } else { 0 },
                deadline_ms,
            )
        };
        let mut guard = FutureGuard(fut);
        if let Some(result) = try_poll_ct_future(&mut guard, FutureKind::Scan) {
            return ScanOutcome::Ready(result.and_then(|out| {
                let entries = decode_scan(out.value, out.slot as usize)?;
                Ok((entries, out.found))
            }));
        }
        let tree = self.inner.clone();
        ScanOutcome::Pending(Box::pin(async move {
            let out = drive_ct_future(guard, &tree, FutureKind::Scan).await?;
            let entries = decode_scan(out.value, out.slot as usize)?;
            Ok((entries, out.found))
        }))
    }
}

/// Result of [`AsyncCrowdbtree::try_get`] -- see its doc comment for why this
/// exists instead of a single uniform `async fn`.
#[allow(clippy::type_complexity)]
pub enum GetOutcome {
    /// Resolved on the very first (and only) poll attempt -- no allocation.
    Ready(Result<Option<(u64, Vec<u8>)>, CtError>),
    /// A genuine demand-load miss, already registered with the reactor
    /// (or, absent one, that will complete synchronously on the next poll
    /// regardless -- `.await` this to completion.
    Pending(Pin<Box<dyn Future<Output = Result<Option<(u64, Vec<u8>)>, CtError>> + Send>>),
}

/// Result of [`AsyncCrowdbtree::try_scan`] -- see its doc comment for why
/// this exists instead of a single uniform `async fn`.
#[allow(clippy::type_complexity)]
pub enum ScanOutcome {
    /// Resolved on the very first (and only) poll attempt -- no allocation
    /// beyond the returned entries themselves.
    Ready(Result<(Vec<ScanEntry>, bool), CtError>),
    /// A genuine cold-leaf miss, already registered with the reactor (or,
    /// absent one, that will complete synchronously on the next poll
    /// regardless -- `.await` this to completion.
    Pending(Pin<Box<dyn Future<Output = Result<(Vec<ScanEntry>, bool), CtError>> + Send>>),
}

/// Result of [`AsyncCrowdbtree::try_get_pinned`] -- like [`GetOutcome`] but
/// the fast path returns a [`PinnedValue`] (zero-copy borrow from the C++
/// frame) instead of an owned `Vec<u8>`. The slow path is identical to
/// [`GetOutcome::Pending`]: the value is always owned (copied by
/// `materialize_owned` on the reactor thread).
#[allow(clippy::type_complexity)]
pub enum PinnedGetOutcome {
    /// Fast path (resident hit/miss) -- zero-copy borrow, no `copy_buf`.
    Ready(Result<Option<(u64, PinnedValue)>, CtError>),
    /// Slow path (demand-load miss) -- resolves to an owned `Vec<u8>`,
    /// same as [`GetOutcome::Pending`].
    Pending(Pin<Box<dyn Future<Output = Result<Option<(u64, Vec<u8>)>, CtError>> + Send>>),
}
