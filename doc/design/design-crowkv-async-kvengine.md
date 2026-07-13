# CrowKV — Async `KVEngine` / `PxLearner` / gRPC Consumer Plan

> **Status:** design/plan (not yet implemented).
> **Parent:** [`design-crowtree-async.md`](design-crowtree-async.md) (the C++/FFI
> reactor this plan consumes). See `plan-tree.md` #11 for the tracker entry.

## 1. Why this doc exists

`design-crowtree-async.md` designs a real io_uring-backed async I/O path
(`Reactor`, `ct_future`, Rust `Future` impls replacing `AsyncCrowtree`'s
`spawn_blocking`). **Found while scoping #11 (2026-07-08): that work has no
consumer today.** `crowkv`'s actual `KVEngine` impl
(`crowkv/src/kv/crowtree_engine.rs::CrowtreeEngine`) deliberately does not use
`AsyncCrowtree` — its own doc comment explains why: `KVEngine` is a
**synchronous** trait, and `PxLearner` calls it synchronously from inside
already-async gRPC handlers (`PxKvStore::kv_get`/`kv_scan` call
`engine_get`/`engine_scan` inline, not `.await`ed).

This means, **today**, a `CrowtreeEngine`-backed group with a real durable
file that suffers an L1 cache miss on `get()` blocks the calling Tokio worker
thread on a synchronous `pread` — the exact anti-pattern #11 exists to fix,
already live and unrelated to whether the reactor ever gets built. Finishing
`design-crowtree-async.md`'s 5 phases without *also* doing the work below
would leave that blocking-`pread` problem completely unfixed in practice.

This doc plans that consumer-side work: making `KVEngine`, `PxLearner`, and
the gRPC read/write paths async so they can actually call into the reactor
once it exists.

## 2. Current state (traced 2026-07-08)

- `KVEngine` trait (`crowkv/src/kv/kv_engine.rs`): every method (`get`,
  `scan`, `apply`, `iter_all`, `live_key_count`, `clear`, `compare`,
  `resume_from_slot`, `persist_snapshot`, `set_gc_watermark`,
  `collect_garbage`) is a plain synchronous `fn`.
- `PxLearner::engine` is `Box<dyn KVEngine>` — **a trait object**, chosen at
  runtime (`--kv-engine {memory,crowtree}` CLI flag). This is the crux of the
  design problem below.
- `Learner::learn()` (`crowkv/src/paxos/roles.rs`) is a plain synchronous
  `fn`, implemented by `PxLearner`. **Never used as `dyn Learner`** — always
  a concrete `Arc<PxLearner>` — confirmed by grep across the crate.
- `Acceptor` (same file) **already uses native `async fn` in a trait**
  (`async fn accept`, `async fn prepare`, `#[allow(async_fn_in_trait)]`) —
  also never used as `dyn Acceptor`. This is the established, working
  convention to extend to `Learner`.
- `crowkv`'s `rust-version = "1.75"` (workspace `Cargo.toml`) — exactly where
  native async-fn-in-traits stabilized. `async-trait` is **not** currently a
  workspace dependency anywhere.
- The call sites that would need to await a now-async `learn()` **are
  already async and already structured as a thin wrapper around it**:
  `PxLocalReplica::learn_chosen` and `PxLocalReplica::apply_committed_up_to`
  are already `async fn` (currently `#[allow(clippy::unused_async)]`,
  because they don't yet await anything), and every one of their call sites
  (`group.rs`'s accept/repair paths, `group_election.rs`'s bulk Phase 1,
  `local_replica.rs`'s heartbeat handler) **already `.await`s them**.
  `PxKvStore::kv_get`/`kv_scan` are already `async fn` gRPC handlers that
  call `engine_get`/`engine_scan` inline. **This means the ripple is much
  smaller than "thread async through the whole call graph"** — it's
  "implement real async under boundaries someone already put in the right
  place."
- Existing tests: several call `learner.learn(...)` directly from plain
  `#[test]` functions (`learner_dedup_test.rs`, `group/safe_slot_test.rs`,
  `group/proposer_test.rs`, `replica/election_test.rs`,
  `election/role_test.rs`, `group/maintenance_test.rs`, and likely a few
  more) — these need to become `#[tokio::test]` once `learn()` is async. Pure
  mechanical churn, several files in the same test module already use
  `#[tokio::test]` for other tests, so the pattern is already established.

## 3. The central design tension: `dyn KVEngine` vs. `async fn` in traits

Native `async fn` in traits (used for `Acceptor` today) is **not
dyn-compatible** — you cannot have `Box<dyn KVEngine>` if `KVEngine` has
`async fn` methods, without either:

- **(a) `async-trait` crate** — boxes every async call into a
  `Pin<Box<dyn Future<Output = T> + Send>>>` via macro, one heap allocation
  **per call, including the fast (in-memory, no I/O) path**. This directly
  contradicts `design-crowtree-async.md §2`'s stated goal ("Fast path...
  completes synchronously... zero scheduling overhead") — the exact thing
  #11 is trying to achieve at the C++/FFI layer would be undone again at the
  very next layer up.
- **(b) Generic `PxLearner<E: KVEngine>`** — compile-time monomorphization,
  zero overhead, but `E` would need to propagate through `PxLocalReplica`,
  `PxGroup`, `PxKvStore`, and everywhere a group is stored generically
  (`DashMap<GroupId, PxGroup>` in `PxKvStore`) — the engine is chosen at
  **runtime** via a CLI flag today, so this would require either an enum
  wrapper at the storage layer anyway (defeating the purpose) or duplicating
  the entire store type per engine kind. Too invasive for the actual
  runtime-selection requirement.
- **(c) Hybrid fast-path/slow-path return type (recommended below)** —
  keeps `KVEngine::get`/`scan`/`apply` as plain (non-`async`) `fn`s returning
  a small custom future enum that resolves *immediately* (no allocation) for
  the fast path and only boxes a real future for the rare slow (I/O) path.
  Fully `dyn`-compatible (the trait itself has no `async fn`), zero new
  dependency, and mirrors the *exact* fast/slow split
  `design-crowtree-async.md §4`'s table already specifies at the C++ layer —
  this is that same split, faithfully carried through the Rust trait
  boundary instead of being lost at it.

**Recommendation: (c).**

## 4. Proposed `KVEngine` trait shape

```rust
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Fast-path-or-real-future result for an operation that is usually
/// synchronous (in-memory hit / no I/O) but occasionally needs to wait on
/// real I/O (crowtree demand-load miss, or a write that triggers a flush).
///
/// `Ready` costs nothing beyond the enum tag + inline value -- no
/// allocation, no `Pin<Box<..>>>` -- so a `KVEngine` that never needs real
/// I/O (`InMemKV`, or `CrowtreeEngine` on every in-memory/resident hit)
/// never pays anything for being "async-capable". Only the genuine I/O path
/// boxes a future.
pub enum KVFuture<T> {
    // `Some` until first polled; `take()`n on completion so polling an
    // already-completed `Ready` again panics loudly (matches the standard
    // "polling after Ready" contract violation instead of silently
    // returning a bogus value).
    Ready(Option<T>),
    Pending(Pin<Box<dyn Future<Output = T> + Send>>),
}

impl<T> Future for KVFuture<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        match self.get_mut() {
            KVFuture::Ready(v) => {
                Poll::Ready(v.take().expect("KVFuture::Ready polled after completion"))
            }
            KVFuture::Pending(fut) => fut.as_mut().poll(cx),
        }
    }
}

impl<T> KVFuture<T> {
    /// Construct the zero-allocation fast-path variant. Preferred over
    /// spelling `KVFuture::Ready(Some(v))` directly at call sites.
    pub fn ready(v: T) -> Self {
        KVFuture::Ready(Some(v))
    }
}

pub trait KVEngine: Send + Sync {
    /// Apply `batch` at `slot`. `Ready(())` for the common case (MemTable
    /// insert only); `Pending` only when this apply triggers a flush that
    /// must wait on real durable I/O.
    fn apply(&self, slot: u64, batch: &Batch) -> KVFuture<()>;

    /// `Ready(..)` for an L0 hit or an L1-resident-page hit (today's *only*
    /// case for every engine); `Pending` only for a genuine demand-load miss.
    fn get(&self, key: &[u8]) -> KVFuture<Option<(u64, Vec<u8>)>>;

    #[allow(clippy::type_complexity)]
    fn scan(&self, prefix: &[u8], limit: usize) -> KVFuture<(Vec<(Vec<u8>, u64, Vec<u8>)>, bool)>;

    // iter_all / live_key_count / clear / compare / resume_from_slot /
    // persist_snapshot / set_gc_watermark / collect_garbage: UNCHANGED,
    // stay plain sync fns. They're diagnostic/maintenance-path only
    // (compare/iter_all: tests + snapshot export; resume_from_slot/
    // persist_snapshot/set_gc_watermark/collect_garbage: restore path +
    // `cluster::group_maintenance`'s already-tolerant periodic task) --
    // never on the hot Paxos accept / gRPC read path, so a brief blocking
    // call there is an acceptable, unchanged trade-off (matches
    // `group_maintenance`'s existing design, which already does this).
}
```

`InMemKV::get/scan/apply` simply wrap their existing body in
`KVFuture::Ready(...)` — no behavior change, no allocation, verified by a
UT that the returned future is *always* the `Ready` variant (see §6).

`CrowtreeEngine::get/scan/apply` do the fast-path check they do **today**
first; if it resolves without a demand-load miss (the common case, and the
*only* case today, since `CrowtreeEngine` has no async I/O yet), return
`KVFuture::Ready(...)`. Once `design-crowtree-async.md`'s reactor exists,
the miss path constructs `KVFuture::Pending(Box::pin(async move { ... }))`
wrapping a `crowtree_ffi::CtGetFuture`-equivalent (see that doc §3.4) awaited
inside the boxed async block.

## 5. Caller-side changes — revised after re-scoping (2026-07-09)

**Re-scoped while implementing:** the original estimate ("several" test
files call `learner.learn(...)`/`engine_get`/`engine_scan` directly) badly
undercounted the real ripple. A fresh grep found **75+ `engine_get`/
`engine_scan` call sites across 13 test files**
(`replica/persistence_test.rs`, `replica/op_correctness_test.rs`,
`wal/replay_tests.rs`, `replica/snapshot_test.rs`, `store/node_test.rs`,
`replica/replay_ordering_test.rs`, `store/multi_group_test.rs`, and more),
plus `learner.learn(...)` direct calls in `group/proposer_test.rs`,
`group/safe_slot_test.rs`, `learner_dedup_test.rs`,
`replica/election_test.rs`, `election/role_test.rs`,
`group/maintenance_test.rs`.

Converting all of that to `async`/`.await` **now** would be pure churn: no
`KVFuture::Pending` can be constructed until `design-crowtree-async.md`'s
reactor exists (Phase 6 of that doc) — until then, every `KVEngine` impl
only ever produces `Ready`. Threading `async fn`/`.await` through 75+ call
sites today for a case that is structurally impossible to hit yet is
exactly the over-engineering this project's own discipline warns against.

**Revised plan: land the trait-shape change now, defer the caller-side
`async`/`.await` ripple to whenever `CrowtreeEngine` actually starts
constructing `Pending` (this doc's original Phase-6-adjacent trigger).**

1. `KVEngine::apply/get/scan` return `KVFuture<T>` (as designed in §4) —
   this part lands now.
2. `KVFuture<T>` gets an `into_ready()` unwrap helper:
   ```rust
   impl<T> KVFuture<T> {
       pub fn ready(v: T) -> Self { KVFuture::Ready(Some(v)) }

       /// Unwrap a `KVFuture` that is known, by construction, to never be
       /// `Pending` -- true for every call site in this codebase today,
       /// since no `KVEngine` impl constructs `Pending` yet (no reactor
       /// exists). Panics on `Pending` so the day a real `Pending` future
       /// shows up here, it's a loud, unmissable signal that this call
       /// site now needs converting to real `async`/`.await` instead of a
       /// silent wrong-answer or a hang.
       pub fn into_ready(self) -> T {
           match self {
               KVFuture::Ready(v) => v.expect("KVFuture::Ready already taken"),
               KVFuture::Pending(_) => {
                   panic!("KVFuture::into_ready called on a Pending future -- \
                           this call site needs to become async now")
               }
           }
       }
   }
   ```
3. `PxLearner::apply_entry`/`engine_get`/`engine_scan` and `Learner::learn`
   **stay fully synchronous** — call `.into_ready()` on the `KVFuture`
   internally. **Zero signature change**, so all 75+ existing call sites
   across the test suite and `PxKvStore::kv_get`/`kv_scan` need **no
   changes at all**.
4. The handful of call sites that invoke `KVEngine::apply/get/scan`
   **directly** (bypassing `PxLearner`) do need a one-line `.into_ready()`
   added: `crowkv/tests/kv/conformance.rs`, `crowkv/tests/kv/
   crowtree_engine_test.rs`, `crowkv/tests/kv/mem_kv_test.rs`,
   `crowkv/tests/wal/replay_tests.rs`'s three direct `engine.apply(...)`
   calls. This is the entire real ripple for landing §4's trait-shape
   change today.
5. **Trigger for the deferred work:** the day `CrowtreeEngine::get/scan/
   apply` starts constructing a real `KVFuture::Pending` (this doc's
   original Phase-6 trigger, once `design-crowtree-async.md`'s reactor
   exists), every `.into_ready()` call in `PxLearner` becomes a compile-time-safe
   place to swap in real `.await` — and *only* `PxLearner`'s three methods
   plus `Learner::learn` need to change; the 75+ test call sites and
   `PxKvStore` still don't, because `PxLearner`'s own methods can become
   `async fn` (or, if avoiding *that* ripple is still desired at that time,
   `PxLearner` could instead run the genuinely-pending future to completion
   with a bounded synchronous wait — a decision to make with full
   information once there's a real `Pending` case to look at, not before).

The rest of this section (§5 original numbering below) is kept for
reference as the **eventual** full-async caller plan, once deferred work
above is triggered — not something to implement today.

## 5a. Original (deferred) caller-side changes

1. `Learner::learn` becomes `async fn` (native, matching `Acceptor`'s
   existing convention — never `dyn`-used, so this is a direct, safe
   change):
   ```rust
   pub trait Learner {
       async fn learn(&self, entry: PxLogEntry, client_id: Option<u64>, seq: Option<u64>);
   }
   ```
2. `PxLearner::apply_entry` becomes `async fn`, `.await`s
   `self.engine.apply(...)`'s `KVFuture`. `PxLearner::learn` (the trait impl)
   becomes `async fn`, awaits `apply_entry`.
3. `PxLearner::engine_get`/`engine_scan` become `async fn`, `.await` the
   `KVFuture` from `self.engine.get`/`scan`.
4. `PxLocalReplica::learn_chosen` / `apply_committed_up_to`: remove
   `#[allow(clippy::unused_async)]`, add `.await` to their internal
   `self.learner.learn(...)` calls. **No signature change** — already
   `async fn`, already awaited by every caller.
5. `PxLocalReplica::restore_from_replay_with_engine`'s Pass 2 loop: add
   `.await` to `replica.learner.learn(entry, None, None)` inside the loop —
   already inside an `async fn`.
6. `PxKvStore::kv_get`/`kv_scan`: add `.await` to
   `group.local_replica().learner.engine_get(key)` /
   `.engine_scan(prefix, limit)` — already inside `async fn` gRPC handlers.
7. **Test migration** (mechanical, low-risk): every plain `#[test]` that
   calls `learner.learn(...)` directly becomes `#[tokio::test]` +
   `async fn` + `.await`. Enumerated in §2; grep
   `learner\.learn\(|\.learn\(entry` across `crowkv/tests/` before starting
   to get the authoritative, current list (this doc's enumeration is a
   snapshot, not a substitute for a fresh grep at implementation time).

## 6. Testing plan — what actually landed (2026-07-09)

- `kv/kv_future_test.rs` (new): `KVFuture::ready(v)` polls to `Poll::Ready(v)`
  on the very first poll without registering a waker; `into_ready()` returns
  the value for `Ready` and panics for `Pending`; a `Pending` variant polls
  through to the wrapped future's own result.
- `InMemKV` regression guard (new test in `mem_kv_test.rs`): every
  `InMemKV::get/scan/apply` call returns the `Ready` variant — asserted via
  a `matches!` check before calling `.into_ready()`, not just the unwrapped
  value, so a future accidental switch to `Pending` fails loudly here first.
- `CrowtreeEngine` regression guard (new test in `crowtree_engine_test.rs`):
  same `matches!`-before-`into_ready()` check — proves the "fast path stays
  fast" property holds today (no reactor exists, so it structurally must).
- **Not added yet (deferred with the rest of §5's original caller-side
  plan):** `learner_async_test.rs`, the `kv_get`/`kv_scan` async regression
  gate. These depend on `PxLearner`/gRPC actually being converted to
  `async`, which is deferred per §5's revised plan — adding them now would
  test code that doesn't exist.

## 7. Sequencing — status

**Landed (2026-07-09):** `KVFuture<T>` type + `KVEngine::apply/get/scan`
trait signature change + `InMemKV`/`CrowtreeEngine` impls (`KVFuture::ready`
only, `Pending` never constructed) + `PxLearner`/`Learner::learn` kept
synchronous via `.into_ready()` + the ~4 direct-call test-file fixups from
§5 item 4 + the regression-guard tests from §6. Zero behavior change,
verified by the full existing test suite passing unmodified everywhere
except the ~4 direct-call sites.

**Landed (2026-07-09, same session as `design-crowtree-async.md` Phase 6):**
§5a's full original caller-side `async`/`.await` plan, once
`CrowtreeEngine::get` started genuinely constructing `KVFuture::Pending`
(via the new `crowtree_ffi::AsyncCrowtree::try_get` -> `GetOutcome`, itself
built on Phase 3's reactor-driven futures + Phase 4's zero-copy fast path).
Chose **full async conversion** over the bounded-synchronous-wait
alternative §5's revised plan had also left open, now that there was a
real `Pending` case to decide with in hand:

- `Learner::learn` -> native `async fn` (matching `Acceptor`'s existing
  convention; never `dyn`-used, so this was a direct, safe change).
- `PxLearner::apply_entry`/`engine_get`/`engine_scan` -> `async fn`,
  `.await` the `KVFuture` directly instead of `.into_ready()`.
  `engine_get` is the one that can now genuinely suspend; `engine_scan`/
  `apply_entry` stay `async fn` for signature uniformity but never actually
  suspend (`KVEngine::scan`/`apply` still have no async C API of their own
  -- see `CrowtreeEngine`'s doc comment for why that's an honest, current
  gap rather than an oversight).
- `PxLocalReplica::learn_chosen`/`apply_committed_up_to`: dropped
  `#[allow(clippy::unused_async)]`, added `.await`. No signature change --
  already `async fn`, already awaited by every caller.
- `PxLocalReplica::restore_from_replay_with_engine`'s Pass 2 loop: added
  `.await` to `replica.learner.learn(...)`.
- `PxKvStore::kv_get`/`kv_scan`: added `.await` to
  `learner.engine_get`/`engine_scan`.
- Test migration: a fresh grep (not the doc's original snapshot list, per
  its own advice) found direct `learner.learn(...)`/`.engine_get(...)`/
  `.engine_scan(...)` call sites across 15 files (13 `crowkv` integration
  test files + `crowkv-server/tests/startup_test.rs`); every plain `#[test]`
  among them became `#[tokio::test]` + `async fn` + `.await`, every
  already-`async fn` site just gained `.await`.
- New regression tests: `kv/crowtree_engine_test.rs`'s
  `get_constructs_pending_for_genuine_demand_load_miss` (engine layer --
  evict a durable engine's resident leaf, assert `KVEngine::get` returns
  `KVFuture::Pending` and it resolves to the right value) and
  `paxos/learner_async_test.rs` (this doc's §6-deferred `learner_async_test.rs`,
  the same property one layer up through `PxLearner::engine_get`).
- Verification: full `cargo test --workspace` -- every `crowkv`/
  `crowtree-ffi` test (the crates this migration actually touches) passes
  consistently across repeated runs; `crowkv-web`'s network-integration
  tests show pre-existing, unrelated flakiness under parallel execution
  (a different test fails each full-workspace run, always passes in
  isolation) -- confirmed via isolated re-runs to be a real, pre-existing
  environment issue, not a regression from this change.

`design-crowtree-async.md` Phases 0–5 (Reactor, C API, Rust `Future`s,
zero-copy fast path, benchmarks) landed first, independently, exactly as
planned -- nothing in this doc's caller-side work needed to precede it.
