# CrowKV — Async `KVEngine` Trait Shape

> **Status:** implemented (landed 2026-07-09).
> **Parent:** [`design-crowtree-engine.md`](design-crowtree-engine.md) (the C++/FFI
> reactor this trait shape's `Pending` path consumes; §3 "Async FFI Bridge").

This document records the design of the Rust-side `KVEngine` trait's async
shape (`KVFuture<T>`) and why it looks the way it does — kept as the
rationale record for a decision that is easy to get wrong (the naive
`async-trait` conversion), not as a live plan.

## 1. The problem

`KVEngine` is consumed as `Box<dyn KVEngine>` (`PxLearner::engine`), chosen at
runtime via a CLI flag (`--kv-engine {memory,crowtree}`). Its `get`/`scan`/
`apply` need an async-capable return type so that a genuine I/O path
(crowtree demand-load miss, served by the io_uring reactor,
[`design-crowtree-engine.md §3`](design-crowtree-engine.md#3-async-ffi-bridge))
can suspend instead of blocking a Tokio worker thread on a synchronous
`pread` — the exact anti-pattern the reactor exists to avoid at the C++/FFI
layer, which would otherwise resurface immediately one layer up in Rust.

## 2. The central tension: `dyn KVEngine` vs. `async fn` in traits

Native `async fn` in traits is **not `dyn`-compatible**. Three ways to square
that with runtime engine selection:

| Option | Cost |
| --- | --- |
| **(a) `async-trait` crate** | Boxes every async call into a `Pin<Box<dyn Future>>` via macro — one heap allocation **per call, including the fast in-memory path with no I/O**. Undoes the reactor's "fast path costs nothing" property one layer up. |
| **(b) Generic `PxLearner<E: KVEngine>`** | Zero overhead, but `E` would need to propagate through `PxLocalReplica`, `PxGroup`, `PxKvStore`, and `DashMap<GroupId, PxGroup>` — too invasive for an engine chosen at runtime. |
| **(c) Hybrid fast-path/slow-path future (chosen)** | Plain (non-`async`) `fn`s return a small custom future enum that resolves immediately (no allocation) for the fast path and only boxes a real future for the rare slow (I/O) path. Fully `dyn`-compatible; mirrors the exact fast/slow split the C++ layer already makes. |

## 3. `KVFuture<T>` and the trait shape

```rust
pub enum KVFuture<T> {
    Ready(Option<T>),                              // taken on first poll; re-polling panics
    Pending(Pin<Box<dyn Future<Output = T> + Send>>),
}

impl<T> KVFuture<T> {
    pub fn ready(v: T) -> Self { KVFuture::Ready(Some(v)) }
}

pub trait KVEngine: Send + Sync {
    fn apply(&self, slot: u64, batch: &Batch) -> KVFuture<()>;
    fn get(&self, key: &[u8]) -> KVFuture<Option<(u64, Vec<u8>)>>;
    fn scan(&self, prefix: &[u8], limit: usize) -> KVFuture<(Vec<(Vec<u8>, u64, Vec<u8>)>, bool)>;
    // iter_all / live_key_count / clear / compare / resume_from_slot /
    // persist_snapshot / set_gc_watermark / collect_garbage: plain sync fns.
    // Diagnostic/maintenance-path only (compare/iter_all: tests + snapshot
    // export; the rest: restore path + the periodic group-maintenance
    // task), never on the hot Paxos-accept / gRPC-read path, so a brief
    // blocking call there is an acceptable trade-off.
}
```

`Ready` costs nothing beyond the enum tag + inline value — no allocation, no
`Pin<Box<..>>`. `InMemKV` always returns `Ready`. `CrowtreeEngine::get`
(`crowkv/src/kv/crowtree_engine.rs`) does the same fast-path check the C++
layer does first, via `crowtree_ffi::AsyncCrowtree::try_get`; on a resident
hit/miss it returns `Ready` at zero extra cost, and only on a genuine
demand-load miss does it construct `Pending`, wrapping the reactor-driven
future `try_get` already builds. `CrowtreeEngine::scan`/`apply` always
resolve `Ready` today (no async `scan`/`apply` C API exists yet — an honest
gap, not an oversight; see `CrowtreeEngine`'s doc comment).

## 4. Caller-side wiring

`Learner::learn` is a native `async fn` (mirroring `Acceptor`'s existing
`async fn accept`/`prepare` convention — neither trait is ever used as
`dyn`, so native `async fn` is safe for both). `PxLearner::apply_entry`/
`engine_get`/`engine_scan` are `async fn` and `.await` the `KVFuture`
directly. `PxLocalReplica::learn_chosen`/`apply_committed_up_to` and
`PxKvStore::kv_get`/`kv_scan` were already `async fn` (already awaited by
every caller) and now genuinely `.await` a future that can suspend, instead
of always resolving on the first poll.

`engine_scan`/`apply_entry` stay `async fn` for signature uniformity with
`engine_get`, but never actually suspend today, matching §3's note that
`scan`/`apply` have no async C API yet.

## 5. Testing

- `kv/kv_future_test.rs`: `KVFuture::ready(v)` polls to `Ready(v)` on the
  first poll without registering a waker; a `Pending` variant polls through
  to the wrapped future's result; polling a completed `Ready` again panics.
- `InMemKV` regression guard (`mem_kv_test.rs`): every call returns `Ready`
  (asserted via `matches!`, not just the unwrapped value, so an accidental
  switch to `Pending` fails loudly here first).
- `CrowtreeEngine` regression tests (`crowtree_engine_test.rs`): a resident
  hit/miss returns `Ready`; `get_constructs_pending_for_genuine_demand_load_miss`
  evicts a durable engine's resident leaf and asserts `get` returns `Pending`
  and resolves to the right value.
- `paxos/learner_async_test.rs`: the same fast/slow property one layer up,
  through `PxLearner::engine_get`.
