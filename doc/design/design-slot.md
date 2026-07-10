# CrowKV - Design: Concurrent Sparse Slot List

Depends on: [`design.md`](../design.md), [`plan.md`](../plan.md)
Satisfies: [`requirement.md`](../requirement.md) §5.1 (high-concurrency log), [`plan.md`](../plan.md) §1 P1 M1 / P2 M3

## 1. Problem Statement

The current Acceptor state uses two `BTreeMap<PxSlot, T>` instances for `promised` and `accepted`:

- **O(log n) lookup** — sequential Paxos workloads are dominated by append + tail-read + occasional replay; a tree is overkill.
- **High allocator pressure** — every slot is a separate heap allocation (`BTreeMap` node).
- **No natural front-GC boundary** — trimming old slots requires iterating and deleting individual tree nodes.
- **Not cache-friendly** — tree nodes are scattered in memory; sequential replay follows pointer chains.

`PxSlot` is a **monotonically increasing `u64`** allocated by an external sequencer. The access pattern is strongly **tail-biased** (latest-slot prepare / accept / replay), while actual allocation may still be **sparse** because chunks are created lazily and repair may touch older slots out of order. We need a data structure that exploits both properties.

## 2. Goals

| Goal | How measured |
|---|---|
| O(1) insert by slot | `insert(slot, value)` must not re-allocate on every slot; chunk created lazily if missing |
| O(1) hot-slot access | `get_tail(slot)` should hit the newest chunk first because most reads target recent slots |
| O(1) general index access | `get(slot)` resolves a slot by chunk offset without hash/tree traversal |
| O(1) batch front-trim | `trim(before_slot)` drops whole chunks, not per-slot |
| Wait-free / lock-free reads | Readers (proposer, learner, state machine) never block each other |
| Safe reclamation | A chunk is freed only after **all** concurrent readers have passed it |
| Async-compatible | Must work inside `tokio` tasks; no `std::thread::park` or blocking locks |

## 3. Design Overview

A **chunked, reader-pinned concurrent sparse list** (`SlotList<T>`).

```
SlotList<T>
├─ head  ──► Chunk { start: 1024, entries: [1024..2047] }  (partially filled)
│            ⇅
│            Chunk { start: 4096, entries: [4096..5119] }  (sparse gap 2048..4095)
│            ⇅
│            Chunk { start: 5120, entries: [5120..6143] }  ◄── tail
│
├─ trim_slot: AtomicU64  (slots below this are logically invalid)
└─ retired:   AtomicPtr<RetiredChunk<T>>
```

`PxSlot` is assigned by an external global sequencer.  The list is **sparse**: only slots that actually carry a value allocate a chunk, and chunks inside a chunk are created **lazily** on first `insert`.  No `end_slot` is kept by the list itself; the caller (Acceptor / WAL) decides the next slot to use.

Each chunk is a fixed-size array (`SLOT_CHUNK_SIZE = 1024` slots). A slot index maps to a chunk via `chunk = start / SLOT_CHUNK_SIZE` and an intra-chunk offset. Chunks are linked in both directions so the general path can walk from `head`, while the hot path can walk backward from `tail`.

### 3.1 Chunk Layout

```rust
const SLOT_CHUNK_SIZE: usize = 1024;

struct Chunk<T> {
    start_slot: PxSlot,
    /// Each entry is an atomic pointer so readers and writers race safely.
    /// `null`  = slot not yet written (or already trimmed).
    entries: [AtomicPtr<T>; SLOT_CHUNK_SIZE],
    next: AtomicPtr<Chunk<T>>,
    prev: AtomicPtr<Chunk<T>>,
    /// Number of live entries still in this chunk.  When it hits zero the
    /// chunk is eligible for retirement.
    live_count: AtomicUsize,
    /// Number of readers currently pinned on this chunk.
    reader_refs: AtomicU32,
    /// Set once the chunk has been detached from the live list.
    retired: AtomicBool,
    /// padded to a cache line to prevent false sharing between adjacent chunks
    _pad: [u8; 64],
}
```

**Why `AtomicPtr<T>` per slot instead of `Option<T>`?**
- Writers CAS from `null → Box::into_raw(Box::new(value))`.
- Readers pin the containing chunk (`reader_refs += 1`), then load the pointer.
- Retired chunks are freed only after `reader_refs == 0`, guaranteeing no dangling reader.

### 3.2 List Header

```rust
pub struct SlotList<T> {
    /// First chunk that still contains live data.
    head: AtomicPtr<Chunk<T>>,
    /// Rightmost chunk — new inserts land here (or a newly created gap chunk).
    tail: AtomicPtr<Chunk<T>>,
    /// Monotonically increasing; slots **strictly below** this are logically
    /// invalid (trimmed).  Every `get` checks against this watermark first.
    trim_slot: AtomicU64,
    /// Chunks detached by `trim` and waiting for `reader_refs == 0`.
    retired: AtomicPtr<Chunk<T>>,
}
```

### 3.3 Slot Node (Paxos usage)

For the `PxAcceptor` we store **both** the promised ballot and the accepted entry in a single node, eliminating the double-map indirection.  Because multiple proposers may race on the same slot, the node uses field-level `AtomicPtr` so updates are lock-free once the node pointer is installed.

```rust
pub struct PxSlotNode {
    /// Highest ballot promised.  Null until first `prepare`.
    promised: AtomicPtr<PxBallot>,
    /// Accepted entry.  Null until first `accept`.
    accepted: AtomicPtr<PxLogEntry>,
    // ---------- deferred reclamation state (correctness-critical) ----------
    // Replaced field pointers are pushed here and reclaimed when node drops.
    retired_promised: AtomicPtr<RetiredPtr<PxBallot>>,
    retired_accepted: AtomicPtr<RetiredPtr<PxLogEntry>>,
}

/// The acceptor holds one slot list.
pub struct PxAcceptor {
    log: SlotList<PxSlotNode>,
}
```

Paxos operations use `get_tail_ptr` / `get_ptr` to obtain the `AtomicPtr<PxSlotNode>` for a slot, then CAS the node pointer itself (installing a default node if absent) before mutating fields inside the stable node:

```rust
/// Hot path: tail-first lookup, install default node if absent.
fn get_or_prepare_slot(list: &SlotList<PxSlotNode>, slot: PxSlot)
    -> Option<&PxSlotNode>
{
    let ptr_guard = list.get_tail_ptr(slot)?;
    let slot_atomic = &*ptr_guard;

    let mut node_ptr = slot_atomic.load(Ordering::Acquire);
    if node_ptr.is_null() {
        let new = Box::into_raw(Box::new(PxSlotNode::default()));
        match slot_atomic.compare_exchange(null_mut(), new, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_)  => node_ptr = new,
            Err(p) => { unsafe { drop(Box::from_raw(new)); } node_ptr = p; }
        }
    }
    // ptr_guard is dropped here → chunk pin released.
    // The PxSlotNode is separately heap-allocated and stays valid
    // because SlotList::insert never replaces an existing slot pointer.
    Some(unsafe { &*node_ptr })
}
```

`prepare(slot, ballot)` → `get_or_prepare_slot(log, slot)` → CAS `promised` field inside node.
`accept(entry)`          → `get_or_prepare_slot(log, entry.slot)` → validate ballot, CAS `accepted`.

`PxSlotNode` uses deferred reclamation for field replacement: when
`cas_promised` / `cas_accepted` successfully swaps an existing pointer, the old
pointer is linked into a per-field retired list instead of being freed
immediately. On `PxSlotNode::drop`, the current pointers and retired lists
are drained and freed together. This avoids historical replacement leaks while
preserving the raw-reference API (`promised() -> Option<&PxBallot>`,
`accepted() -> Option<&PxLogEntry>`).

## 4. Algorithms

### 4.1 Insert (by external slot)

The caller (global sequencer, Acceptor, or WAL) decides the exact `slot`; the list only stores the value.  Slots may be non-consecutive (sparse).

```rust
pub fn insert(&self, slot: PxSlot, value: T) -> SlotReadGuard<'_, T> {
    assert!(slot >= self.trim_slot.load(Ordering::Acquire));

    let offset = slot % SLOT_CHUNK_SIZE as u64;

    loop {
        // 1. Locate or lazily create the chunk covering this slot.
        let chunk = self.find_or_create_chunk(slot);
        assert!(offset < SLOT_CHUNK_SIZE as u64);

        // 2. Pin the chunk *before* touching any slot pointer.
        chunk.reader_refs.fetch_add(1, Ordering::Acquire);

        // 3. If trim raced and retired the chunk, retry with a fresh one.
        if chunk.retired.load(Ordering::Acquire)
            || slot < self.trim_slot.load(Ordering::Acquire)
        {
            chunk.reader_refs.fetch_sub(1, Ordering::Release);
            continue;
        }

        // 4. Install the slot object exactly once.
        //    If another writer raced, keep the existing object and discard ours.
        let new_ptr = Box::into_raw(Box::new(value));
        match chunk.entries[offset as usize].compare_exchange(
            null_mut(),
            new_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                chunk.live_count.fetch_add(1, Ordering::Relaxed);
                return SlotReadGuard::new(chunk, new_ptr);
            }
            Err(existing) => {
                unsafe { drop(Box::from_raw(new_ptr)); }
                return SlotReadGuard::new(chunk, existing);
            }
        }
    }
}
```

`insert` is a convenience wrapper around `get_ptr` + `compare_exchange` + `into_read_guard` (see §4.6).  It never replaces an existing slot object.  Callers that need finer-grained control (e.g. Paxos field-level CAS) should use `get_ptr` or `get_tail_ptr` directly.

**Chunk lookup / lazy creation:**

```rust
fn find_or_create_chunk(&self, slot: PxSlot) -> &Chunk<T> {
    let aligned_start = (slot / SLOT_CHUNK_SIZE as u64) * SLOT_CHUNK_SIZE as u64;

    // Retry loop: find predecessor/successor window, then splice a new chunk in.
    loop {
        let (pred, succ) = self.find_window(aligned_start);
        if !succ.is_null() && unsafe { &*succ }.start_slot == aligned_start {
            return unsafe { &*succ };
        }

        let new_chunk = Box::into_raw(Box::new(Chunk::new(aligned_start)));
        unsafe {
            (*new_chunk).prev.store(pred, Ordering::Relaxed);
            (*new_chunk).next.store(succ, Ordering::Relaxed);
        }

        if self.link_between(pred, succ, new_chunk).is_ok() {
            if !succ.is_null() {
                unsafe { &*succ }.prev.store(new_chunk, Ordering::Release);
            } else {
                self.tail.store(new_chunk, Ordering::Release);
            }
            return unsafe { &*new_chunk };
        }

        unsafe { drop(Box::from_raw(new_chunk)); }
    }
}
```

**Notes:**
- `find_window` returns the predecessor / successor pair around the aligned chunk start.  It is O(number of chunks) but the chunk count is bounded by `live_window / SLOT_CHUNK_SIZE`.
- If the chunk already exists, `insert` performs a single slot-level CAS and never swaps out a live pointer.
- Sparse gaps are cheap: no chunk is allocated for empty ranges, so `SLOT_CHUNK_SIZE` can remain large (1 K or 4 K) without wasting memory on unused slots.

### 4.2 Get (head-first, general path)

```rust
pub fn get(&self, slot: PxSlot) -> Option<SlotReadGuard<'_, T>> {
    if slot < self.trim_slot.load(Ordering::Acquire) {
        return None;
    }

    let mut chunk = self.head.load(Ordering::Acquire);
    while !chunk.is_null() {
        let c = unsafe { &*chunk };
        let end = c.start_slot + SLOT_CHUNK_SIZE as u64;

        if slot >= c.start_slot && slot < end {
            c.reader_refs.fetch_add(1, Ordering::Acquire);

            if c.retired.load(Ordering::Acquire)
                || slot < self.trim_slot.load(Ordering::Acquire)
            {
                c.reader_refs.fetch_sub(1, Ordering::Release);
                return None;
            }

            let offset = (slot - c.start_slot) as usize;
            let ptr = c.entries[offset].load(Ordering::Acquire);
            if ptr.is_null() {
                c.reader_refs.fetch_sub(1, Ordering::Release);
                return None;
            }

            return Some(SlotReadGuard::new(c, ptr));
        }

        if c.start_slot > slot {
            return None;
        }

        chunk = c.next.load(Ordering::Acquire);
    }

    None
}
```

Use `get(slot)` when the caller needs a correct answer for **any** historical slot and is willing to pay a head-to-tail walk.

### 4.3 Get Tail (tail-first, hot path)

```rust
pub fn get_tail(&self, slot: PxSlot) -> Option<SlotReadGuard<'_, T>> {
    if slot < self.trim_slot.load(Ordering::Acquire) {
        return None;
    }

    let mut chunk = self.tail.load(Ordering::Acquire);
    while !chunk.is_null() {
        let c = unsafe { &*chunk };
        let end = c.start_slot + SLOT_CHUNK_SIZE as u64;

        if slot >= c.start_slot && slot < end {
            c.reader_refs.fetch_add(1, Ordering::Acquire);

            if c.retired.load(Ordering::Acquire)
                || slot < self.trim_slot.load(Ordering::Acquire)
            {
                c.reader_refs.fetch_sub(1, Ordering::Release);
                return None;
            }

            let offset = (slot - c.start_slot) as usize;
            let ptr = c.entries[offset].load(Ordering::Acquire);
            if ptr.is_null() {
                c.reader_refs.fetch_sub(1, Ordering::Release);
                return None;
            }

            return Some(SlotReadGuard::new(c, ptr));
        }

        if slot >= end {
            return None;
        }

        chunk = c.prev.load(Ordering::Acquire);
    }

    None
}
```

`get_tail` is the normal Paxos read path: latest-slot prepare / accept / replay almost always hit the last chunk or one of its immediate predecessors.

### 4.4 Trim (front GC)

Triggered by snapshot installation or explicit log compaction:

```rust
/// All slots `< before_slot` become logically invalid and their chunks
/// are eventually reclaimed.
pub fn trim(&self, before_slot: PxSlot) {
    // Contract: single GC caller only.
    // Concurrent trim callers are unsupported and should fail fast.

    self.trim_slot.fetch_max(before_slot, Ordering::AcqRel);

    let mut chunk_ptr = self.head.load(Ordering::Acquire);
    while !chunk_ptr.is_null() {
        let chunk = unsafe { &*chunk_ptr };
        let chunk_end = chunk.start_slot + SLOT_CHUNK_SIZE as u64;

        if chunk_end > before_slot {
            break; // still contains live slots
        }

        let next = chunk.next.load(Ordering::Acquire);
        chunk.retired.store(true, Ordering::Release);

        match self.head.compare_exchange(chunk_ptr, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => {
                if !next.is_null() {
                    unsafe { &*next }.prev.store(null_mut(), Ordering::Release);
                } else {
                    self.tail.store(null_mut(), Ordering::Release);
                }

                self.push_retired(chunk_ptr);
                chunk_ptr = next;
            }
            Err(actual) => {
                chunk.retired.store(false, Ordering::Release);
                chunk_ptr = actual;
            }
        }
    }
}
```

`trim` is **single-caller by contract**. The unlink path updates `head`, `prev`, and retired-list ownership as one logical GC operation; concurrent trim callers would need additional synchronization (or a stronger lock-free proof) to guarantee consistent list structure.

Each `get` / `get_tail` checks `trim_slot` **before** touching any chunk pointer, so once `trim_slot` advances, newly arriving readers will immediately reject invalid slots.

### 4.5 Chunk-Level Reclamation

**Requirement:** a chunk must not be `drop`ped while any reader still holds a reference to an entry inside it.

Because the list rejects trimmed slots via `trim_slot` before any pointer chase, the only readers that can still observe a retired chunk are those that pinned it **before** trim detached it. We therefore track read-side critical sections at **chunk granularity** rather than with a global epoch.

**Mechanism:**

1. `get()` / `get_tail()` increments `chunk.reader_refs` before dereferencing any slot pointer.
2. `trim()` marks the chunk `retired = true`, detaches it from the live list, and pushes it onto a **retired list**.
3. A background `reclaim()` task (or explicit call after trim) walks the retired list:
   - For each retired chunk, check whether `reader_refs == 0`.
   - If yes → `unsafe { drop(Box::from_raw(chunk_ptr)) }`.
   - If no → leave it on the list for the next GC pass.

`reclaim` is also **single-caller by contract**. Concurrent reclaim callers can race on retired-list unlinking and must be rejected by API/runtime guard.

**Why chunk pinning is sufficient:**
- `trim_slot` is advanced **before** chunk unlinking (see §4.4). New readers that start after trim will return `None` immediately, never touching the chunk.
- Only readers that pinned the chunk before trim can still hold a reference to it.
- Paxos reads are short-lived; the practical lifetime of a pinned chunk is typically a few microseconds.

**Reader lifecycle race analysis (current limitation):**
- There is a narrow race between "reader has observed a chunk pointer" and "reader has incremented `reader_refs`".
- If reclaim frees the chunk inside that window, a late reader can dereference a freed chunk pointer.
- Therefore, the current manual `reader_refs` scheme is only safe under a restricted envelope (single GC caller, operational discipline, and no reclaim while such late readers are possible).
- For a fully general lock-free safety guarantee, use one of: global epoch/hazard pointers, or `Arc<Chunk<T>>` ownership for read traversal.

**Read guard:**
```rust
pub struct SlotReadGuard<'a, T> {
    chunk: &'a Chunk<T>,
    ptr: *const T,
    _marker: PhantomData<&'a T>,
}

impl<T> Deref for SlotReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr }
    }
}

impl<T> Drop for SlotReadGuard<'_, T> {
    fn drop(&mut self) {
        self.chunk.reader_refs.fetch_sub(1, Ordering::Release);
    }
}
```

This keeps the safety rule local: if a caller holds a `SlotReadGuard`, the backing chunk cannot be reclaimed.

### 4.6 Slot-Pointer Access (Caller-Controlled CAS)

For algorithms that need to CAS the slot pointer itself — e.g. Paxos installing a default node, or idempotent updates — `SlotList` exposes `get_ptr` and `get_tail_ptr`.  These return a `SlotPtrGuard` that pins the chunk and derefs to `&AtomicPtr<T>` (the slot pointer itself, not the value).

```rust
pub struct SlotPtrGuard<'a, T> {
    chunk: &'a Chunk<T>,
    offset: usize,
}

impl<T> Deref for SlotPtrGuard<'_, T> {
    type Target = AtomicPtr<T>;

    fn deref(&self) -> &Self::Target {
        &self.chunk.entries[self.offset]
    }
}

impl<T> Drop for SlotPtrGuard<'_, T> {
    fn drop(&mut self) {
        self.chunk.reader_refs.fetch_sub(1, Ordering::Release);
    }
}
```

**List methods:**

```rust
impl<T> SlotList<T> {
    /// General path: walks from head, returns `&AtomicPtr<T>` for the slot.
    pub fn get_ptr(&self, slot: PxSlot) -> Option<SlotPtrGuard<'_, T>>;
    /// Hot path: walks from tail, returns `&AtomicPtr<T>` for the slot.
    pub fn get_tail_ptr(&self, slot: PxSlot) -> Option<SlotPtrGuard<'_, T>>;
}
```

`insert` (§4.1) is exactly `get_tail_ptr` + `compare_exchange` + construct `SlotReadGuard` from the winning pointer.  Paxos `prepare` uses `get_tail_ptr` to obtain the slot pointer, CAS-installs a default `PxSlotNode` if absent, and then CASes fields inside the stable node.

**Alternative (simpler, acceptable for initial implementation):**
- Use `Arc<Chunk<T>>` for chunk links instead of raw pointers.
- `trim()` unlinks chunks; the `Arc` ref-count drops to zero when the last reader finishes.
- `get()` / `get_tail()` clones the `Arc` briefly, reads, then drops it.
- This avoids `unsafe` entirely and is only marginally slower under the expected single-leader workload.

## 5. API Surface (Rust)

```rust
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicU32, AtomicBool, Ordering};

pub struct SlotReadGuard<'a, T> {
    chunk: &'a Chunk<T>,
    ptr: *const T,
    _marker: PhantomData<&'a T>,
}

pub struct SlotList<T> {
    // ... (fields above)
}

impl<T> SlotList<T> {
    pub fn new() -> Self;
    /// General access: walks from head, valid for any slot (including gaps).
    pub fn get(&self, slot: PxSlot) -> Option<SlotReadGuard<'_, T>>;
    /// Optimised tail access: checks tail chunk first, then predecessor chunks.
    /// Callers that expect recent slots (e.g., replay, prepare on latest log
    /// entry) should use this path.
    pub fn get_tail(&self, slot: PxSlot) -> Option<SlotReadGuard<'_, T>>;
    /// General path returning the raw `AtomicPtr<T>` for caller-controlled CAS.
    pub fn get_ptr(&self, slot: PxSlot) -> Option<SlotPtrGuard<'_, T>>;
    /// Hot path returning the raw `AtomicPtr<T>` for caller-controlled CAS.
    pub fn get_tail_ptr(&self, slot: PxSlot) -> Option<SlotPtrGuard<'_, T>>;
    /// Iterate all present entries in `[start_slot, end_slot_exclusive)`.
    /// Each item carries its own guard and is safe with concurrent readers/writers.
    pub fn iter_range(
        &self,
        start_slot: PxSlot,
        end_slot_exclusive: PxSlot,
    ) -> SlotIter<'_, T>;
    pub fn insert(&self, slot: PxSlot, value: T) -> SlotReadGuard<'_, T>;
    /// Logically invalidates all slots `< before_slot` and unlinks their chunks.
    pub fn trim(&self, before_slot: PxSlot);
    /// Walk retired chunks and free those whose `reader_refs == 0`.
    /// Contract: single GC caller only.
    pub fn reclaim(&self) -> usize;
    /// Returns the current trim watermark.  Slots `< this` are permanently gone.
    pub fn trim_slot(&self) -> PxSlot;
    /// Number of logically live slot objects.
    pub fn len(&self) -> usize;
}

impl<T> Deref for SlotReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target;
}

impl<T> Drop for SlotReadGuard<'_, T> {
    fn drop(&mut self);
}
```

## 6. Correctness Invariants

| Invariant | Why it matters | Enforced by |
|---|---|---|
| **I1 — External slot assignment** | Slot number is chosen by the caller; the list only stores at that index | `insert` validates `slot` is inside the chunk range it targets |
| **I2 — Stable slot object** | Once a slot object is installed, readers keep seeing the same pointer | `insert` / `get_ptr` only CASes `null → ptr`; later updates mutate fields inside the object, controlled by `T` not `SlotList` |
| **I3 — Trim coherence** | Readers never observe a slot that has been trimmed | `trim_slot` is advanced **before** chunks are unlinked; every `get` / `get_tail` checks it first |
| **I4 — No dangling reads** | A retired chunk is freed only after all readers pinned on it have dropped their guards | Chunk-level `reader_refs` + retired list |
| **I5 — Sparse ordering** | Chunks stay ordered by `start_slot`, so head-first and tail-first walks can stop early | `find_window` and `link_between` preserve sorted doubly linked order |

## 7. Performance Model

| Operation | Cost | Notes |
|---|---|---|
| `insert` (existing chunk) | 1 slot CAS + 1 reader-ref increment | Reuses the existing slot object if already present |
| `insert` (new chunk) | 1 allocation + chunk-link CAS | Amortised only when touching a previously empty chunk range |
| `get_tail` | 1 trim check + 1 chunk pin + 1 atomic load | Tail chunk is cache-hot; covers most reads in practice |
| `get` (head-first) | 1 trim check + ≤ N chunk hops + 1 atomic load | N = number of live sparse chunks |
| `trim` | 1 watermark advance + O(retired chunks) unlink work | Drops whole chunks, never individual slots |
| `reclaim` | O(retired chunks) | Frees only chunks whose `reader_refs == 0` |
| Memory overhead | ~8 bytes / slot (on 64-bit) | `AtomicPtr` per slot; chunk metadata negligible |

**Key insight:** `trim_slot` is a very cheap first-line filter, and `get_tail` avoids the head walk entirely for the dominant newest-slot read path.

Compared to `BTreeMap`:
- **~10× faster insert** (array index vs. tree rebalance)
- **~5× faster latest-slot access** (tail-first bounded walk vs. tree lookup)
- **~3× lower per-slot overhead** (8 bytes vs. BTree node ~24 bytes + allocator metadata)

## 8. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| `unsafe` in raw-pointer linking | Review with `miri`; maintain `Arc` fallback for comparison |
| `reader_refs` leak due to forgotten guard drop | Keep all public reads guard-based; no bare `&T` escapes |
| Prev/next inconsistency under sparse insertion | Centralise linking in `find_window` / `link_between`; validate with stress tests |
| Chunk alignment / false sharing | `#[repr(align(64))]` on `Chunk`; validate with `perf c2c` |
| Reader late-pin race (`ptr observed` before `reader_refs += 1`) | Short-term: single GC caller + disciplined reclaim timing. Long-term: epoch/hazard pointers or `Arc<Chunk<T>>` |
| `PxSlotNode` replacement churn increases temporary memory | Replaced field pointers go to retired lists and are reclaimed on node drop; earlier reclamation requires a guarded/epoch field read API (future evolution) |

## 9. Open Questions

1. **Manual `reader_refs` vs `Arc<Chunk<T>>`:**
   - Manual ref counting is cheaper on the hot path and keeps chunk layout explicit.
   - `Arc` dramatically reduces unsafe linking complexity.
   - **Recommendation:** keep manual `reader_refs` as the target design; validate against an `Arc` prototype if implementation risk grows.

2. **Chunk size tuning:**
   - 1 K slots = 8 KB of `AtomicPtr`s per chunk (plus metadata).
   - Larger chunks (4 K, 16 K) reduce pointer-chasing but increase allocator pressure on chunk creation.
   - **Decision:** start with 1 K; make `SLOT_CHUNK_SIZE` a `const` generic for easy benchmarking.

3. **Tail-get cache line layout:**
   - `get_tail` loads `self.tail` then the chunk's entries array.  If `tail` and `head`/`trim_slot` share a cache line, tail-get readers may false-share with trim writers.
   - **Mitigation:** pad the `SlotList` header to 64 bytes so `tail` sits on its own cache line; verify with `perf c2c`.

4. **Per-slot `AtomicPtr` vs. inline slot storage:**
   - `AtomicPtr` is simplest and lock-free.
   - Inline storage avoids one extra pointer indirection but makes sparse chunk initialisation and guard-based lifetimes more awkward.
   - **Decision:** start with `AtomicPtr`; optimise if profiles show it as a bottleneck.

5. **`PxSlotNode` replaced-pointer reclamation evolution:**
   - Current behavior (implemented): replaced `promised` / `accepted` pointers are retired and reclaimed on `PxSlotNode::drop`.
   - Open issue: this is safe for current raw-reference API, but replacement-heavy hot slots can accumulate temporary memory until node drop.

## 10. Reclamation Evolution Plan (`PxSlotNode`)

### 10.1 Current Implementation

- External API: `promised() -> Option<&PxBallot>`, `accepted() -> Option<&PxLogEntry>`.
- On successful field replacement CAS, old pointer is moved into a per-field retired list.
- Retired lists are drained only when the node is dropped.
- Benefit: no historical replacement leak, no dangling-reference regression.

### 10.2 Future Evolution Triggers

Consider implementing earlier reclamation when **any** of the following is observed:

- Per-node retired-chain depth keeps growing across GC cycles for hot slots.
- Process RSS growth is dominated by `PxBallot` / `PxLogEntry` churn from repeated replacements.
- Tail latency regression correlates with large node-drop reclamation bursts.

### 10.3 Future Evolution Options

1. **Epoch/Hazard-based early reclaim (preferred long-term)**
   - Introduce reader critical-section API for node-field reads.
   - Retire replaced pointers into epoch bins.
   - Reclaim when no active reader can hold references to those bins.

2. **Guarded/Owned return API (interface change)**
   - Replace raw-reference returns with owned snapshots or explicit read guards.
   - Allows earlier reclamation with clearer lifetime boundaries.
   - Requires broad caller migration in Paxos paths.

3. **`Arc` payload fields (lowest unsafe complexity, higher overhead)**
   - Store `Arc<PxBallot>` / `Arc<PxLogEntry>` semantics at field level.
   - CAS pointer replacement still possible; object lifetime delegated to refcount.
   - Simpler correctness, higher per-operation cost.

### 10.4 Validation

- Command:
  - `cargo bench -p crowkv --bench slot_list -- slot_node_reclaim_churn --warm-up-time 0.2 --measurement-time 0.8 --sample-size 20`
- Latest baseline:
  - `slot_node_reclaim_churn/1000`: `125.72 µs`
  - `slot_node_reclaim_churn/10000`: `1.6247 ms`

### 10.5 Acceptance Criteria (for any future reclamation evolution)

- No use-after-free under stress (`miri` + concurrency stress harness).
- No unbounded retired growth for replacement-heavy workloads.
- P50/P99 latency of `prepare`/`accept` remains within agreed budget.
- Design and test docs updated with the selected reclamation mode.
