<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R32: Custom Rust RPC library to replace gRPC on the hot path

**Problem**: gRPC (tonic + h2) serializes concurrent writers on a
connection-level userspace lock. HTTP/2's stream/HPACK/flow-control
architecture inherently requires a connection-level lock for correctness
— the shared HPACK table, frame output buffer, and flow-control windows
are mutable state that demands serialization. When N threads submit to
one gRPC connection, they funnel through a single-threaded userspace
critical section before any of them reach `write()`. The kernel's TCP
coalescing never sees concurrent writers; h2 hands it one merged buffer
from one thread.

The measured cost is a **~17% throughput drop at 2T:1C** (read bench,
`doc/design/kv/kv-read-flow-analysis.md` ≈L520-545). At 1T:1C the lock is uncontended
and the cost is zero; the loss grows with thread:connection ratio.

A custom protocol (`[len][req_id][protobuf]` over raw TCP) has no
connection-level userspace lock. Length-prefixed framing is stateless —
no shared encoder table, no per-stream state, no flow-control windows.
Note that threads cannot call `write()` on the socket directly: a TCP
`write()` is not atomic under partial writes (a full send buffer can
interleave bytes from two threads and corrupt the framing). The proven
design (protosocket, Volo) is a per-connection writer task draining a
lock-free MPSC queue with `writev` batching. That is still one
serialization point per connection, but a vastly cheaper one — a queue
push instead of HPACK + stream state + flow-control bookkeeping under a
mutex. The expensive userspace funnel is gone.

This is a **design mismatch**, not a tuning problem — h2 cannot accept
concurrent writers without a lock.

**Related but separate — heartbeat starvation under write backpressure.**
The same single-connection design also serializes heartbeats behind
accepts on the wire (E5 reserves queue admission, not wire priority).
Under 16 KiB write backpressure this can exceed the election timeout
and cause spurious leader churn. This is a **correctness/availability**
issue with a pure-gRPC mitigation (separate `Channel` for heartbeats),
tracked separately as
[R53](R53-kv-replica-heartbeat-channel.md) — it does not require R32's
custom transport.

**Decision (2026-07-29)**: not replacing gRPC now. A custom transport
would eliminate the connection lock and recover the lost concurrency,
but requires reimplementing connection pooling, reconnect, timeout,
cancellation, error propagation, backpressure, and TLS — 2–4K lines of
infrastructure that gRPC provides. The lock's cost is bounded (~17% at
2T:1C, avoided entirely at 1T:1C) and the current bottleneck for
production workloads is consensus (writes) or disk I/O, not read-path
framing.

**Deferred until**: read throughput becomes the primary constraint AND
the h2 connection lock is profiled as the hot spot. Until then, R16a /
R17 / R30 (write-path) and any disk-I/O work take precedence.

**Expected gain** (set expectations before building):
- ~17–30% recovery at moderate thread:connection ratios (2T:1C and
  up) — the measured h2-lock loss, not a multiple.
- 2–3× only at extreme ratios (e.g. 48T:12C lin = 42K @ 1.1ms vs
  48T:48C = 145K @ 326us) — the regime behind protosocket's ~2.75×
  claim. Today that regime is avoided by running 1T:1C.
- Near-zero gain at 1T:1C — the lock is uncontended; only HPACK/frame
  encoding overhead (a few us vs ~138us read latency) is saved.
- The strategic value is removing the constraint that forces 1T:1C:
  few connections carrying high concurrency with linear thread scaling.

**Transport phasing** (event backend):
- Phase 1 — tokio/epoll: raw TCP + length-prefix + prost on plain
  tokio (mio/epoll underneath). Removes the h2 lock while keeping the
  async ecosystem (timeouts, cancellation, tower layers). This is the
  protosocket architecture and captures the full lock-removal win.
- Phase 2 (separate, only if profiled) — io_uring: batched submission
  and fewer syscalls matter at millions of ops/s per core; at current
  throughput syscalls are not the bottleneck. io_uring requires owned
  buffers (kernel holds them during the op), which conflicts with
  tokio's borrowed-buffer model — effectively a thread-per-core runtime
  change (monoio/glommio/tokio-uring), not a transport swap. Also
  needs recent kernels and is often disabled by seccomp/container
  policies in production.

**Priority**: Medium — the 17% loss at 2T:1C is measured, not
hypothetical, but it only bites when thread:connection ratio is high on
the read path, which is not the current production bottleneck.

**Complexity**: High — 2–4K lines of custom RPC infrastructure
(framing, connection pool, reconnect, timeout, cancellation, error
propagation, backpressure, TLS). Keep `prost`/protobuf for
serialization to reuse existing `.proto` schemas and avoid a
serialization-format rewrite.

**Scope**: Replace gRPC on the **internal replica-to-replica hot path**
only. The management API (Axum HTTP) and client-facing surface can stay
on gRPC/HTTP until there is a separate reason to migrate them.

**Reference implementations to study before building**:
- **protosocket** (Momento) — tokio + prost, no HTTP/2, reported
  ~2.75× over gRPC. Closest architectural match (Rust, prost, raw TCP).
- **Volo** (CloudWeGo) — custom binary transport, 350k+ QPS reported.
- **Cap'n Proto RPC** — zero-copy serialization, promise pipelining;
  relevant for the zero-copy path (R30) if serialization format is ever
  revisited.

**Files** (expected, not started):
- New crate `crow-kv-rpc` (or similar) — framing, pool, reconnect.
- `crow-kv` consensus/RPC wiring — swap tonic client/server for the new
  transport on the internal hot path.
- `.proto` schemas — unchanged (prost reused).

**Acceptance**:
- A custom Rust RPC transport replaces gRPC on the internal
  replica-to-replica path with no contract change (same request/response
  semantics, same protobuf payloads).
- 2T:1C read throughput recovers the ~17% h2-lock loss; higher
  thread:connection ratios scale linearly with threads (no h2-style
  userspace funnel — only the cheap per-connection writer queue).
- Connection pooling, reconnect, timeout, cancellation, error
  propagation, backpressure, and TLS parity with the gRPC path is
  covered by tests.
- The management API and client-facing surface remain on gRPC/HTTP
  unless separately migrated.

**Note**: The full analysis (h2 lock mechanics, kernel coalescing, why
this is a design mismatch not a tuning problem) lives in
`doc/design/kv/kv-read-flow-analysis.md` ≈L546-622. This backlog item is the
trackable stub; the working doc is the rationale.
