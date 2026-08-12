<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan: Coding Style & Module Layout Refactor

Status: **Rules locked (§1 finalized, §3 decisions recorded).** Section 2
is the per-module refactor task list, staged for execution.

---

## 1. Proposed Coding Rules (for review)

### 1.1 References

These rules are grounded in the canonical Rust style sources. We adopt
their naming/ casing conventions wholesale and layer CROW-specific
structure rules on top.

- **Rust API Guidelines** — <https://rust-lang.github.io/api-guidelines/>
  - Naming (C-CASE, C-CONV, C-GETTER, C-WORD-ORDER, C-FEATURE)
  - Interoperability, documentation, type conversions
- **The Rust Style Guide** — <https://doc.rust-lang.org/style-guide/>
  - Casing, reserved-word handling, "avoid `#[path]`"
- **RFC 430** — finalizing naming conventions (the basis of C-CASE)
- **Clippy `pedantic`** — already `warn` in this repo; the lint set is
  the mechanical floor, this doc is the design ceiling.
- **Rust 2018 module system** — `foo.rs` + `foo/` over `foo/mod.rs`.

### 1.2 Module layout — Rust 2018 style

Adopt the **`foo.rs` + `foo/`** layout for every multi-file module.
`mod.rs` is permitted only as a transitional artifact during the
refactor; the end state has none in non-test code.

```
src/
  foo.rs            ← module root
  foo/
    bar.rs          ← submodule
    baz.rs          ← submodule
```

`foo.rs` is the module's **front door**. It must contain only:

- Module-level docs (`//! ...`) — what the module is, invariants,
  links to design docs.
- Submodule declarations (`pub mod bar;`, `mod baz;`).
- Re-exports (`pub use ...`) — the module's public surface, gathered
  from submodules so callers write `crow_kv::cluster::Group`, not
  `crow_kv::cluster::group::Group`.
- The module's **headline types/traits** — the central abstraction the
  module is "about" (see §1.3 for the test).
- Module-wide constants/enums/error types shared by every submodule
  (the module's *vocabulary*).

`foo.rs` must **not** contain:

- Implementation logic that belongs to a named submodule.
- Small utility functions (push them into the submodule that uses
  them, or a `util.rs` if genuinely shared by ≥2 submodules).
- One submodule's private helpers.
- Inline `#[cfg(test)] mod tests` (tests live in `tests/` per the
  existing `/coding` workflow).

### 1.3 The "front door" test

> If a new contributor opens only `foo.rs`, can they understand what
> the module **is** and where to look next — without wading through
> helpers or implementation?

If yes, the content is right. If they have to scroll past utility
functions or one submodule's internals to find the headline type, the
file is mis-organized.

Rule of thumb:

- `foo.rs` = **"what this module is"** (types, traits, docs, re-exports).
- `foo/*.rs` = **"how it works"** (implementation).

### 1.4 File size thresholds

Measured in non-blank, non-comment lines (use `pixi run cargo fmt
--check` then `awk` to count; raw `wc -l` is a rough proxy).

- **≤ 300 lines** — healthy. No action.
- **301–600 lines** — acceptable if the file has a single clear
  responsibility. Review at the next touch; split if §1.5 triggers.
- **601–1000 lines** — smell. Must have a documented justification
  (e.g. a single state machine whose states cannot be cleanly
  separated) or be split.
- **> 1000 lines** — must be split. No new code may be added to a
  file in this range; any change must first extract a submodule.

Current codebase outliers (must be addressed in §2):

- `lib/crow-kv/src/cluster/group.rs` — 3477 lines
- `lib/crow-kv/src/cluster/local_replica.rs` — 1878 lines
- `app/crow-web/src/mgmt.rs` — 1870 lines
- `lib/crow-tree/ffi/src/lib.rs` — 1848 lines (FFI, special — see §1.12)
- `app/crow-kv-server/src/mgmt_api.rs` — 1726 lines
- `lib/crow-kv/src/cluster/group_election.rs` — 1280 lines
- `lib/crow-kv-client/src/client.rs` — 1275 lines
- `lib/crow-common/rust/src/metrics/mod.rs` — 1263 lines (a `mod.rs`!)
- `app/crow-web/src/lifecycle.rs` — 1136 lines
- `app/crow-cli/src/bench/runner.rs` — 1128 lines

### 1.5 When to refactor into a new file

Split a file into a submodule when **any** of these holds:

- It crosses a size threshold in §1.4 with no documented justification.
- It contains ≥2 distinct responsibilities (e.g. a type's definition
  *and* its gRPC service impl *and* its HTTP handlers).
- A reader needs to scroll past unrelated code to reach the part they
  care about.
- A coherent unit (a state machine, a codec, a background task) can be
  named in one or two words — that name becomes the new file.
- Two functions share state/imports that the rest of the file does not.
- The file fails the cohesion test in §1.7 (functions that don't
  belong together are co-located).

Splitting procedure:

1. Name the new submodule per §1.6 (one or two words, `snake_case`).
2. Move the cohesive block into `foo/<name>.rs`.
3. In `foo.rs`, add `pub mod <name>;` (or `mod <name>;` if internal)
   and a `pub use` if it's part of the public surface.
4. Re-run `cargo check` + `cargo Clippy` on the crate; fix imports.
5. Verify the front-door test still passes for `foo.rs`.

### 1.6 File naming patterns

A file name is a promise. A reader should be able to guess the file's
contents from the name alone, and be right.

**Naming rules:**

- `snake_case`, lowercase, no abbreviations except well-established ones
  (`kv`, `rpc`, `wal`, `gc`, `ffi`, `cfg`, `mgmt`, `px`, `cli`). No
  `mgr`, `svc`, `util` style shortenings — spell it out.
- One or two words. Three only if the third is a conventional suffix
  from the list below. Four words is always wrong.
- Name the *subject*, not the *kind*. `wal_engine.rs` (subject) is
  good; `engine_impl.rs` (kind) is bad — every file is an impl.
- Name what the file *is*, not what it *does*. `segment.rs` (a thing)
  is good; `append_records.rs` (an action) is a function name, not a
  file name. State machines and drivers are the exception: they are
  named after what they do (`election_driver`, `gc_worker`,
  `apply_loop`).
- Avoid `_helpers`, `_utils`, `_common`, `_misc`, `_stuff`. These are
  "I couldn't decide" names. If the contents are cohesive enough to
  extract, they have a real name. If they aren't, they belong with
  their callers. A `util.rs` may exist at a crate root for genuinely
  cross-cutting primitives (e.g. `crc32`), never inside a module.

**Conventional suffixes** (use these when they fit; do not invent new
ones without adding them here):

- `*_engine.rs` — the top-level type that drives a subsystem
  (`wal_engine`, `kv_engine`).
- `*_service.rs` — a gRPC/HTTP/RPC service impl
  (`px_service`, `kv_service`).
- `*_handler.rs` — a request handler layer (HTTP handlers, message
  dispatchers).
- `*_backend.rs` — a pluggable backend impl behind a trait
  (`file_backend`, `block_backend`).
- `*_worker.rs` / `*_loop.rs` — a background task / driver
  (`gc_worker`, `apply_loop`).
- `*_codec.rs` — encode/decode for a wire or disk format.
- `*_view.rs` / `*_status.rs` — read-only projections for reporting
  (`status`, `physical_view`).
- `*_config.rs` — configuration types and parsing for one subsystem.
- `*_error.rs` — error types for a subsystem (only if ≥2 error
  variants; one-variant errors stay with their type).
- `*_tests.rs` — inline tests (only where the `/coding` workflow
  permits inline tests; normally tests live in `tests/`).

**Anti-patterns to rename on sight:**

- `mod.rs` containing logic (see §1.2 — migrate to 2018 style).
- `types.rs` — too vague. Split by what the types are
  (`mgmt.rs`, `status.rs`, `error.rs`).
- `impl.rs` — every file is an impl. Name the subject.
- `core.rs` — meaningless. Name the actual responsibility.
- `misc.rs`, `common.rs` (inside a module) — see `_utils` rule above.
- `handler.rs` (singular, with no subject) — `kv_handler.rs` or group
  by surface into `kv.rs` / `mgmt.rs`.
- `utils.rs` inside a module — push helpers to their callers.

**Naming test:** show the file name to a contributor who hasn't seen
the codebase and ask "what's in this file?" If they can't answer, or
answer "various things," the name is wrong (or the file is
incohesive — see §1.7).

### 1.7 File cohesion — two-level grouping

Code is organized in a **two-level hierarchy**: group first, subject
second. This maps directly to the 2018 module layout — the group is
the module (`foo/`), the subject is a file within it (`foo/<subject>.rs`).

**Level 1 — Group by domain area (the module).**

The first organizing principle is the broad domain area: a subsystem
or feature that a contributor can name in one or two words. This
becomes the module directory.

```
src/
  cluster/       ← group: cluster management (membership, replicas, election)
  wal/           ← group: write-ahead log (segments, pipeline, replay, gc)
  paxos/         ← group: consensus core (acceptor, learner, roles)
  mgmt/          ← group: management API (cluster init, replica ops, topology)
```

A group is the right size to be a module when:

- Its contents share a common vocabulary (types, traits, config).
- A contributor would say "I'm working on the WAL" or "I'm working on
  cluster management" — not "I'm working on HTTP handlers."
- It can be described without naming a technical layer
  (not "all the async code", not "all the serializers").

**Level 2 — Split by subject when the group is too large.**

If a group's code exceeds the §1.4 file-size cap for a single file,
split it by **subject** — a specific thing within the domain area that
can be named in one or two words. Each subject becomes a file in the
module directory.

```
src/
  cluster/
    cluster.rs       ← module root: docs, pub mod, pub use
    group.rs         ← subject: the PxGroup type + its core impl
    group_election.rs← subject: leader election state machine
    replica.rs       ← subject: the Replica trait + handler
    local_replica.rs ← subject: local replica impl
    remote_replica.rs← subject: remote replica impl
    status.rs        ← subject: status/view types
  mgmt/
    mgmt.rs          ← module root: docs, pub mod, pub use
    cluster_init.rs  ← subject: cluster bootstrap / init handlers
    replica_ops.rs   ← subject: add/remove replica handlers
    topology.rs      ← subject: topology refresh + cache helpers
```

A subject is the right size to be a file when:

- It holds everything about one thing — the type, its impl, its
  handlers, its helpers — not just one layer of that thing.
- It can be named after the *subject* (`segment`, `replica`), not the
  *layer* (`handlers`, `services`, `async`).
- Removing the technical keywords (`async`, `axum`, `tonic`, `serde`)
  from its functions would still leave them belonging together.

**Never group by layer.** A file like `handlers.rs` containing all
HTTP handlers for rack, node, store, group, and replica is
layer-grouping — the only thing the functions share is `axum::extract`.
This produces god files where finding the one handler you need means
scrolling past unrelated resources. Always group by subject instead:
`rack.rs`, `node.rs`, `store.rs`.

**Cohesion rules (apply within each subject file):**

- **One responsibility per file.** A file named `segment.rs` holds the
  segment type, its append/seal/read methods, and nothing else. If a
  function doesn't operate on a segment, it doesn't belong here — even
  if it's small.
- **Group by shared state.** If a set of functions all operate on the
  same struct's private fields, they belong together in that struct's
  impl file. If they operate on different structs, they don't.
- **Group by shared imports.** If a cluster of functions pulls in the
  same set of types (e.g. all the `PxGroup` + `PxSlot` + `Ballot`
  imports), that's a cohesion signal. A function whose imports are
  disjoint from the rest of the file is a stranger.
- **Handlers group by resource, not by verb.** HTTP/gRPC handlers for
  the same resource (`/api/stores`, `/api/groups`) go in one file,
  even if there are eight of them. Splitting `add_store.rs` and
  `remove_store.rs` is over-fragmentation; the resource is the
  cohesion unit. Split only when a single resource's handlers exceed
  the §1.4 cap, and then split by sub-resource (`store_groups.rs`).
- **Types live with their impl.** A struct and its `impl` block belong
  in the same file. A `types.rs` that declares structs while their
  impls live elsewhere is a cohesion violation (and a navigation
  hazard). Exception: trait definitions may live in `foo.rs` while
  impls live in `foo/<impl_name>.rs`.

**Cohesion test (the "stranger" check):**

Open a file, pick any function at random, and ask:

1. Does this function operate on the same primary type as the others?
2. Does it share imports with the majority of the file?
3. Would a reader looking for this function expect to find it here?

If any answer is "no," the function is a stranger — move it to where
the answers become "yes," or to a new subject file named after its
real subject (§1.6).

**Anti-cohesion patterns to fix on sight:**

- A "god file" that holds a type, its impl, its background tasks, its
  error enum, and its config struct. Split: `<type>.rs` (the type +
  core impl), `<type>_worker.rs` (tasks), `<type>_config.rs`,
  `<type>_error.rs`.
- A file that mixes read-path and write-path logic for a subsystem
  when both paths are large. Split: `<subsystem>_read.rs` /
  `<subsystem>_write.rs`. (Only when both are large; small read/write
  helpers stay with the subsystem — don't over-fragment.)
- A handler file that mixes two unrelated resources
  (`mgmt.rs` holding both cluster-init and replica-add). Split by
  resource or by operation family.
- "Utility" functions that are really one subsystem's helpers living
  in a shared file. Move them to that subsystem's file.

### 1.8 Function length rules

Measured in non-blank, non-comment lines.

- **≤ 40 lines** — healthy.
- **41–80 lines** — acceptable for orchestrators (a function whose job
  is to sequence other functions). Avoid for logic-bearing functions.
- **81–150 lines** — smell. Must have a documented reason (a single
  match over a large enum, a codec with many cases) or be split.
- **> 150 lines** — must be split. No exceptions for new code.

When a function is too long, extract sub-functions by responsibility,
not by line count. A 200-line function split into four 50-line
functions that each do one thing is the goal; four 50-line functions
that are arbitrary slices of the original is not.

Current worst offenders (must be addressed in §2):

- `group.rs::run_accept_phase_impl` — 359 lines
- `group.rs::run_prepare_phase_impl` — 280 lines
- `mgmt.rs::http_cluster_init` — 228 lines
- `group_election.rs::run_bulk_phase1` — 204 lines
- `mgmt.rs::http_add_replica` — 206 lines
- `group.rs::propose_inner_impl` — 235 lines
- `local_replica.rs::initial` — 186 lines

### 1.9 When to create a function

Create a function when **any** of these holds:

- A block of code has a single, nameable responsibility.
- The same logic appears (or will appear) in ≥2 places.
- A comment like `// now we do X` introduces a section — that section
  is a function named `do_x`.
- A function's body has ≥3 levels of nesting that could be flattened by
  an early-return helper.
- A test needs to invoke the logic in isolation.

Do **not** create a function when:

- It would be called from exactly one place and the body is ≤5 lines —
  inline it (clippy `too_many_lines` is the mechanical signal).
- It exists only to share a trivial helper between two functions —
  prefer duplicating 2–3 lines over a premature abstraction.
- Its name would have to repeat the caller's context
  (`cluster_group_run_prepare_phase_impl_step2` is a smell, not a name).

### 1.10 Type and struct placement

- **Headline types** (the module's central abstraction: `WalEngine`,
  `PxGroup`, `CrowkvClient`) — **always** live in a named submodule
  (`foo/wal_engine.rs`) and are re-exported from `foo.rs` via
  `pub use`. Never defined directly in `foo.rs`. This keeps `foo.rs`
  a pure index (docs + `pub mod` + `pub use`) — every module root
  looks the same. A 20-line submodule file is fine; a 20-line type
  mixed into the module root is not.
- **Supporting types** (config structs, error enums, view types) —
  live with the code that owns them, or in a shared submodule if used
  by ≥2 submodules. Named by subject (`foo/config.rs`, `foo/error.rs`),
  never `foo/types.rs` (see §1.6 ban).
- **Request/response DTOs** — live in the protocol crate
  (`crow-protocol`) when shared across crates; otherwise with the
  handler that owns them. Never duplicated across crates (see
  `doc/design/protocol/design-crow-protocol-types.md`).
- **Traits** — live in a named submodule (`foo/<contract>.rs`),
  re-exported from `foo.rs`. Same rule as headline types: `foo.rs`
  is a pure index.

### 1.11 Visibility — `pub` review and test-only access

As we refactor module by module, review every `pub` item. The repo
currently has 648 `pub` items in `src/` versus 210 `pub(crate)` —
roughly 3:1. Many `pub` items are only needed by tests, not by other
crates. Each unnecessary `pub` widens the public API surface, making
future changes harder and hiding the real contract.

**Visibility hierarchy (use the narrowest that works):**

- **private** (no modifier) — default. Use when the item is only used
  within its own module.
- `pub(crate)` — visible to the entire crate, not to external crates.
  Use when the item is used by ≥2 modules *within the same crate* but
  not by other crates. This is the workhorse for internal APIs.
- `pub(super)` — visible to the parent module only. Use when an item
  is needed by its parent but not the whole crate.
- `pub(in path)` — visible within a specific module subtree. Rare;
  use when `pub(crate)` is too wide and `pub(super)` is too narrow.
- `pub` — visible to all crates. Reserve for the crate's actual
  public contract. Every `pub` item is a commitment.

**Review procedure (per module, during the refactor):**

1. List every `pub` item in the module.
2. For each, grep the entire workspace for external callers (outside
   the module).
3. If no caller outside the module → make it private.
4. If callers exist only within the same crate → `pub(crate)`.
5. If callers exist only in the parent module → `pub(super)`.
6. If callers exist in other crates → keep `pub`, but verify it's
   part of the intended contract, not an accident.
7. If the only external caller is a test → use the test-only pattern
   below, not `pub`.

**Test-only access — the `test-util` feature pattern:**

The repo already uses a `test-util` Cargo feature (in `crow-kv` and
`crow-kv-client`) to expose internals to tests without polluting the
production API. This is the preferred technique. The pattern:

1. Declare a `test-util = []` feature in the crate's `Cargo.toml`.
2. Add a self dev-dependency that auto-enables it for the crate's own
   tests:
   ```toml
   [dev-dependencies]
   crow-kv = { path = ".", features = ["test-util"] }
   ```
3. Gate test-only fields, methods, and impls with
   `#[cfg(feature = "test-util")]`:
   ```rust
   /// Test-only gate; `None` in production.
   #[cfg(feature = "test-util")]
   pub(crate) apply_gate: Mutex<Option<Arc<Notify>>>,
   ```
4. Name test-only setters with the `_for_tests` suffix:
   ```rust
   #[cfg(feature = "test-util")]
   pub fn set_apply_gate_for_tests(&self, notify: Arc<Notify>) { ... }
   ```
5. Other crates that need test access to these internals add
   `features = ["test-util"]` in their `[dev-dependencies]` entry:
   ```toml
   [dev-dependencies]
   crow-kv = { path = "../crow-kv", features = ["test-util"] }
   ```

This keeps the production binary clean (the feature is off, the code
is not compiled) while giving tests full access. It is strictly better
than `pub` for test-only access because:

- The item does not appear in the public API at all.
- No downstream user can accidentally depend on it.
- The production binary has zero overhead (the code is absent).

**When `pub(crate)` under `test-util` is not enough:**

Integration tests in `tests/` are external to the crate — they can
only see `pub` items. If an integration test needs access to a
`pub(crate)` item, two options:

- **Preferred: expose a test-only `pub` function** gated by
  `#[cfg(feature = "test-util")]`. It compiles away in production:
  ```rust
  #[cfg(feature = "test-util")]
  pub fn internal_thing_for_tests() -> InternalType { ... }
  ```
- **Alternative: move the test into the crate** as an inline
  `#[cfg(test)] mod tests` block. This is discouraged per the
  `/coding` workflow (integration tests only), but acceptable when
  the test genuinely needs private access and the `test-util` gate
  would be too heavy.

**Anti-patterns to fix on sight:**

- `pub fn` that is only called from `tests/` → gate behind
  `#[cfg(feature = "test-util")]` or make `pub(crate)` + use the
  self dev-dependency.
- `pub` struct fields that should be `pub(crate)` — fields are part
  of the API too; `pub` fields lock the layout.
- `pub` on an impl block for a `pub(crate)` type — the impl is
  reachable but the type isn't, which is confusing. Match visibility.
- `pub` on a helper function used by one other module in the same
  crate → `pub(crate)` or `pub(super)`.
- `#[cfg(test)]` on production code to expose internals — use
  `#[cfg(feature = "test-util")]` instead, so integration tests
  (which are separate compilations) can also access it.

### 1.12 Special cases

- **`crow-tree-ffi`** — `unsafe_code = deny` is relaxed here, but the
  1000-line cap still applies. The FFI bindings split cleanly by C++
  header boundary (`tree.rs`, `iterator.rs`, `batch.rs`) — each
  submodule can have its own `unsafe extern "C"` blocks. The `unsafe`
  nature doesn't prevent splitting. Split the 1848-line `lib.rs` into
  submodules by header boundary, re-export from `lib.rs`.
- **Test code (`tests/`)** — strict 2018 style, same as `src/`. No
  `mod.rs` anywhere. The `testkit/` directories are renamed to
  `common/` (the established Rust convention, used by tokio, hyper,
  reqwest).

  **2018 style in `tests/`:** `tests/common.rs` is the module root
  (`pub mod cluster; pub mod logging; ...`), `tests/common/cluster.rs`
  etc. are the helper files. Cargo compiles `common.rs` as a separate
  test binary with zero `#[test]` functions — this is harmless (it
  compiles fast, runs nothing, doesn't affect real test binaries).
  This is the tradeoff for strict 2018 consistency; it's worth it.

  **Naming convention — distinguish helpers from test cases:**

  - **Test case files:** `*_test.rs` suffix. Every file that contains
    `#[test]` or `#[tokio::test]` functions ends with `_test.rs`.
    Examples: `group_test.rs`, `wal_test.rs`, `election_test.rs`,
    `kv_correctness_test.rs`. This is already the dominant pattern in
    the codebase; the few exceptions (`election.rs`, `group.rs`,
    `kv.rs`, `cluster_cli.rs`, `lifecycle_cli.rs`,
    `bench_benchmark.rs`, `conformance.rs`, `mem_kv_impl.rs`) are
    renamed during the refactor.
  - **Test helper files:** live inside `common/`, named by subject
    without a prefix — `cluster.rs`, `logging.rs`, `net_lock.rs`.
    They are clearly helpers because they're in `common/`; no prefix
    needed.
  - **Test helper types:** `Test*` prefix — `TestCluster`,
    `TestNode`, `TestTimer`, `TestRouter`. This is already used;
    keep it.

  **Visual distinction at a glance:**
  ```
  tests/
    common.rs            ← helper module root (empty binary, harmless)
    common/
      cluster.rs         ← TestCluster builder
      logging.rs         ← init_test_subscriber
      net_lock.rs        ← unique_port
    group_test.rs        ← test cases
    wal_test.rs          ← test cases
    paxos/
      acceptor_test.rs   ← test cases
      learner_test.rs    ← test cases
  ```

  `*_test.rs` = has test cases. `common/` = helpers. No ambiguity.

  **Do not move test fixtures to `src/` under the `test-util` feature.**
  The `test-util` feature is for **production type hooks** — gates,
  setters, internal field exposure that lets tests manipulate
  production types (`set_apply_gate_for_tests`,
  `set_persist_gate_for_tests`). These must be in `src/` because they
  access private fields. Test fixtures (`TestCluster`,
  `init_test_subscriber`, `unique_port`) are **test code that uses the
  public API + hooks** — they belong in `tests/`, not `src/`. Mixing
  them into `src/` blurs the line between library code and test code,
  pollutes `cargo doc`, and risks accidental production use.

  **Cross-crate test sharing:** if a second crate's tests need helpers
  from another crate's `tests/common/`, do not put test fixtures in
  production `src/`. Instead, extract a dedicated `crow-test-support`
  workspace crate — clean separation, 2018 style, shareable, no
  feature flags on production crates. Not needed today (each crate's
  test helpers are self-contained), but the escape hatch if overlap
  emerges.
- **Generated code** — exempt from size rules; must carry a
  `// Generated; do not edit by hand.` header.

### 1.13 Enforcement

- **Mechanical** (workspace `Cargo.toml` `[workspace.lints.clippy]`):
  - `all = "deny"`, `pedantic = "warn"` — pre-existing.
  - `mod_module_files = "warn"` — enforces §1.2 (no `mod.rs` in `src/`).
    14 current violations, worked down per §2 Stage B.
  - `too_many_lines = "warn"` — enforces §1.8. Uses clippy's default
    threshold of 100 (no `clippy.toml`). 36 existing `#[allow]`
    suppressions in the codebase; these are removed as each function
    is split per §2 Stage D. Bump to `"deny"` per crate as splits land.
  - `items_after_statements = "warn"` — enforces statement-ordering
    hygiene. Already suppressed in generated code (`rpc/mod.rs`).
- **Review-time**: the `/review` workflow gains a checklist item:
  "Does the changed file pass §1.4? Do new/changed functions pass
  §1.8? Was §1.5 considered for any file that grew? Does the file
  name pass §1.6? Is the file cohesive per §1.7? Was §1.11 (visibility
  review) applied to every changed `pub` item?"
- **No new violations**: once approved, no PR may *add* a file to the
  >1000-line list or *add* a function to the >150-line list. Existing
  violations are worked down per §2.

---

## 2. Refactor Plan — Per-Module Tasks

All rules in §1 are locked. Each task below is one commit-sized unit.
Order: leaf crates first, then dependent crates. Within each crate,
module by module. Each task does: (1) `mod.rs` → `foo.rs` migration,
(2) file splitting if >1000 lines, (3) function splitting if >150
lines, (4) visibility review, (5) ID/wire-type compliance check —
all for the named scope.

Verify after each task: `pixi run cargo fmt --check`, `pixi run cargo
clippy -- -D warnings` (changed crate only), relevant tests.

### Stage 0 — Lock rules

- [x] **0.1** Update `.devin/workflows/coding.md` and `review.md`
      with the finalized rules (done).
- [x] **0.2** Add `clippy::mod_module_files`, `clippy::too_many_lines`,
      and `clippy::items_after_statements` to workspace `Cargo.toml`
      `[workspace.lints.clippy]` as `"warn"` (done). No `clippy.toml` —
      `too_many_lines` uses default threshold 100. Existing 36
      `#[allow]` suppressions remain until §2 split tasks remove them.
      Bump each lint to `"deny"` per crate as violations are worked down.

### Stage 1 — Leaf crates (no internal dependencies)

#### crow-protocol (2701 lines, 11 files)

- [ ] **1.1** `key/` module — migrate `key/mod.rs` → `key.rs`.
      Rename `key/tests.rs` → `key/key_tests.rs`. Visibility review
      on all `pub` items in `key.rs`, `key/common.rs`, `key/diskdb.rs`
      (667 lines, under cap), `key/kv_cluster.rs`. Split `key.rs` if
      the root content exceeds 300 lines after migration (currently
      297 — borderline).
- [ ] **1.2** Flat files visibility review — `mgmt.rs` (430),
      `common_type.rs`, `sysdata.rs`, `bitmap.rs`,
      `diskdb_type_util.rs`, `lib.rs`. Review every `pub` item, narrow
      to `pub(crate)` where possible. Add `test-util` feature to
      `Cargo.toml` if any test-only `pub` items exist.

#### crow-common (2957 lines, 11 files)

- [ ] **1.3** `metrics/` module — migrate `metrics/mod.rs` (1263
      lines!) → `metrics.rs` (pure index) + split content into
      subject files: `metrics/registry.rs`, `metrics/layer.rs`, etc.
      The 1263-line file is the largest non-`group.rs` split. Analyze
      content first, then split by subject. Each extracted submodule
      = one sub-task if the total exceeds 500 lines of extraction.
- [ ] **1.4** Flat files — `logging.rs` (302), `report.rs`, `time.rs`,
      `lib.rs`. Visibility review.

#### crow-tree-ffi (1848 lines, 1 file)

- [ ] **1.5** Split `lib.rs` (1848) by C++ header boundary:
      `tree.rs`, `iterator.rs`, `batch.rs`, `snapshot.rs`, etc.
      Analyze `#include` boundaries in the C++ headers first to
      determine the split points. `lib.rs` becomes a pure index
      (`pub mod tree; pub use tree::*; ...`). May need 2 commits if
      the split is complex (one for extraction, one for cleanup).

### Stage 2 — crow-kv (19954 lines, 57 files) — largest crate

#### cluster/ module (14 files, ~13000 lines)

- [ ] **2.1** Migrate `cluster/mod.rs` → `cluster.rs` (pure index).
      Visibility review on the re-exports.
- [ ] **2.2** Split `cluster/group.rs` (3477) — extract
      `cluster/group_prepare.rs` (prepare phase, ~280-line function
      + supporting code). One commit, file must compile after.
- [ ] **2.3** Split `cluster/group.rs` — extract
      `cluster/group_accept.rs` (accept phase, ~359-line function
      + supporting code). One commit.
- [ ] **2.4** Split `cluster/group.rs` — extract
      `cluster/group_fetchgap.rs` (fetch-gap driver, ~131 lines
      + supporting code). One commit.
- [ ] **2.5** Split `cluster/group.rs` — extract
      `cluster/group_maintenance_impl.rs` or further splits if the
      remaining `group.rs` still exceeds 1000 lines. Review what's
      left (propose, coalesce, core state). One commit.
- [ ] **2.6** Split `cluster/local_replica.rs` (1878) — analyze
      content, split by subject (e.g. `local_replica_apply.rs`,
      `local_replica_heartbeat.rs`, `local_replica_replay.rs`).
      One commit per extraction, file must compile after each.
- [ ] **2.7** Split `cluster/group_election.rs` (1280) — split by
      election phase/role: `group_election_candidate.rs`,
      `group_election_leader.rs`, `group_election_driver.rs`.
      One commit per extraction.
- [ ] **2.8** Split `cluster/px_kv_store.rs` (1059) — analyze
      content, split by subject (e.g. `px_kv_store_get.rs`,
      `px_kv_store_scan.rs`, or by lifecycle). One commit.
- [ ] **2.9** Visibility review on remaining cluster files:
      `group_config.rs`, `group_maintenance.rs`, `kv_server.rs`,
      `kv_store.rs`, `learner_stream.rs`, `node_config.rs`,
      `remote_replica.rs` (741), `replica.rs`, `status.rs`.
      Narrow `pub` → `pub(crate)` where possible. One commit.

#### wal/ module (13 files, ~6000 lines)

- [ ] **2.10** Migrate `wal/mod.rs` → `wal.rs` (pure index, already
      46 lines — clean). Visibility review on re-exports.
- [ ] **2.11** Visibility + function-length review on large WAL
      files: `segment.rs` (766), `block_backend.rs` (682),
      `record.rs` (584), `pipeline_writer.rs` (533), `wal_engine.rs`
      (463). Split any functions >150 lines. One commit.
- [ ] **2.12** Visibility review on remaining WAL files:
      `file_backend.rs`, `gc.rs`, `index.rs`, `io_backend.rs`,
      `pipeline.rs`, `pipeline_backend.rs`, `replay.rs`,
      `wal_file.rs`. One commit.

#### paxos/ module (7 files, ~2500 lines)

- [ ] **2.13** Migrate `paxos/mod.rs` → `paxos.rs` (pure index, 26
      lines — clean). Visibility review on all paxos files:
      `acceptor.rs`, `error.rs`, `learner.rs` (564), `roles.rs`,
      `slot_list.rs` (598), `slot_node.rs`. Split any functions
      >150 lines. One commit.

#### rpc/ module (5 files, ~2200 lines)

- [ ] **2.14** Migrate `rpc/mod.rs` → `rpc.rs` (pure index, 41
      lines — clean). Visibility review on `px_service.rs` (805),
      `kv_service.rs` (705), `kv_response.rs`,
      `snapshot_service.rs`. Split any functions >150 lines.
      One commit.

#### common/ module (5 files, ~1500 lines)

- [ ] **2.15** Migrate `common/mod.rs` → `common.rs` (pure index,
      25 lines — clean). Visibility review on `config.rs` (562),
      `logging.rs`, `metrics.rs`, `report.rs`, `time.rs`. One
      commit.

#### io/ module (2 files, ~small)

- [ ] **2.16** Migrate `io/mod.rs` → `io.rs` (24 lines — clean).
      Visibility review on `async_file.rs`. One commit.

#### kv/ module (5 files, ~1300 lines)

- [ ] **2.17** Migrate `kv/mod.rs` → `kv.rs` (pure index, 31 lines
      — clean). Visibility review on `crow_tree_engine.rs` (405),
      `kv_engine.rs` (239), `kv_future.rs`, `op.rs`. One commit.

#### metrics/ module (1 file)

- [ ] **2.18** Migrate `metrics/mod.rs` → `metrics.rs` (29 lines
      — clean). Visibility review. One commit.

### Stage 3 — crow-kv-client (3750 lines, 10 files)

- [ ] **3.1** Split `client.rs` (1275) — analyze content, split by
      subject (e.g. `client_kv.rs`, `client_admin.rs`,
      `client_retry.rs`). One commit per extraction.
- [ ] **3.2** Visibility review on remaining files: `hardware.rs`
      (643), `kv_cluster.rs` (553), `metrics.rs` (431),
      `service_registry.rs` (303), `topology.rs` (259), `config.rs`,
      `pool.rs`, `error.rs`, `lib.rs`. Add `test-util` feature if
      needed. One commit.

### Stage 4 — crow-console-shared (5110 lines, 18 files)

- [ ] **4.1** Migrate `clients/mod.rs` → `clients.rs` (19 lines —
      clean). Visibility review on `clients/console.rs` (693),
      `clients/http.rs` (188). One commit.
- [ ] **4.2** Visibility + ID-type review on `config.rs` (927) —
      check for `String` ID fields, widen to numeric aliases. Split
      if any functions >150 lines. One commit.
- [ ] **4.3** Visibility review on `monitor.rs` (622), `lifecycle.rs`
      (490), `ssh.rs` (473), `ssh/known_hosts.rs` (197). Migrate
      `ssh/` to 2018 style if it uses `mod.rs` (it doesn't — `ssh.rs`
      + `ssh/known_hosts.rs` is already 2018 style). One commit.
- [ ] **4.4** Visibility + ID-type review on `cluster.rs` (341) —
      verify `StoreView`/`GroupView`/`ReplicaView` are console-
      internal types (not wire-type duplicates — confirmed in
      compliance review). Check for `String` ID fields. One commit.
- [ ] **4.5** Visibility review on remaining files: `expand.rs`
      (316), `mgmt.rs` (259), `ops_log.rs` (244), `corr_id.rs`,
      `topology.rs`, `snapshot.rs`, `error.rs`, `test_ports.rs`,
      `lib.rs`. Add `test-util` feature. One commit.

### Stage 5 — crow-diskdb-client (21 lines, 1 file)

- [ ] **5.1** Visibility review on `lib.rs`. Trivial. One commit
      (or merge with Stage 7).

### Stage 6 — crow-kv-server (3450 lines, 10 files)

- [ ] **6.1** Rename `mgmt_api.rs` → `mgmt.rs` (per Q10 decision).
      Split `mgmt.rs` (1725) by subject: `mgmt/store_ops.rs`,
      `mgmt/group_ops.rs`, `mgmt/replica_ops.rs`,
      `mgmt/system_init.rs`, `mgmt/topology.rs`. `mgmt.rs` becomes
      the module root (pure index + shared helpers). One commit per
      extraction.
- [ ] **6.2** Visibility review on `main.rs` (464),
      `engine_collector.rs` (282), `operation_registry.rs` (203),
      `startup.rs` (201), `cli.rs` (195), `store_registry.rs` (129),
      `keepalive.rs` (125), `reconcile.rs` (108), `lib.rs`. One
      commit.

### Stage 7 — crow-web (4985 lines, 13 files)

- [ ] **7.1** Split `mgmt.rs` (1855) by resource family (per Q12):
      `mgmt/cluster_init.rs`, `mgmt/store_ops.rs`,
      `mgmt/group_ops.rs`, `mgmt/replica_ops.rs`,
      `mgmt/topology.rs`. `mgmt.rs` becomes module root (pure index
      + shared helpers: `mgmt_url_for_node`, `build_server_client`,
      `wait_for_new_leader`). One commit per extraction.
- [ ] **7.2** Split `lifecycle.rs` (1125) by subject: rack/node
      lifecycle vs. server deploy lifecycle. Analyze content first.
      One commit per extraction.
- [ ] **7.3** Fix protocol-types violations V1 (`physical_view.rs`
      `NodeView.id/rack_id: String` → `NodeId`/`RackId`) and V2/V3
      (`lifecycle.rs` `NodeQuery.rack_id: Option<String>` →
      `Option<RackId>`, remove `.parse().unwrap()`). One commit.
- [ ] **7.4** Visibility review on remaining files: `kv.rs` (470),
      `physical.rs` (280), `state.rs` (215), `lib.rs` (184),
      `expand.rs` (120), `spa.rs` (107), `main.rs`, `error.rs`,
      `corr_id.rs`, `health.rs`. Fix V8 (`state.rs`
      `runtime_pids: HashMap<String, u32>` → `HashMap<NodeId, u32>`).
      One commit.

### Stage 8 — crow-cli (5534 lines, 18 files)

- [ ] **8.1** Migrate `bench/mod.rs` → `bench.rs`,
      `commands/mod.rs` → `commands.rs`, `utils/mod.rs` →
      `utils.rs`. All are small index files. One commit.
- [ ] **8.2** Split `bench/runner.rs` (1128) — analyze content,
      split by benchmark type or phase. One commit per extraction.
- [ ] **8.3** Split `bench/report.rs` (988) — analyze content,
      split by report format/section. One commit.
- [ ] **8.4** Fix protocol-types violations V4
      (`commands/node.rs` `NodeAddArgs.id/rack_id: String` →
      `NodeId`/`RackId` via clap value parser), V5
      (`commands/rack.rs` `id.parse().unwrap()` → clap value
      parser), V6 (`bench/provision.rs` `node_ids` String parse →
      `Vec<NodeId>`). One commit.
- [ ] **8.5** Visibility review on remaining files:
      `commands/bench.rs` (642), `commands/kv.rs` (573),
      `bench/provision.rs` (513), `commands/cluster.rs` (325),
      `bench/workload.rs` (304), `commands/node.rs` (183),
      `commands/server.rs` (179), `main.rs` (152),
      `commands/paxos.rs` (144), `commands/store.rs` (133),
      `commands/rack.rs` (101), `commands/replica.rs` (89),
      `utils/client.rs`. One commit.

### Stage 9 — crow-diskdb (1023 lines, 9 files)

- [ ] **9.1** Migrate `grpc/mod.rs` → `grpc.rs`, `node/mod.rs` →
      `node.rs`, `status/mod.rs` → `status.rs`, `sync/mod.rs` →
      `sync.rs`. All small. Visibility review on all files. One
      commit.

### Stage 10 — Test code migration (all crates)

- [ ] **10.1** Rename all `tests/testkit/` → `tests/common/` across
      all crates. Migrate `common/mod.rs` → `common.rs` (2018
      style). One commit per crate (or one commit for all if
      mechanical).
- [ ] **10.2** Rename test case files without `_test.rs` suffix:
      `election.rs` → `election_test.rs`, `group.rs` →
      `group_test.rs`, `kv.rs` → `kv_test.rs`, `cluster_cli.rs` →
      `cluster_cli_test.rs`, `lifecycle_cli.rs` →
      `lifecycle_cli_test.rs`, `bench_benchmark.rs` →
      `bench_benchmark_test.rs`, `conformance.rs` →
      `conformance_test.rs`, `mem_kv_impl.rs` → `mem_kv_impl_test.rs`,
      `test_util.rs` → move to `common/`. One commit per crate.

### Stage 11 — Final verification

- [ ] **11.1** `pixi run cargo fmt --all -- --check`
- [ ] **11.2** `pixi run cargo clippy --all-targets -- -D warnings`
      (remove all remaining `#[allow(clippy::too_many_lines)]`)
- [ ] **11.3** All test suites, each separately:
      `pixi run clean-env && pixi run test-kv-core`
      `pixi run clean-env && pixi run test-kv-server`
      `pixi run clean-env && pixi run test-console-cli`
      `pixi run clean-env && pixi run test-console-server`
- [ ] **11.4** `find src tests -name mod.rs` — should be empty.
- [ ] **11.5** `find src -name 'types.rs' -o -name 'utils.rs'` —
      should be empty.
- [ ] **11.6** `grep -rn 'Path<String>\|Path\((String' src/` —
      should be empty.
- [ ] **11.7** `grep -rn '\.parse::<u64>().*unwrap' src/` — should
      be empty (excluding non-ID parses like `SocketAddr`).
- [ ] **11.8** Delete this plan doc (working doc, per `/doc`
      workflow). Update `doc_index.md` if any design docs
      referenced it.

### Protocol-Types Design Compliance Review

Audited against `doc/design/protocol/design-crow-protocol-types.md`
rules: §4.1 (single home), §4.2 (no `String` for numeric IDs),
§4.3 (re-export, never redefine), §6.3 (`Path<u64>`, not
`Path<String>`).

**Already compliant:**

- `lib/crow-console-shared/src/snapshot.rs` — re-exports all wire types
  from `crow_protocol::mgmt` via `pub use ... as ...` aliases. No
  local struct definitions for cross-boundary types. Compliant with
  §4.3.
- `lib/crow-protocol/src/common_type.rs` — all 7 ID aliases
  (`RackId`, `NodeId`, `DiskGroupId`, `StoreId`, `GroupId`,
  `ReplicaId`, `InstanceId`) defined as `pub type X = u64;`.
  Compliant with §4.2.
- `lib/crow-protocol/src/mgmt.rs` — all wire types defined once with
  `#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]`.
  Compliant with §4.1, §4.4.
- `app/crow-web/src/physical.rs` — `Path<u64>` / `Path<(u64, ...)>`
  for all ID parameters. Compliant with §6.3.
- `app/crow-web/src/kv.rs` — `NodeId` used throughout, no
  `Path<String>`. Compliant with §6.3.
- `app/crow-web/src/mgmt.rs` — `mgmt_url_for_node` takes `NodeId`,
  no `.parse().unwrap()`. Compliant with §6.3.
- `lib/crow-console-shared/src/cluster.rs` `StoreView`, `GroupView`,
  `ReplicaView` — these are **console-internal aggregation types**
  (cluster-wide logical views), not wire types. They are distinct
  from `crow_protocol::mgmt::StoreStatus`/`GroupStatus`/
  `ReplicaStatus` (per-node topology items). Not a violation of §4.1
  — they don't cross a crate boundary as wire types.

**Violations found — create tasks to fix:**

- [ ] **V1: `app/crow-web/src/physical_view.rs:55-56`** — `NodeView`
      fields `id: String` and `rack_id: String` use `String` for
      numeric IDs. Violates §4.2 ("String is not an ID type").
      Fix: widen to `NodeId` / `RackId`, serialize as numbers.
      May require frontend changes if the UI expects string IDs.
- [ ] **V2: `app/crow-web/src/lifecycle.rs:65`** — `NodeQuery.rack_id`
      is `Option<String>`. Violates §4.2. Query params should
      deserialize as `Option<RackId>` (serde supports deserializing
      `u64` from a query string).
      Fix: change to `Option<RackId>`, remove `.parse::<u64>().unwrap()`
      at line 154.
- [ ] **V3: `app/crow-web/src/lifecycle.rs:1108`** —
      `cfg.remove_rack(rid.parse().unwrap())` — `rid` is `String`,
      parsed to `u64` with `unwrap()`. Violates §4.2 + §6.3.
      Fix: widen `rid` to `RackId` at the source, remove the parse.
- [ ] **V4: `app/crow-cli/src/commands/node.rs:74-75`** —
      `NodeAddArgs.id: String`, `NodeAddArgs.rack_id: String`.
      Violates §4.2. CLI args should parse to `NodeId`/`RackId`
      via clap's value parser.
      Fix: use `clap::value_parser!(NodeId)` on the arg, store as
      `NodeId`/`RackId`.
- [ ] **V5: `app/crow-cli/src/commands/rack.rs:41`** —
      `id: id.parse().unwrap()` — `id` is `String`, parsed to `u64`.
      Violates §4.2.
      Fix: parse via clap value parser, store as `RackId`.
- [ ] **V6: `app/crow-cli/src/bench/provision.rs:264`** —
      `node_ids.iter().map(|n| n.parse().unwrap())` — `node_ids` are
      `String`, parsed to `u64`. Violates §4.2.
      Fix: parse via clap value parser, store as `Vec<NodeId>`.
- [ ] **V7: `lib/crow-console-shared/src/error.rs:12,15`** —
      `Error::NodeUnreachable { node_id: String, ... }` and
      `Error::UpstreamRpc { node_id: String, ... }` use `String` for
      `node_id`. Violates §4.2. Note: the design doc §4.2 exempts
      `UpstreamRpc.node_id` ("holds a URL, not a numeric ID") —
      verify whether `NodeUnreachable.node_id` is also a URL handle
      or a numeric ID. If numeric, widen to `NodeId`.
- [ ] **V8: `app/crow-web/src/lifecycle.rs:42`** —
      `state.runtime_pid(node_id.to_string())` — converts `NodeId`
      back to `String` for the `runtime_pids` map. The map is keyed
      by `String` (see `state.rs`). This is an internal implementation
      detail, not a wire boundary, but it's a symptom of the
      `runtime_pids: HashMap<String, u32>` using `String` keys for
      numeric IDs. Fix: widen the map to `HashMap<NodeId, u32>`,
      remove the `.to_string()` calls.

**Note:** V1–V6 are the same root issue — `String` used where a
numeric ID type belongs. The fix pattern is the same everywhere:
use `NodeId`/`RackId` at the boundary (clap arg, axum path, query
param), remove the `.parse().unwrap()` calls. V7 needs a design-doc
check first. V8 is an internal map key that should be widened.

---

## 3. Open Questions for Review

These need the user's decision before Stage A can close.

1. **File size caps** — **DECIDED: keep as-is (300 / 600 / 1000).**
   Statistics support the thresholds: 115 of 167 files are already
   ≤300 lines (healthy, 69%); 21 exceed 600 (smell, 13%); 11 exceed
   1000 (must split, 6.6%). The thresholds match the codebase's
   natural distribution.
2. **Function length caps** — **DECIDED: keep as-is (40 / 80 / 150).**
   The median function is well under 40 lines. 150 as the hard cap
   means only the 7 listed offenders need splitting — tractable.
3. **`mod.rs` in test code** — **DECIDED: strict 2018 style
   everywhere, including `tests/`.** No `mod.rs` anywhere. Rename
   `testkit/` to `common/`. Use `common.rs` + `common/` (2018 style).
   Cargo compiles `common.rs` as an empty test binary — harmless.
   Naming convention: test case files use `*_test.rs` suffix, test
   helper files live in `common/` named by subject, test helper types
   use `Test*` prefix. Test fixtures stay in `tests/`, not in `src/`
   under `test-util`. If cross-crate test sharing is needed later,
   extract a `crow-test-support` crate.
4. **`crow-tree-ffi`** — **DECIDED: no exemption.** Hold it to the
   same 1000-line cap. FFI bindings split cleanly by C++ header
   boundary (`tree.rs`, `iterator.rs`, `batch.rs`) — each submodule
   can have its own `unsafe extern "C"` blocks. The `unsafe` nature
   doesn't prevent splitting.
5. **Lint enforcement** — **DECIDED: add lints to workspace
   `[workspace.lints.clippy]` as `"warn"`, no `clippy.toml`.** Three
   lints added: `mod_module_files` (§1.2), `too_many_lines` (§1.8,
   default threshold 100), `items_after_statements`. Set to `"warn"`
   so existing violations don't break the build; bump to `"deny"` per
   crate as the §2 refactor works them down. 36 existing
   `#[allow(clippy::too_many_lines)]` suppressions are removed as each
   function is split. No new `#[allow]` may be added.
6. **Headline-type placement** — **DECIDED: always in a named
   submodule.** `foo.rs` is a pure index (docs + `pub mod` +
   `pub use`) — never contains type definitions. Headline types go in
   `foo/<type>.rs` and are re-exported. Every module root looks the
   same.
7. **Commit granularity** — **DECIDED: one commit per file split.
   For files >2000 lines, multiple commits — extract one submodule
   per commit, each leaving the file compilable.** Only `group.rs`
   (3477) qualifies for the escape hatch. Aligns with AGENTS.md "one
   commit per coherent unit."
8. **Ordering vs. the protocol-consolidation effort** — **DECIDED:
   protocol-consolidation landed first (commit `a251fce`), this
   refactor goes second.** The two efforts touched the same files;
   landing protocol-consolidation first (which changed *what* is in
   each file) before this refactor (which changes *how* files are
   split) avoided rework.
9. **File naming — allowed abbreviations** — **DECIDED: add `mgmt`,
   `px`, `cli`.** All three are pervasive in the codebase (`mgmt.rs`,
   `mgmt_url_for_node`, `PxGroup`, `px_service`, `px_kv_store`,
   `crow-cli`). `mgmt` is standard, `px` is the established Paxos
   prefix, `cli` is used by clap and the Rust ecosystem.
10. **File naming — the `mgmt.rs` / `mgmt_api.rs` split** — **DECIDED:
    keep `mgmt.rs` in crow-web, rename `mgmt_api.rs` to `mgmt.rs` in
    crow-kv-server.** They're in different crates — the crate name
    disambiguates (`crow_web::mgmt` vs `crow_kv_server::mgmt`). The
    `_api` suffix isn't in our conventional suffixes list (§1.6) —
    drop it. Both will be split by subject during the refactor.
11. **Cohesion — read/write path split** — **DECIDED: split `group.rs`
    by Paxos phase.** `group.rs` (struct + propose + core),
    `group_prepare.rs`, `group_accept.rs`, `group_fetchgap.rs`. The
    phases are distinct subjects with clear boundaries — more natural
    than read/write for Paxos. Use split `impl` blocks (idiomatic Rust
    — impl blocks for the same type can live in different files within
    the same crate, maintaining private field access).
12. **Cohesion — handler grouping** — **DECIDED: split `mgmt.rs` by
    resource/operation family.** `cluster_init.rs`, `store_ops.rs`,
    `group_ops.rs`, `replica_ops.rs`, `topology.rs`. The module root
    `mgmt.rs` keeps shared helpers (`mgmt_url_for_node`,
    `build_server_client`, `wait_for_new_leader`). Follows §1.7
    "handlers group by resource"; shared helpers stay in the root.
13. **The `types.rs` ban** — **DECIDED: yes, the ban applies
    everywhere, including `crow-protocol`.** Types are named by
    subject (`mgmt.rs`, `status.rs`, `sysdata.rs`, `common_type.rs`).
    The design doc `design-crow-protocol-types.md` already follows
    this pattern — no `types.rs` file.
14. **Visibility review scope** — **DECIDED: fix all.** Every `pub`
    item in `src/` across all crates will be reviewed and narrowed per
    §1.11, not just those in files being split. This is a workspace-
    wide pass, done module by module alongside the refactor.
15. **`test-util` feature rollout** — **DECIDED: all crates.** Roll
    out the `test-util` feature to every crate that has test-only
    `pub` items (`crow-console-shared`, `crow-protocol`,
    `crow-diskdb`, `crow-web`, `crow-cli`, etc.), following the
    5-step recipe in §1.11.
