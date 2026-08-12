<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan: Coding Style & Module Layout Refactor

Status: **DRAFT — rules pending review.** Section 1 is the proposed rule
set for the user to review and finalize. Section 2 (the actual refactor
tasks) will be filled in after the rules are locked.

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
- `lib/crow-tree/ffi/src/lib.rs` — 1848 lines (FFI, special — see §1.11)
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
  (`kv`, `rpc`, `wal`, `gc`, `ffi`, `cfg` if the codebase already uses
  it). No `mgr`, `svc`, `util` style shortenings — spell it out.
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
  `PxGroup`, `CrowkvClient`) — may live in `foo.rs` if small, or in a
  named submodule (`foo/wal_engine.rs`) and re-exported from `foo.rs`.
  Prefer the submodule + re-export once the impl exceeds ~150 lines.
- **Supporting types** (config structs, error enums, view types) —
  live with the code that owns them, or in a `foo/types.rs` if shared
  by ≥2 submodules and trivial.
- **Request/response DTOs** — live in the protocol crate
  (`crow-protocol`) when shared across crates; otherwise with the
  handler that owns them. Never duplicated across crates (this is the
  subject of the parallel `plan-protocol-consolidation.md` effort).
- **Traits** — live in `foo.rs` if they define the module's contract,
  or in a `foo/<contract>.rs` if the trait + its impls are large.

### 1.11 Special cases

- **`crow-tree-ffi`** — `unsafe_code = deny` is relaxed here, and the
  FFI surface is inherently one large translation unit. The 1848-line
  `lib.rs` is a known outlier; split by C++ header boundary
  (`tree`, `iterator`, `batch`, ...) into submodules, re-export from
  `lib.rs`. Not bound by the 1000-line hard cap, but should still be
  split for navigability.
- **Test kit (`tests/testkit/`)** — `mod.rs` may be retained in test
  code; the 2018-style rule applies to `src/` only.
- **Generated code** — exempt from size rules; must carry a
  `// Generated; do not edit by hand.` header.

### 1.12 Enforcement

- **Mechanical**: `cargo clippy -- -D warnings` already enforces
  `pedantic`. Add `clippy::too_many_lines` (threshold 150) and
  `clippy::items_after_statements` to the workspace lint set once the
  rules are approved.
- **Review-time**: the `/review` workflow gains a checklist item:
  "Does the changed file pass §1.4? Do new/changed functions pass
  §1.8? Was §1.5 considered for any file that grew? Does the file
  name pass §1.6? Is the file cohesive per §1.7?"
- **No new violations**: once approved, no PR may *add* a file to the
  >1000-line list or *add* a function to the >150-line list. Existing
  violations are worked down per §2.

---

## 2. Refactor Plan (to be filled in after rules are approved)

This section will list per-file refactor tasks once the rules in §1 are
finalized. Each task will be a checkbox with: file, target split, new
submodule names, dependency order, and the verification command.

Current state assessment (concrete numbers, measured 2026-08-12):

- 167 `.rs` source files across `lib/` and `app/` (excl. `tests/`).
- 52 files exceed 300 non-blank/comment lines.
- 21 files exceed 600 lines.
- 11 files exceed 1000 lines (listed in §1.4).
- 21 `mod.rs` files in `src/` (to be migrated to 2018 style).
- Longest function: 359 lines (`group.rs::run_accept_phase_impl`).

Proposed staging (subject to rule approval):

- [ ] **Stage A — Lock rules.** User reviews §1, edits, approves.
      Update `.devin/workflows/coding.md` with the finalized rules;
      add the lint set from §1.11.
- [ ] **Stage B — `mod.rs` → `foo.rs` migration.** Mechanical, one
      crate at a time. No logic changes. Verify with `cargo check` +
      `cargo clippy` per crate.
- [ ] **Stage C — Split the >1000-line files.** One commit per file
      (or per tightly-coupled pair). Order by dependency: leaf crates
      first (`crow-protocol`, `crow-common`), then `crow-kv`, then
      `crow-kv-client`, then apps.
- [ ] **Stage D — Split the >150-line functions.** Done as part of
      Stage C for the files already being split; otherwise as a
      follow-up pass per crate.
- [ ] **Stage E — Verify.** Full `cargo fmt --check`, `cargo clippy
      --all-targets -- -D warnings`, and the relevant test suites
      per the `/coding` workflow.

Per-file task list (fill in after Stage A):

- [ ] `lib/crow-kv/src/cluster/group.rs` (3477) →
      target submodules: _TBD_
- [ ] `lib/crow-kv/src/cluster/local_replica.rs` (1878) →
      target submodules: _TBD_
- [ ] `app/crow-web/src/mgmt.rs` (1870) →
      target submodules: _TBD_
- [ ] `lib/crow-tree/ffi/src/lib.rs` (1848) →
      target submodules: _TBD_
- [ ] `app/crow-kv-server/src/mgmt_api.rs` (1726) →
      target submodules: _TBD_
- [ ] `lib/crow-kv/src/cluster/group_election.rs` (1280) →
      target submodules: _TBD_
- [ ] `lib/crow-kv-client/src/client.rs` (1275) →
      target submodules: _TBD_
- [ ] `lib/crow-common/rust/src/metrics/mod.rs` (1263) →
      target submodules: _TBD_
- [ ] `app/crow-web/src/lifecycle.rs` (1136) →
      target submodules: _TBD_
- [ ] `app/crow-cli/src/bench/runner.rs` (1128) →
      target submodules: _TBD_

---

## 3. Open Questions for Review

These need the user's decision before Stage A can close.

1. **File size caps** — are the §1.4 thresholds (300 / 600 / 1000)
   right for this codebase, or should they be tighter / looser?
2. **Function length caps** — are the §1.8 thresholds (40 / 80 / 150)
   right? The longest current function is 359 lines; getting every
   function under 150 is a non-trivial amount of extraction work.
3. **`mod.rs` in test code** — §1.11 proposes keeping `mod.rs` in
   `tests/testkit/`. Confirm, or apply 2018 style there too?
4. **`crow-tree-ffi`** — §1.11 exempts it from the 1000-line hard cap.
   Confirm, or hold it to the same rules as pure-Rust crates?
5. **Lint enforcement** — §1.12 proposes adding `clippy::too_many_lines`
   (threshold 150) to the workspace lint set. This will fail the build
   on the current long functions until Stage D completes. Acceptable,
   or gate it behind `#[allow]` on a per-file basis until then?
6. **Headline-type placement** — §1.10 says headline types *may* live in
   `foo.rs` if small, else in a named submodule + re-export. Should we
   instead mandate "always in a named submodule, `foo.rs` is only
   docs + `pub mod` + `pub use`"? Stricter, but more uniform.
7. **Commit granularity** — §2 proposes one commit per >1000-line file
   split. The `group.rs` split (3477 lines) may itself need to be
   several commits to stay reviewable. Confirm the one-commit-per-file
   default with an escape hatch for the largest files?
8. **Ordering vs. the parallel protocol-consolidation effort**
   (`plan-protocol-consolidation.md`) — that effort moves types between
   crates and will touch many of the same files. Should this refactor
   go first, go second, or be interleaved? My recommendation: land
   protocol-consolidation first (it changes *what* is in each file),
   then do this refactor (which changes *how* files are split).
9. **File naming — allowed abbreviations** — §1.6 permits
   `kv`, `rpc`, `wal`, `gc`, `ffi`, `cfg` as established abbreviations.
   Add or remove any? (e.g. allow `mgmt`? `px` for Paxos? `cli`?)
10. **File naming — the `mgmt.rs` / `mgmt_api.rs` split** — the
    codebase has both `app/crow-web/src/mgmt.rs` (console-side cluster
    management handlers, 1870 lines) and
    `app/crow-kv-server/src/mgmt_api.rs` (server-side management API,
    1726 lines). Both are "management" but in different binaries. Are
    these names acceptable, or should they be renamed to reflect their
    actual surface (e.g. `cluster_ops.rs` / `server_admin.rs`)?
11. **Cohesion — read/write path split** — §1.7 permits splitting a
    large subsystem by read/write path (`<sub>_read.rs` /
    `<sub>_write.rs`) but warns against over-fragmentation. Should
    `group.rs` (3477 lines) be split this way, or by phase
    (`group_prepare.rs` / `group_accept.rs` / `group_apply.rs`)?
12. **Cohesion — handler grouping** — §1.7 says handlers group by
    resource. `mgmt.rs` currently mixes cluster-init, replica-add, and
    topology-refresh. Confirm the split should be by resource family
    (`cluster_init.rs`, `replica_ops.rs`, `topology_refresh.rs`), or
    by a different axis?
13. **The `types.rs` ban** — §1.6 bans `types.rs` as too vague. The
    protocol-consolidation effort may introduce shared type files in
    `crow-protocol`. Confirm those should be named by subject
    (`mgmt.rs`, `status.rs`) rather than `types.rs`?
