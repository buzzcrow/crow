<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW

Distributed KV store: Paxos consensus, per-key slots, WAL durability, crow-tree storage engine.
Rust workspace + C++ storage engine (via FFI).

## Crates

- **`crow-kv`** — core lib: consensus, engine, WAL, I/O, RPC, reconfiguration.
- **`crow-kv-client`** — client library (retry, topology cache, `NotLeaderHint`) + group-0 sysdata service classes (`HardwareClient`, `ServiceRegistryClient`, `KVClusterMetaClient`, `KVClusterAdmin`).
- **`crow-kv-server`** — binary: CLI, HTTP management API (internal — only called by `crow-kv-client`), store/group/replica wiring, keep-alive loop.
- **`crow-diskdb`** — binary: distributed disk-block allocator (sync loop, status management, gRPC service stubs; allocation logic is R72).
- **`crow-console-shared`** / **`crow-web`** / **`crow-cli`** — management console (shared core lib, Axum+React web, `clap` CLI); general cluster-management surface, not limited to CROW.
- **`lib/crow-tree/ffi`** — Rust FFI bindings to C++ crow-tree storage engine.

## Hard Constraints

- All build/test/format commands run under **pixi** — never bare `cargo` or `clang-format`.
- `unsafe_code = deny` (except `crow-tree-ffi`); Clippy `pedantic = warn`.
- Markdown is read as raw text — prefer bullet or definition lists; tables allowed only when genuinely necessary for data/metric comparison (e.g. benchmark results). `doc_index.md` always uses tables.
- `test-util` auto-enabled for tests via self dev-dependency — no flags needed.
- Commit messages: single-line subject only — no body, no trailers (e.g. `Co-Authored-By`, `Generated with`), no doc references, no task numbers (R-numbers). Code comments: single line, no doc references or task numbers.
- **One commit per task** — a "task" is a coherent unit of work (e.g. "restructure docs", "add CLI rename", "implement R7"). Small, closely-related changes may be merged into one commit. For continuous interactive changes, accumulate and commit only when asked. Before pushing, squash unpushed commits from the same task into one (soft reset to remote tip, re-commit). Before committing, verify no temp/generated files are staged; add to `.gitignore` if needed.
- **Pre-commit quality gate — do not skip:**
  - Lint must pass: `cargo fmt --check`, `cargo clippy -- -D warnings`, `clang-format --dry-run --Werror` (changed `.cpp`/`.h`), `tree-lint` (clang-tidy, changed C++). Fix up to 3 times — always, regardless of cause.
  - Tests: run only relevant tests (Rust or `test-tree-ct`), not the entire suite. Fix up to 3 times; skip pre-existing failures with a stated reason.
- **Playwright E2E uses the system browser** — `app/crow-web/ui/playwright.config.ts` auto-detects Chromium/Edge/Chrome via its `localBrowsers` list; never run `npx playwright install` locally. The CI workflow's install step is conditional (downloads only when no system browser is found). Override with `PLAYWRIGHT_CHANNEL` or `PLAYWRIGHT_CHROMIUM_EXECUTABLE`.
- **Shell commands — short timeout, full output, no filtering:**
  - Default `timeout` to `60000` ms (60s); only raise it when explicitly told a command is long-running.
  - For anything potentially long-running or hang-prone, use `timeout: 0` (background) + `get_output` polling instead of a long blocking wait.
  - **Do not pipe output through `grep`/`head`/`tail` or redirect `2>&1 | grep error`** — run commands raw so the full stdout/stderr is captured. Only filter when the output is known to be huge (e.g. build logs) and you've said so in your response.
  - After every shell command, paste the full output (or a clearly marked truncation with the first + last N lines) in your response — never just "it passed" or "no errors." The user needs to see the actual output to judge whether a command is hung.

## Dispatch — Read Before Acting

| Action | Read first |
| --- | --- |
| Write/modify code | `/coding` workflow (conventions, doc-first) |
| Design or architecture question | `doc/doc_index.md` → match row → open only that doc under `doc/design/{kv,tree,console}/`, grep for `##` section |
| Write/modify docs | `/doc` workflow (hierarchy, naming, formatting rules) |
| Commit changes | Hard Constraints above — no extra doc needed |
| Debug a test failure | `/debug-test` workflow (env check, log-first, data-first, add missing logs) |
| Pre-push review | `/review` workflow (checklist, hot-path rules, clippy exceptions) |
| Implement a new-requirements item | `doc/backlog/backlog.md` (index) → open the matched `R**-<component>-<topic>.md` → `/implement-requirement` workflow (lifecycle: understand → design → plan → implement → commit → merge → cleanup) |
| User guide / operations | `doc/user-manual/user-guide.md` (quick start, KV ops, cluster management, upgrade, API reference) |
