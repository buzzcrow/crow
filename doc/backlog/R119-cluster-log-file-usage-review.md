<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R119: cluster — Log File Usage Review & Unification

**Problem**

CROWDB has two logging stacks — a Rust `tracing` stack
(`crowdb-common/rust/src/logging.rs`) and a C++ `spdlog` stack
(`crowdb-common/cpp/src/log.cpp` + `compressing_sink.cpp`) — and four
server binaries (`crowdb-kv-server`, `crowdb-diskdb`, `crowdb-chunkdb`,
`crowdb-web`) plus `crowdb-cli`. Only `crowdb-kv-server` wires up file
logging with rotation and compression; the other three servers
initialize console-only `tracing_subscriber::fmt().init()` and lose
every log line the moment they are daemonized or their stderr is
redirected to /dev/null. The C++ `crowdb-rpc` library ships a logging
C API (`crowdb_rpc_init_logging`) that no Rust caller ever invokes, so
its logs are never configured. There has never been an audit of
whether the log lines themselves are meaningful and self-explaining:
the project has log *infrastructure* but no reviewed log *content*.

**Current behavior + impact**

- **Rust logging infrastructure** —
  `crowdb-common/rust/src/logging.rs` provides `RotatingLogWriter`
  (size-based rotation, gzip compression of rotated files, default
  30 MiB per file, 5 rotated files kept). File naming:
  `{prefix}-{YYYYMMDD-HHMMSS.mmm}-{pid}.log`, rotated to `.log.gz`.
  `init_file_logging` / `init_file_and_console_logging` (and a
  `_split` variant separating file vs console levels) build a
  `tracing-subscriber` `fmt` layer (no ANSI for file, `with_target`
  + `with_thread_names`) over a `tracing-appender` non-blocking
  wrapper. `EnvFilter` from `RUST_LOG` with a per-project default
  fallback (`crowdb-kv/src/common/logging.rs::CROWDB_KV_DEFAULT_FILTER`).
  A separate `open_metrics_log` writes a metrics log with the same
  rotation scheme. Now used by `crowdb-kv-server`, `crowdb-diskdb`,
  `crowdb-chunkdb`, and `crowdb-web` — but only `crowdb-kv-server`
  exposes CLI flags (`--log`, `--log-max-file-mb`, `--log-max-files`)
  to tune it; the others hardcode the defaults.
- **C++ logging infrastructure** —
  `crowdb-common/cpp/src/log.cpp` + `log.h` provide an async spdlog
  logger over `compressing_file_sink_mt` (same size-rotation +
  gzip scheme, same `{prefix}-{ts}-{pid}.log` naming). Macros
  `CRB_LOG_*` (not `CR_LOG_*`) gate on a runtime `logging_enabled()`
  atomic. No-op when built without `CROWDB_HAVE_SPDLOG` (the Rust FFI
  `cc` build). `crowdb-tree-ffi::ct_init_logging`
  (`lib/crowdb-tree/ffi/src/tree.rs`) bridges it. `crowdb-rpc` has
  `crowdb_rpc_init_logging` / `crowdb_rpc_flush_logging` /
  `crowdb_rpc_shutdown_logging` in
  `lib/crowdb-rpc/include/crowdb-rpc/c_api.h` — a Rust FFI wrapper
  now exists (`lib/crowdb-rpc/ffi/src/logging.rs::init_logging`) and
  is called by `crowdb-kv-server` and `crowdb-web` at startup. But
  `crowdb-diskdb` and `crowdb-chunkdb` do NOT call it, so their
  crowdb-rpc C++ transport logs are still silent.
- **Per-server logging setup (the remaining inconsistencies)**:
  - `crowdb-kv-server` (`app/crowdb-kv-server/src/main.rs` lines
    34-74) — full file logging: Rust
    `init_file_and_console_logging_split` to dir `"log"`, prefix
    `"crowdb-kv-server"`; C++ crowdb-tree via
    `ct_init_logging("log", "info", ...)` prefix
    `"crowdb-kv-server-tree"`; C++ crowdb-rpc via
    `crowdb_rpc_ffi::init_logging("log", "info", ...)` prefix
    `"crowdb-kv-server-rpc"`; metrics log via `MetricsRunner` prefix
    `"crowdb-kv-server-metrics"`. CLI flags `--log-max-file-mb`
    (default 30), `--log-max-files` (default 5), `--log` (console
    toggle, short `-l`), `--metrics-interval` (default 5s, 0
    disables). Remaining gap: the C++ level (both tree and rpc) is
    hardcoded to `"info"` — not driven by `RUST_LOG` or any CLI
    flag; the log dir is hardcoded `"log"` (no `--log-dir` flag);
    `CrowDBConfig.log_dir` (config.rs:515, default `"log"`, set to
    `root/log` by `apply_root`) is computed but never used by the
    logging init calls.
  - `crowdb-diskdb` (`app/crowdb-diskdb/src/main.rs` lines 72-80) —
    file logging is wired via
    `init_file_and_console_logging_split("log", "crowdb-diskdb",
    30, 5, "info", "warn")`. Remaining gaps: no `--log` /
    `--log-dir` / `--log-max-file-mb` / `--log-max-files` CLI flags
    (defaults hardcoded); no metrics log; C++ crowdb-rpc logging
    NOT initialized (transport layer silent); log dir hardcoded
    `"log"`.
  - `crowdb-chunkdb` (`app/crowdb-chunkdb/src/main.rs` lines 61-69)
    — file logging is wired, same shape as diskdb. Same
    remaining gaps: no CLI flags, no crowdb-rpc logging, log dir
    hardcoded.
  - `crowdb-web` (`app/crowdb-web/src/main.rs` lines 37-59) — file
    logging is wired via `init_file_and_console_logging_split`
    to `~/.crowdb-kv/log`, prefix `"console-web"`. C++ crowdb-rpc
    logging IS initialized (`crowdb_rpc_ffi::init_logging("log",
    "info", 30, 5, "crowdb-web-rpc")` + `add_log_stderr("warn")`)
    — BUT the rpc init passes the literal `"log"` (relative to
    CWD), not the `~/.crowdb-kv/log` dir used by the Rust tracing
    init. This is a bug: rpc logs go to a different directory than
    Rust logs. The former `ops_log` module has been deleted; ops
    lines are now folded into the normal tracing log via
    `log_ops_http` in `crowdb-console-shared/src/clients.rs`.
    Remaining gaps: no CLI flags for log tuning; rpc log dir
    mismatch; log dir hardcoded to `~/.crowdb-kv/log` (different
    from the `"log"` used by kv-server/diskdb/chunkdb).
  - `crowdb-cli` (`app/crowdb-cli/src/main.rs` lines 173-200) —
    file logging IS wired: creates a per-invocation directory
    `cli-log/{command-slug}-{timestamp}/` and writes
    `crowdb-cli-*.log` there via `init_file_logging` (50 MiB / 5
    files, filter `"warn,crowdb_cli=info,..."`). C++ crowdb-rpc
    logging IS initialized with the same invocation dir. The
    `bench` subcommand additionally opens a metrics log via
    `BenchMetrics::new` (`open_named_log` with prefix
    `"crowdb-cli-metrics"`). The former `ops_log` module is gone;
    ops lines are folded into the tracing log via `log_ops_http`.
    `--log-root` CLI flag (env `CROWDB_LOG_ROOT`) controls the
    root; default is `cli-log/` under CWD. Remaining gap: D5
    (console-only except bench) is NOT the current behavior —
    every command writes a file log, not just bench.
- **Log directory inconsistency** — `crowdb-kv-server`,
  `crowdb-diskdb`, and `crowdb-chunkdb` all write to `"log"`
  (relative to CWD); `crowdb-web` writes to `~/.crowdb-kv/log`
  (Rust) but `"log"` (C++ rpc — bug); `crowdb-cli` writes to
  `cli-log/{command}-{ts}/`; the test harness
  (`crowdb-test-harness/src/{diskdb,chunkdb,diskio}.rs`) redirects
  child stdout/stderr to `<workspace_root>/test-logs/crowdb-*-e2e-
  {pid}.log` (no rotation, single file). An operator cannot point
  all servers at one log root.
- **Log format inconsistency** — Rust tracing `fmt` layer emits
  target + thread names, no ANSI on file, ANSI on console; the
  in-line timestamp is tracing's own. C++ spdlog emits
  `%Y%m%d-%H%M%S.%e [@] [%l] [%n] %v` (UTC, custom thread-name
  flag). The two stacks cannot be correlated by a shared
  timestamp format or field layout.
- **C++ logger additivity** — the `crowdb-rpc` C++ logging bridge
  (`crowdb_rpc_init_logging` in `c_api.cpp:951-986`) checks
  `logger_initialized()`: if `ct_init_logging` already ran (as in
  kv-server), it calls `add_log_file` (adds a second file sink to
  the same logger) and **ignores the `level` parameter** — the rpc
  sink inherits the tree logger's level. This means the C++ rpc
  level cannot be independently tuned on kv-server; it is always
  the tree logger's level. If `ct_init_logging` has NOT run (as in
  web/cli), `crowdb_rpc_init_logging` creates a fresh logger with
  the given level.
- **Log content quality** — no audit has been done of whether
  individual log lines are meaningful and self-explaining. Some
  call sites log raw error strings with no context; some log at
  `info` what should be `debug`; some state transitions have no
  log at all. The only way to know what a server actually writes
  is to run it and read the output.
- Impact: file logging is now present on all four servers and
  crowdb-cli, so the "no persistent logs" gap is closed. Remaining
  impact: (1) `crowdb-diskdb` and `crowdb-chunkdb` do not wire
  crowdb-rpc C++ logging, so their transport layer is silent; (2)
  the C++ log level is hardcoded `"info"` on every server —
  operators cannot tune it; (3) no server has a `--log-dir` flag,
  so log directories diverge (`"log"` vs `~/.crowdb-kv/log` vs
  `cli-log/`) and cannot be unified per-deploy; (4) `crowdb-web`
  rpc logs go to the wrong directory (`"log"` instead of
  `~/.crowdb-kv/log`); (5) `CrowDBConfig.log_dir` is computed but
  never used by kv-server's logging init; (6) log content is
  unreviewed; (7) e2e log file existence is partially asserted
  (web lifecycle tests) but no content assertions exist.
- Root cause: partially-landed placeholder. The shared logging
  infrastructure was extended to all servers and the crowdb-rpc
  FFI bridge was built, but the work stopped short of: CLI flag
  unification, C++ level propagation, crowdb-rpc wiring in
  diskdb/chunkdb, log directory unification, a logging design-doc
  section, log content review, and e2e log content assertions. The
  `ops_log` module was already deleted (folded into tracing via
  `log_ops_http`), so D4 is already satisfied.

**Design pointers**

- `doc/design/kv/design-crowdb-kv-observability.md` — root
  observability design. **Design gap:** this doc covers metrics
  exhaustively (§2) but has no section on *logging* — log file
  layout, rotation/compression policy, per-server initialization,
  Rust/C++ log coordination, log directory convention, or log
  content guidelines. §1 mentions "structured logs with node_id,
  group_id, slot, term on consensus events" as a mandatory signal
  but never specifies the logging stack that produces them. R119
  must add a "Logging" section to the observability design doc
  anchoring the unified scheme; the backlog references it as
  `§<new>` once added. Flagged here rather than inventing
  architecture in the backlog.
- `doc/design/kv/design-crowdb-kv-server.md` — `crowdb-kv-server`
  binary startup; the existing logging init lives here.
- `doc/design/diskdb/design-crowdb-diskdb.md`,
  `doc/design/chunkdb/design-crowdb-chunkdb-rpc.md` — diskdb/chunkdb
  server startup; file-logging adoption touches these.
- `doc/design/rpc/design-crowdb-rpc.md` — crowdb-rpc design; the
  logging C API and its Rust FFI wrapper land here.

**Use scenarios**

- **Operator deploys diskdb and reviews logs after a failure** —
  operator starts `crowdb-diskdb --config ...`, runs the cluster for
  a day, a disk goes `Bad`, the operator looks in the log
  directory and finds `crowdb-diskdb-{ts}-{pid}.log` (and rotated
  `.log.gz` files) with a clear, self-explaining line naming the
  disk, the zone, the impacted blocks, and the recovery action
  taken. Today this file does not exist.
- **Operator deploys chunkdb and reviews logs after a placement
  error** — same shape: `crowdb-chunkdb-{ts}-{pid}.log` exists,
  rotated and compressed, and the line explaining the allocation
  failure is self-explanatory.
- **Operator debugs a consensus stall with kv-server + crowdb-rpc
  logs** — operator correlates the Rust `crowdb-kv-server` log
  (consensus phase timings, slot watermarks) with the C++
  `crowdb-kv-server-rpc` log (transport-level send/recv, framing,
  correlation) in the same directory, same timestamp format, same
  PID suffix. Today crowdb-rpc logs nothing.
- **Operator sets one log root for all servers** — operator passes
  `--log-dir /var/log/crowdbdb` (or a config field) to every server;
  all Rust and C++ logs for that process land under that directory
  with per-process filenames. Today the log directory is either
  hardcoded `"log"`, `~/.crowdb-kv/log`, or a temp path depending on
  the binary.
- **Operator tunes log level and rotation uniformly** — operator
  sets `RUST_LOG=info` and a C++ level flag/env on every server;
  all servers honor the same rotation size and file count (or
  their own overrides). Today only `crowdb-kv-server` has
  `--log-max-file-mb` / `--log-max-files`; the C++ level is
  hardcoded to `"info"` with no override.
- **Reviewer runs an e2e test and inspects real log output** — a
  reviewer runs each service's e2e test, opens the log file the
  service produced, and reads every line: each line is
  meaningful, names the component and the event, and a reader
  unfamiliar with the code can understand the behavior from the
  log alone. Lines that are noise, redundant, or opaque are
  fixed.

**Solution**

File logging is now wired on all four servers and the crowdb-rpc
FFI bridge exists, so the remaining work is smaller than when this
doc was written. The unification target is clear (every server
exposes `--log-dir` / `--log` / `--log-max-file-mb` / `--log-max-
files` CLI flags; C++ log level driven by the same source as Rust;
crowdb-rpc logging wired in diskdb/chunkdb; one log directory
convention; log content reviewed for meaning), and the open
design questions have been resolved (see Decisions below). The
audit (work item 1) is still the prerequisite for the remediation
scope: its findings determine which log lines need fixing.

**Decisions** (resolved from the open-questions review):

- **D1 — Per-stack files, not merged.** Rust and C++ write to
  separate files in the same directory, both using
  `{prefix}-{YYYYMMDD-HHMMSS.mmm}-{pid}.log`. The shared
  timestamp + PID suffix lets the operator interleave files from
  one process by sort order; no cross-FFI write coordination
  needed. The merged-file option is rejected.
- **D2 — Metrics log is the one combined file.** Only the metrics
  log is combined into a single file per process: Rust queries
  the metrics and writes the content (already the landed pattern
  via `MetricsRunner`).
- **D3 — `info` by default; optional err-to-stderr.** Both Rust
  and C++ stacks default to `info`. A server option (CLI flag or
  env) mirrors `error` + `warn` lines to the process stderr so
  operators tailing stderr see failures without grepping the file.
  Enable this during UT runs to surface failures in test output.
- **D4 — Drop `ops_log` as a separate file.** **Already done.**
  The `ops_log` module has been deleted; ops lines are folded into
  the normal tracing log via `log_ops_http` in
  `crowdb-console-shared/src/clients.rs`. No separate `ops_log`
  rotation work needed — the server log's rotation covers it.
- **D5 — `crowdb-cli` is console-only except `bench`.** **Not the
  current behavior.** `crowdb-cli` currently writes file logs for
  ALL commands to `cli-log/{command-slug}-{timestamp}/`. This is
  acceptable — the per-invocation directory is cheap and keeps run
  history for every command, not just bench. The decision is
  revised: keep the current behavior (file logging for all
  commands); `bench` additionally opens a metrics log. No change
  needed.
- **D6 — Log content: readable and rich, not strictly templated.**
  No strict `component=` field requirement. Guidelines: every
  line should be clear and readable, carry enough context to
  locate the code position and the surrounding state, and bring
  rich info for identifying bugs. Machine-parseability is a
  plus — AI agents do the real log-digging, so structured fields
  where they fit naturally are welcome, but author flexibility
  wins over a rigid template.

**One-line summary**: Audit log infrastructure and log content
across all servers and C++ libraries (code review + e2e log
inspection), then unify the remaining gaps: add `--log-dir` /
`--log-*` CLI flags to diskdb/chunkdb/web, drive C++ log level from
the same source as Rust (info default), wire crowdb-rpc logging in
diskdb/chunkdb, fix the crowdb-web rpc log dir mismatch, make
`CrowDBConfig.log_dir` actually drive kv-server's logging init,
adopt one log directory convention, add a Logging section to the
observability design doc, and fix log lines that are not
meaningful or self-explaining.

Numbered work items:

1. **Audit pass — code** — **Done.** The audit is captured in the
   "Current behavior" section above and in the design draft
   (`doc/working/design-r119-log-file-usage-review.md`). Key
   findings: all 4 servers + cli have Rust file logging; kv-server
   + web + cli have crowdb-rpc C++ logging; diskdb + chunkdb do
   NOT; only kv-server has crowdb-tree C++ logging; `ops_log` is
   deleted (folded into tracing); crowdb-web rpc log dir is
   mismatched; `CrowDBConfig.log_dir` is unused; C++ level is
   hardcoded everywhere; no `--log-dir` flag on any server.
2. **Audit pass — e2e logs** — deferred to work item 9 (log
   content remediation). The e2e log inspection is done as part
   of the content remediation pass, not as a separate upfront
   step, since the code audit already identified the
   infrastructure gaps.
3. **Observability design section** —
   `doc/design/kv/design-crowdb-kv-observability.md` (new "Logging"
   section). Anchor the unified scheme per the resolved decisions:
   log directory convention (CLI `--log-dir` / config field,
   default under `--root`/log or a platform default); rotation +
   compression policy (reuse `RotatingLogWriter` /
   `compressing_file_sink` defaults, 30 MiB / 5 files); **per-stack
   files** (D1) — Rust and C++ each write
   `{prefix}-{YYYYMMDD-HHMMSS.mmm}-{pid}.log` in the same dir, no
   merged file; **metrics log is the one combined file** (D2),
   written by Rust; **`info` default level** (D3) for both stacks
   driven from the same CLI flag / env, plus an err-to-stderr
   mirror option; **no separate `ops_log`** (D4, already done);
   **`crowdb-cli` keeps file logging for all commands** (D5,
   revised); **log content guidelines** (D6) — readable, rich,
   self-locating, no strict template, machine-parseable where
   natural. Closes the design gap flagged above.
4. **`crowdb-diskdb` logging CLI flags + crowdb-rpc wiring** —
   `app/crowdb-diskdb/src/main.rs` + `app/crowdb-diskdb/src/ddb_config.rs`.
   File logging is already wired (`init_file_and_console_logging_split`,
   defaults 30/5). Remaining: add `--log-dir`, `--log-max-file-mb`,
   `--log-max-files`, `--log` (console toggle) CLI flags with the
   same defaults as `crowdb-kv-server`; wire
   `crowdb_rpc_ffi::init_logging` at startup with prefix
   `"crowdb-diskdb-rpc"` (currently not called — transport layer
   silent); drive the C++ level from the same source as Rust.
5. **`crowdb-chunkdb` logging CLI flags + crowdb-rpc wiring** —
   `app/crowdb-chunkdb/src/main.rs` + chunkdb config. File logging
   is already wired, same shape as diskdb. Same remaining work: add
   CLI flags, wire `crowdb_rpc_ffi::init_logging` with prefix
   `"crowdb-chunkdb-rpc"`, drive C++ level from same source as Rust.
6. **`crowdb-web` logging CLI flags + rpc log dir fix** —
   `app/crowdb-web/src/main.rs`. File logging is already wired
   (to `~/.crowdb-kv/log`). crowdb-rpc logging is already
   initialized BUT passes the literal `"log"` instead of the
   `~/.crowdb-kv/log` dir used by Rust tracing — fix this
   mismatch. Add `--log-dir` / `--log-*` CLI flags. The `ops_log`
   module is already deleted (D4 done); no ops_log work needed.
   `crowdb-cli` already has file logging for all commands (D5
   revised) — no change needed there.
7. **`crowdb-rpc` C++ logging — wire diskdb/chunkdb** — folded
   into work items 4 and 5 (the `crowdb_rpc_ffi::init_logging`
   call is added in each server's main.rs). The FFI wrapper already
   exists and works; only the call sites are missing.
8. **`crowdb-kv-server` logging cleanup** —
   `app/crowdb-kv-server/src/main.rs` lines 34-74. Drive the C++
   crowdb-tree AND crowdb-rpc level from the same source as the Rust
   level (CLI flag / `RUST_LOG`), defaulting to `info` (D3) — not
   the hardcoded `"info"`. Add the err-to-stderr mirror option
   (D3): `error` + `warn` lines also go to the process stderr so
   operators tailing stderr see failures without grepping the file.
   Adopt the unified `--log-dir` convention. Make
   `CrowDBConfig.log_dir` actually drive the logging init calls
   (currently the config field is computed but the main.rs passes
   the literal `"log"`). crowdb-rpc logging is already initialized
   — only the level, log-dir, and err-to-stderr wiring are stale.
9. **Log content remediation** — every `tracing::*` / `CRB_LOG_*`
   call site flagged by the audit. Per D6: no strict `component=`
   template, but every line should be clear, readable, and carry
   enough context to locate the code position and the surrounding
   state — fix lines that are opaque (add component + event +
   identifiers), remove or downgrade noise (move chatty lines to
   `debug`), add missing context to state-transition logs, and
   ensure each line self-explains the behavior. Machine-parseable
   structured fields are welcome where they fit naturally (AI
   agents do the real log-digging), but author flexibility wins
   over a rigid template. Per the observability design §1,
   consensus-event logs must carry `node_id`, `group_id`, `slot`,
   `term` where applicable.
10. **E2e log verification harness** —
    `lib/crowdb-test-harness/src/{diskdb,chunkdb,diskio}.rs` + the
    e2e test files. After remediation, each service's e2e test
    must write to a real log file (not just redirected stderr)
    and the test must assert the file exists, is non-empty, and
    contains expected key lines (e.g. "server starting",
    "ready", a representative operational event). This is the
    acceptance gate: the log file and its content are checked,
    not just the process exit code. Note: `lifecycle_routes_test.rs`
    already asserts log file existence for web-managed kv-server
    nodes — extend this pattern to the other services.

Flow diagram (shape only):

```
                 ┌─────────────────────────────────────────┐
                 │  crowdb-common/rust  crowdb-common/cpp       │
                 │  RotatingLogWriter   compressing_sink    │
                 │  (rotation+gzip)     (rotation+gzip)     │
                 └──────────┬──────────────┬───────────────┘
                            │              │
            Rust tracing    │              │  C++ spdlog
          ┌─────────────────┘              └────────────────┐
          ▼                                                 ▼
  ┌────────────────────────────────────────────────────────────┐
  │  crowdb-kv-server   crowdb-diskdb   crowdb-chunkdb   crowdb-web    │
  │  --log-dir <dir>  --log-level <lvl>                        │
  │  --log-max-file-mb <N>  --log-max-files <N>  [--log]       │
  │                                                            │
  │  init_file_logging()  +  ct_init_logging()  (crowdb-tree)    │
  │                       +  rpc_init_logging()  (crowdb-rpc)    │
  └────────────────────────────────────────────────────────────┘
          │                                                 │
          ▼                                                 ▼
  {prefix}-{ts}-{pid}.log               {prefix}-{ts}-{pid}.log
  {prefix}-metrics-{ts}-{pid}.log       rotated → .log.gz
  rotated → .log.gz
          │
          ▼
  e2e test: assert log file exists, non-empty, key lines present
```

Edge cases at a glance:

- Server started with no `--log-dir` → falls back to the
  documented default (under `--root`/log or a platform default);
  logging still works, never silently disabled.
- Log directory cannot be created (permission denied) → startup
  fails with a clear error naming the path; never proceeds with
  logging disabled (current `crowdb-kv-server` behavior is correct
  here, the others must match).
- C++ library loaded by a Rust process that did not call
  `*_init_logging` → C++ logging is no-op (the
  `logging_enabled()` gate), never crashes, never writes to an
  unexpected location.
- `RUST_LOG` set but no C++ level flag → C++ level derives from
  the same source as Rust (D3), defaulting to `info`; the two
  stacks stay in sync without forcing the operator to set two
  knobs.
- Rotated file compression fails (disk full mid-rotate) → the
  current file is still closed and a new one opened; the
  un-compressed rotated file is kept (not deleted) so no log data
  is silently lost.
- `ops_log` folded into the server log (D4) → the server log's
  rotation covers ops lines; no separate unbounded file. The
  exception is `crowdb-web`, whose ops log adopts the same
  rotation as its server log.
- Two processes with the same prefix start in the same second →
  PID suffix in the filename prevents collision (already the
  case; the audit verifies this holds everywhere).
- A log line flagged as opaque in the audit has no obvious fix
  → it is at minimum tagged with the component and event name so
  a reader knows *where* to look, even if the *why* needs code
  reading.

**Dependencies**

- None on other `R**` items for the audit (work items 1 + 2) or
  the remaining diskdb/chunkdb/web CLI-flag + crowdb-rpc wiring
  (items 4-7) — the shared `crowdb-common` logging stack and the
  crowdb-rpc FFI wrapper already exist.
- Item 7 (crowdb-rpc logging in diskdb/chunkdb) has no upstream
  `R**` dependency — the C API and Rust wrapper already exist; only
  the diskdb/chunkdb call sites are missing.
- Item 3 (observability design section) is the design anchor for
  items 4-9; it should land before the remediation so the
  unification target is agreed.
- Downstream: any future `diskio` server must follow the same
  logging scheme — item 3's design section should name this as
  the extension path.

**Acceptance**

**Audit (code)**:

- The code audit (work item 1) is captured in the "Current
  behavior" section above. Every finding is either resolved by a
  later work item or documented as accepted-as-is with a reason.
  Reviewer check (not a test).

**File logging adoption (diskdb / chunkdb / web)**:

- `crowdb-diskdb --config <toml>` started and run through its e2e
  test → a log file `crowdb-diskdb-{ts}-{pid}.log` exists in the
  configured log directory, is non-empty, and contains a
  "crowdb-diskdb starting" line and a "ready" line. E2E test.
- `crowdb-diskdb` run with `--log-dir /tmp/r119-ddb` → the log
  file lands under `/tmp/r119-ddb/`, not under the default.
  E2E test.
- `crowdb-diskdb --log-max-file-mb 1` run with enough traffic to
  exceed 1 MiB → the current file rotates, a `.log.gz` appears,
  and the current file is a fresh one under the size cap.
  Integration test.
- `crowdb-chunkdb` equivalent of the above three bullets. E2E /
  Integration test.
- `crowdb-web` started → a service log file exists; the crowdb-rpc
  log file lands in the SAME directory as the Rust log (not the
  current `"log"` mismatch). E2E test.
- `crowdb-cli bench` run → both console output and a log file
  under the configured log dir are produced; a metrics log is also
  produced. Other `crowdb-cli` subcommands produce a log file
  (current behavior, D5 revised). Integration test.
- `crowdb-diskdb` / `crowdb-chunkdb` started with a log directory
  that cannot be created → startup fails with a clear error
  naming the path; no silent console-only fallback. Integration
  test.

**C++ logging bridges (crowdb-tree + crowdb-rpc)**:

- `crowdb-kv-server` started → both `crowdb-kv-server-tree-*.log`
  (crowdb-tree) and `crowdb-kv-server-rpc-*.log` (crowdb-rpc) exist in
  the log directory alongside the Rust `crowdb-kv-server-*.log`.
  E2E test.
- `crowdb-kv-server` started with `--log-level debug` (or the
  agreed level knob) → the C++ crowdb-tree AND crowdb-rpc loggers
  run at `debug`, not the hardcoded `info`. Integration test
  (grep the C++ log for a debug-level line that would be
  suppressed at `info`).
- `crowdb-kv-server` started with the err-to-stderr option enabled
  (D3) → `error` and `warn` lines appear on the process stderr in
  addition to the log file; `info`/`debug` lines do not. Enable in
  UT runs so failures surface in test output. Integration test.
- `crowdb-diskdb` / `crowdb-chunkdb` started → a
  `crowdb-diskdb-rpc-*.log` / `crowdb-chunkdb-rpc-*.log` exists
  (crowdb-rpc logging initialized). E2E test.
- A Rust process that uses crowdb-rpc but does NOT call
  `rpc_init_logging` (e.g. a unit test) → no crash, no file
  written, `logging_enabled()` returns false. Unit test.

**Log directory + format unification**:

- Every server binary accepts `--log-dir` (or a config field)
  and writes all its logs (Rust + C++) under that directory.
  E2E test (one per server).
- `crowdb-web` rpc log file lands in the same directory as the
  Rust log file (fixes the current `"log"` vs `~/.crowdb-kv/log`
  mismatch). E2E test.
- `crowdb-kv-server` uses `CrowDBConfig.log_dir` (or `--log-dir`)
  to drive all logging init calls, not the literal `"log"`.
  Integration test.
- Rust and C++ log lines in the same process use a correlatable
  timestamp format (documented in the design section); a reader
  can sort lines from both files by timestamp and follow the
  sequence. Integration test (generate one Rust + one C++ line,
  verify timestamp formats match or are sortable).
- No server binary hardcodes a log directory as a bare relative
  path without a `--root`/config derivation (grep for `"log"`
  literals in server main files returns only the default-derivation
  path). Unit test (static check).

**Log content remediation**:

- For each service e2e test, the produced log file is read and
  every line is classifiable as meaningful (carries component +
  event + identifiers + outcome) or accepted-as-is with a
  documented reason; no line is opaque noise. E2E test (the test
  asserts presence of expected key lines; the reviewer reads the
  full file for the rest).
- Consensus-event log lines in `crowdb-kv-server` carry `group_id`,
  `slot`, `term` where applicable (per observability §1).
  Integration test (grep the log for a consensus event line and
  verify the fields).

**E2e log verification harness**:

- Each service e2e test (`crowdb-kv-server`, `crowdb-diskdb`,
  `crowdb-chunkdb`, `crowdb-web`) asserts its log file exists, is
  non-empty, and contains the expected startup + ready lines.
  E2E test.
- A test that deliberately triggers an operational event (e.g.
  diskdb disk-add, chunkdb chunk allocate, kv-server leader
  election) → the log file contains a line describing that
  event with enough context to self-explain it. E2E test.

**Invariants**:

- No server binary in the workspace calls
  `tracing_subscriber::fmt().init()` (console-only) as its sole
  logging init (grep returns nothing in `app/*/src/main.rs`).
  Unit test (static check). **Already satisfied** per the audit.
- Every server that links a C++ library with a logging C API
  (`crowdb-tree`, `crowdb-rpc`) calls the corresponding
  `*_init_logging` FFI bridge at startup. Unit test (static
  check on main.rs / startup paths).

**Test commands**: `pixi run cargo test -p crowdb-diskdb -p
crowdb-chunkdb -p crowdb-web -p crowdb-kv-server` (server e2e + logging
integration), `pixi run cargo test -p crowdb-rpc-ffi` (FFI logging
wrapper unit tests), `pixi run test-tree-ct` only if C++ logging
code changes, plus `pixi run cargo fmt --all -- --check` and
`pixi run cargo clippy --all-targets -- -D warnings`.