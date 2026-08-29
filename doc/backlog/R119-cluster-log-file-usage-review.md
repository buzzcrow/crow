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
  `init_file_logging` / `init_file_and_console_logging` build a
  `tracing-subscriber` `fmt` layer (no ANSI for file, `with_target`
  + `with_thread_names`) over a `tracing-appender` non-blocking
  wrapper. `EnvFilter` from `RUST_LOG` with a per-project default
  fallback (`crowdb-kv/src/common/logging.rs::CROWDB_KV_DEFAULT_FILTER`).
  A separate `open_metrics_log` writes a metrics log with the same
  rotation scheme. Only `crowdb-kv-server` uses any of this.
- **C++ logging infrastructure** —
  `crowdb-common/cpp/src/log.cpp` + `log.h` provide an async spdlog
  logger over `compressing_file_sink_mt` (same size-rotation +
  gzip scheme, same `{prefix}-{ts}-{pid}.log` naming). Macros
  `CR_LOG_*` gate on a runtime `logging_enabled()` atomic. No-op
  when built without `CROWDB_HAVE_SPDLOG` (the Rust FFI `cc` build).
  `crowdb-tree-ffi::ct_init_logging` (`lib/crowdb-tree/ffi/src/tree.rs`)
  bridges it. `crowdb-rpc` has `crowdb_rpc_init_logging` /
  `crowdb_rpc_flush_logging` / `crowdb_rpc_shutdown_logging` in
  `lib/crowdb-rpc/include/crowdb-rpc/c_api.h` — but there is NO Rust
  FFI wrapper for these symbols and NO caller anywhere in the
  workspace (grep for `crowdb_rpc_init_logging` in `*.rs` returns
  nothing). So crowdb-rpc's C++ logs are never initialized.
- **Per-server logging setup (the inconsistencies)**:
  - `crowdb-kv-server` (`app/crowdb-kv-server/src/main.rs` ~lines
    34-61) — full file logging: Rust
    `init_file_and_console_logging` / `init_file_logging` to dir
    `"log"`, prefix `"crowdb-kv-server"`; C++ crowdb-tree via
    `ct_init_logging("log", "info", ...)` prefix
    `"crowdb-kv-server-tree"`; metrics log via `MetricsRunner` prefix
    `"crowdb-kv-server-metrics"`. CLI flags `--log-max-file-mb`
    (default 30), `--log-max-files` (default 5), `--log` (console
    toggle), `--metrics-interval` (default 5s, 0 disables). The
    C++ level is hardcoded to `"info"` — not driven by `RUST_LOG`
    or any CLI flag. crowdb-rpc C++ logging is NOT initialized.
  - `crowdb-diskdb` (`app/crowdb-diskdb/src/main.rs` line 48) —
    `tracing_subscriber::fmt().init()`: console/stderr only. No
    file logging, no rotation, no compression, no metrics log.
  - `crowdb-chunkdb` (`app/crowdb-chunkdb/src/main.rs` line 46) —
    `tracing_subscriber::fmt().init()`: console/stderr only.
    Same gap as diskdb.
  - `crowdb-web` (`app/crowdb-web/src/main.rs` lines 32-37) —
    `tracing_subscriber::fmt().with_env_filter(...).init()`:
    console only. Also opens an `ops_log` (JSON-Lines operation
    log for HTTP/RPC/SSH calls) at
    `~/.crowdb-kv/log/console-web-{secs}-{pid}.log`
    (`crowdb-console-shared/src/ops_log.rs`) — no rotation, no
    compression, no size cap.
  - `crowdb-cli` (`app/crowdb-cli/src/main.rs` line 133) — no
    `tracing_subscriber` init in main; opens `ops_log` via
    `ops_log::init_default("cli")` at
    `~/.crowdb-kv/log/console-cli-{secs}-{pid}.log` — no rotation.
- **Log directory inconsistency** — `crowdb-kv-server` writes to
  `"log"` (relative to CWD, though `--root` derives `log_dir` in
  config); `ops_log` writes to `~/.crowdb-kv/log`; the test harness
  (`crowdb-test-harness/src/{diskdb,chunkdb,diskio}.rs`) redirects
  child stdout/stderr to a temp file like
  `/tmp/crowdb-diskdb-e2e-{pid}.log` (no rotation, single file). An
  operator cannot point all servers at one log root.
- **Log format inconsistency** — Rust tracing `fmt` layer emits
  target + thread names, no ANSI on file, ANSI on console; the
  in-line timestamp is tracing's own. C++ spdlog emits
  `%Y%m%d-%H%M%S.%e [@] [%l] [%n] %v` (UTC, custom thread-name
  flag). The two stacks cannot be correlated by a shared
  timestamp format or field layout. Console-only servers use
  tracing's default format (no thread name, no target by default).
- **Log content quality** — no audit has been done of whether
  individual log lines are meaningful and self-explaining. Some
  call sites log raw error strings with no context; some log at
  `info` what should be `debug`; some state transitions have no
  log at all. The only way to know what a server actually writes
  is to run it and read the output — which console-only servers
  discard under normal deployment.
- Impact: in a real deployment, `crowdb-diskdb`, `crowdb-chunkdb`, and
  `crowdb-web` produce no persistent logs — an operator debugging a
  disk allocation failure, a chunk placement error, or a console
  API timeout has nothing to read. `crowdb-kv-server` has logs but
  the C++ crowdb-rpc transport layer (used by the consensus hot path
  per R32) is silent. Log content is unreviewed, so even where
  logs exist they may not explain the behavior they report.
- Root cause: deferred placeholder. The shared logging
  infrastructure was built for `crowdb-kv-server` (and crowdb-tree)
  but never extended to the other servers; the crowdb-rpc C API was
  declared but never wired; no logging section exists in the
  observability design doc to anchor a unified scheme; and no
  content review pass was ever scheduled.

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

**No clear solution yet — deferred to design.** The unification
target is clear (every server initializes file logging with
rotation + compression via the shared `crowdb-common` stack; C++
libraries' logging is wired through their FFI bridges and
coordinated with the Rust process; one log directory convention;
one timestamp/format scheme; log content reviewed for meaning), but
the specific decisions — whether to unify Rust and C++ into one
file or keep per-stack files, how to propagate log level from CLI
to both stacks, whether `ops_log` adopts the shared rotation or
stays separate, and what the log content guidelines say — need a
design draft informed by the audit. The audit (work item 1) is the
prerequisite: its findings determine the remediation scope.

**One-line summary**: Audit log infrastructure and log content
across all servers and C++ libraries (code review + e2e log
inspection), then unify every server on the shared rotating-file
logging stack, wire the crowdb-rpc C++ logging through its FFI
bridge, adopt one log directory and format convention, and fix log
lines that are not meaningful or self-explaining.

Numbered work items:

1. **Audit pass — code** — every crate/binary under `lib/` and
   `app/`. Catalog every logging init call site, every log
   directory, every rotation/compression setting, every C++ FFI
   logging bridge, and a sample of `tracing::*` / `CR_LOG_*` call
   sites per component. Output: a findings list (which servers
   lack file logging, which C++ libs are unwired, which log lines
   are opaque/noisy/redundant, where log directories diverge).
   This is the input to all later work items.
2. **Audit pass — e2e logs** — `crates/crowdb-kv/tests/`,
   `app/crowdb-diskdb/tests/`, `app/crowdb-chunkdb/tests/`,
   `app/crowdb-web/tests/`, `lib/crowdb-test-harness/`. Run each
   service's e2e test, capture the real log output (file or
   redirected stderr), and read every line. Mark each line
   meaningful / noise / opaque / missing-context. Output: a
   per-service log-content findings list that drives work item 7.
3. **Observability design section** —
   `doc/design/kv/design-crowdb-kv-observability.md` (new "Logging"
   section). Anchor the unified scheme: log directory convention
   (CLI `--log-dir` / config field, default under `--root`/log or
   a platform default), rotation + compression policy (reuse
   `RotatingLogWriter` / `compressing_file_sink` defaults), Rust
   vs C++ file layout (one file per stack per process vs. merged),
  timestamp + field format for cross-stack correlation, log level
  propagation (CLI flag / env → both stacks), and log content
  guidelines (what every line must carry: component, event,
  identifiers, outcome). Closes the design gap flagged above.
4. **`crowdb-diskdb` file logging** —
   `app/crowdb-diskdb/src/main.rs` + `app/crowdb-diskdb/src/ddb_config.rs`.
   Replace `tracing_subscriber::fmt().init()` with
   `crowdb_common::logging::init_file_logging` (or the
   `crowdb_kv`-style wrapper if one is extracted to `crowdb-common`).
   Add `--log-dir`, `--log-max-file-mb`, `--log-max-files`,
   `--log` (console toggle) CLI flags (or config fields) with the
   same defaults as `crowdb-kv-server`. Add a metrics log if diskdb
   metrics warrant one (pending audit). No C++ library logging
   needed unless diskdb adopts a C++ engine (it does not today).
5. **`crowdb-chunkdb` file logging** —
   `app/crowdb-chunkdb/src/main.rs` + chunkdb config. Same shape as
   diskdb: replace console-only init with the shared file logging
   stack; add the same CLI/config flags.
6. **`crowdb-web` + `crowdb-cli` file logging + ops_log rotation** —
   `app/crowdb-web/src/main.rs`, `app/crowdb-cli/src/main.rs`,
   `lib/crowdb-console-shared/src/ops_log.rs`. Replace console-only
   `tracing` init with the shared file logging stack for the
   service log. Decide (in design) whether `ops_log` adopts
   `RotatingLogWriter` or stays a separate append-only JSON-Lines
   file with its own rotation. `crowdb-cli` is short-lived so its
   logging may be console-only by design — the audit + design
   decide.
7. **`crowdb-rpc` C++ logging FFI bridge** —
   `lib/crowdb-rpc/ffi/src/` (new Rust wrapper mirroring
   `crowdb-tree-ffi::ct_init_logging`), `app/crowdb-kv-server/src/main.rs`,
   `app/crowdb-diskdb/src/main.rs`, `app/crowdb-chunkdb/src/main.rs`.
   Add a Rust FFI wrapper for `crowdb_rpc_init_logging` /
   `crowdb_rpc_flush_logging` / `crowdb_rpc_shutdown_logging`. Every
   server that uses crowdb-rpc (kv-server consensus + client RPC,
   diskdb, chunkdb) calls it at startup with the same log dir,
   level, rotation settings, and a per-server prefix
   (`crowdb-kv-server-rpc`, `crowdb-diskdb-rpc`, `crowdb-chunkdb-rpc`).
   The C++ level must be driven by the same CLI flag / env as the
   Rust stack, not hardcoded.
8. **`crowdb-kv-server` logging cleanup** —
   `app/crowdb-kv-server/src/main.rs` ~lines 55-61. Drive the C++
   crowdb-tree level from the same source as the Rust level (CLI
   flag / `RUST_LOG`), not the hardcoded `"info"`. Adopt the
   unified `--log-dir` convention. Ensure crowdb-rpc logging is
   initialized (work item 7).
9. **Log content remediation** — every `tracing::*` / `CR_LOG_*`
   call site flagged by the audit (work items 1 + 2). Fix lines
   that are opaque (add component + event + identifiers), remove
   or downgrade noise (move chatty lines to `debug`), add missing
   context to state-transition logs, and ensure each line
   self-explains the behavior. Per the observability design §1,
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
    not just the process exit code.

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
  a documented mapping (e.g. `info` default, or a `CROWDB_CPP_LOG`
  env) so the two stacks stay roughly in sync without forcing the
  operator to set two knobs.
- Rotated file compression fails (disk full mid-rotate) → the
  current file is still closed and a new one opened; the
  un-compressed rotated file is kept (not deleted) so no log data
  is silently lost.
- `ops_log` grows unbounded (long-running `crowdb-web` session) →
  rotation or size cap applies (pending design decision on
  whether `ops_log` adopts `RotatingLogWriter`).
- Two processes with the same prefix start in the same second →
  PID suffix in the filename prevents collision (already the
  case; the audit verifies this holds everywhere).
- A log line flagged as opaque in the audit has no obvious fix
  → it is at minimum tagged with the component and event name so
  a reader knows *where* to look, even if the *why* needs code
  reading.

**Dependencies**

- None on other `R**` items for the audit (work items 1 + 2) or
  the diskdb/chunkdb/web file-logging adoption (items 4-6) — the
  shared `crowdb-common` logging stack already exists.
- Item 7 (crowdb-rpc FFI logging bridge) has no upstream `R**`
  dependency — the C API already exists in `c_api.h`; only the
  Rust wrapper and call sites are missing.
- Item 3 (observability design section) is the design anchor for
  items 4-9; it should land before the remediation so the
  unification target is agreed.
- Downstream: any future `diskio` server must follow the same
  logging scheme — item 3's design section should name this as
  the extension path.

**Acceptance**

**Audit (code + e2e)**:

- The code audit (work item 1) produces a findings document
  listing every server's logging init, log directory, rotation
  settings, and C++ FFI bridge status; every finding is either
  resolved by a later work item or documented as accepted-as-is
  with a reason. Reviewer check (not a test).
- The e2e audit (work item 2) produces a per-service log-content
  findings list; every line marked noise/opaque/missing-context
  is either fixed in work item 9 or documented as accepted-as-is.
  Reviewer check (not a test).

**File logging adoption (diskdb / chunkdb / web)**:

- `crowdb-diskdb --config <toml>` started and run through its e2e
  test → a log file `crowdb-diskdb-{ts}-{pid}.log` exists in the
  configured log directory, is non-empty, and contains a
  "crowdb-diskdb starting" line and a "ready" line. E2E test.
- `crowdb-diskdb` run with `--log-dir /tmp/r119-ddb` → the log
  file lands under `/tmp/r119-r119-ddb/`, not under the default.
  E2E test.
- `crowdb-diskdb --log-max-file-mb 1` run with enough traffic to
  exceed 1 MiB → the current file rotates, a `.log.gz` appears,
  and the current file is a fresh one under the size cap.
  Integration test.
- `crowdb-chunkdb` equivalent of the above three bullets. E2E /
  Integration test.
- `crowdb-web` started → a service log file exists (not just the
  `ops_log`); `ops_log` either rotates or has a documented size
  cap. E2E test.
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
  Unit test (static check).
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

**Open Questions**

1. **Rust vs C++ log file layout** — one merged file per process
   (Rust and C++ write to the same file, requiring a shared
   writer or a merge tool) vs. one file per stack per process
   (`crowdb-kv-server.log` + `crowdb-kv-server-tree.log` +
   `crowdb-kv-server-rpc.log`). Merged gives a single chronological
   view but requires cross-language file coordination (a mutex
   across FFI or a post-merge step); per-stack is simpler and
   matches today's crowdb-tree pattern but the operator must
   interleave three files to follow a request. Trade-off:
   operator experience vs. implementation complexity. Needs a
   human decision on the operator workflow.
2. **Log level propagation across stacks** — a single CLI flag /
   env that maps to both `tracing` `EnvFilter` and spdlog level,
   vs. separate knobs (`RUST_LOG` + `CROWDB_CPP_LOG`). Single-knob
   is simpler for operators but the level granularities do not
   map 1:1 (spdlog `trace` vs. tracing `trace`; `RUST_LOG`
   per-target directives have no spdlog equivalent). Separate
   knobs preserve full power but risk the stacks drifting out of
   sync. Trade-off: simplicity vs. control. Needs a decision on
   whether per-target filtering matters for C++.
3. **`ops_log` rotation** — adopt `RotatingLogWriter` for
   `ops_log` (unified rotation, but `ops_log` is JSON-Lines and
   `RotatingLogWriter` is line-oriented text — compatible) vs.
   keep it as a separate append-only file with its own size cap
   vs. leave it unbounded (it is a session-scoped audit trail,
   not a runtime log). Trade-off: consistency vs. the audit-trail
   semantics of `ops_log`. Needs a decision on whether `ops_log`
   is a log or an audit record.
4. **`crowdb-cli` logging** — `crowdb-cli` is short-lived (one
   command then exit); does it need file logging at all, or is
   console output + `ops_log` sufficient? File logging adds a
   log directory creation cost per invocation for a process that
   exits in seconds. Trade-off: completeness vs. startup cost.
   Needs a decision on whether the CLI is ever run detached.
5. **Log content guidelines granularity** — the design section
   must state what every log line carries (component, event,
   identifiers, outcome), but how prescriptive should it be?
   A strict template (every line must have a `component=` field)
   enables machine parsing but constrains free-form diagnostic
   messages; a loose guideline ("every line must be
   self-explanatory") is subjective and hard to test. Trade-off:
   machine-parseability vs. author flexibility. Needs a decision
   on whether logs are for humans, scripts, or both.
