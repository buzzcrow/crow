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
  - `crowdb-kv-server` (`app/crowdb-kv-server/src/main.rs` ~lines
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
    flag; the log dir is hardcoded `"log"` (no `--log-dir` flag).
  - `crowdb-diskdb` (`app/crowdb-diskdb/src/main.rs` lines 56-64) —
    file logging is now wired via
    `init_file_and_console_logging_split("log", "crowdb-diskdb",
    30, 5, "info", "warn")`. Remaining gaps: no `--log` /
    `--log-dir` / `--log-max-file-mb` / `--log-max-files` CLI flags
    (defaults hardcoded); no metrics log; C++ crowdb-rpc logging
    NOT initialized (transport layer silent); log dir hardcoded
    `"log"`.
  - `crowdb-chunkdb` (`app/crowdb-chunkdb/src/main.rs` lines 52-60)
    — file logging is now wired, same shape as diskdb. Same
    remaining gaps: no CLI flags, no crowdb-rpc logging, log dir
    hardcoded. The chunkdb server binary has landed.
  - `crowdb-web` (`app/crowdb-web/src/main.rs` lines 33-68) — file
    logging is now wired via `init_file_and_console_logging_split`
    to `~/.crowdb-kv/log`, prefix `"console-web"`. Also opens an
    `ops_log` (JSON-Lines operation log for HTTP/RPC/SSH calls) at
    `~/.crowdb-kv/log/console-web-{secs}-{pid}.log`
    (`crowdb-console-shared/src/ops_log.rs`) — no rotation, no
    compression, no size cap. Remaining gaps: no CLI flags for log
    tuning; `ops_log` unbounded; log dir hardcoded to
    `~/.crowdb-kv/log` (different from the `"log"` used by
    kv-server/diskdb/chunkdb).
  - `crowdb-cli` (`app/crowdb-cli/src/main.rs` line 133) — no
    `tracing_subscriber` init in main; opens `ops_log` via
    `ops_log::init_default("cli")` at
    `~/.crowdb-kv/log/console-cli-{secs}-{pid}.log` — no rotation.
- **Log directory inconsistency** — `crowdb-kv-server`,
  `crowdb-diskdb`, and `crowdb-chunkdb` all write to `"log"`
  (relative to CWD); `crowdb-web` and `ops_log` write to
  `~/.crowdb-kv/log`; the test harness
  (`crowdb-test-harness/src/{diskdb,chunkdb,diskio}.rs`) redirects
  child stdout/stderr to `<workspace_root>/test-logs/crowdb-*-e2e-
  {pid}.log` (no rotation, single file). An operator cannot point
  all servers at one log root.
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
- Impact: file logging is now present on all four servers, so the
  "no persistent logs" gap is largely closed. Remaining impact: (1)
  `crowdb-diskdb` and `crowdb-chunkdb` do not wire crowdb-rpc C++
  logging, so their transport layer is silent; (2) the C++ log level
  is hardcoded `"info"` on every server — operators cannot tune it;
  (3) no server has a `--log-dir` flag, so log directories diverge
  (`"log"` vs `~/.crowdb-kv/log`) and cannot be unified per-deploy;
  (4) `ops_log` has no rotation/size cap and can grow unbounded on a
  long-running `crowdb-web`; (5) log content is unreviewed, so even
  where logs exist they may not explain the behavior they report;
  (6) no e2e test asserts log file existence or key content lines.
- Root cause: partially-landed placeholder. The shared logging
  infrastructure was extended to diskdb/chunkdb/web and the
  crowdb-rpc FFI bridge was built, but the work stopped short of:
  CLI flag unification, C++ level propagation, crowdb-rpc wiring in
  diskdb/chunkdb, `ops_log` rotation, a logging design-doc section,
  log content review, and e2e log assertions.

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
crowdb-rpc logging wired in diskdb/chunkdb; `ops_log` rotation; one
log directory convention; log content reviewed for meaning), but
the specific decisions — whether to unify Rust and C++ into one
file or keep per-stack files, how to propagate log level from CLI
to both stacks, whether `ops_log` adopts the shared rotation or
stays separate, and what the log content guidelines say — need a
design draft informed by the audit. The audit (work item 1) is the
prerequisite: its findings determine the remediation scope.

**One-line summary**: Audit log infrastructure and log content
across all servers and C++ libraries (code review + e2e log
inspection), then unify the remaining gaps: add `--log-*` CLI flags
to diskdb/chunkdb/web, drive C++ log level from CLI/env, wire
crowdb-rpc logging in diskdb/chunkdb, add `ops_log` rotation, adopt
one log directory convention, and fix log lines that are not
meaningful or self-explaining.

Numbered work items:

1. **Audit pass — code** — every crate/binary under `lib/` and
   `app/`. Catalog every logging init call site, every log
   directory, every rotation/compression setting, every C++ FFI
   logging bridge, and a sample of `tracing::*` / `CRB_LOG_*` call
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
4. **`crowdb-diskdb` logging CLI flags + crowdb-rpc wiring** —
   `app/crowdb-diskdb/src/main.rs` + `app/crowdb-diskdb/src/ddb_config.rs`.
   File logging is already wired (`init_file_and_console_logging_split`,
   defaults 30/5). Remaining: add `--log-dir`, `--log-max-file-mb`,
   `--log-max-files`, `--log` (console toggle) CLI flags (or config
   fields) with the same defaults as `crowdb-kv-server`; wire
   `crowdb_rpc_ffi::init_logging` at startup with prefix
   `"crowdb-diskdb-rpc"` (currently not called — transport layer
   silent); add a metrics log if diskdb metrics warrant one (pending
   audit).
5. **`crowdb-chunkdb` logging CLI flags + crowdb-rpc wiring** —
   `app/crowdb-chunkdb/src/main.rs` + chunkdb config. File logging
   is already wired, same shape as diskdb. Same remaining work: add
   CLI flags, wire `crowdb_rpc_ffi::init_logging` with prefix
   `"crowdb-chunkdb-rpc"`.
6. **`crowdb-web` + `crowdb-cli` logging CLI flags + ops_log rotation** —
   `app/crowdb-web/src/main.rs`, `app/crowdb-cli/src/main.rs`,
   `lib/crowdb-console-shared/src/ops_log.rs`. File logging is
   already wired on `crowdb-web` (to `~/.crowdb-kv/log`). Remaining:
   add `--log-dir` / `--log-*` CLI flags; decide (in design) whether
   `ops_log` adopts `RotatingLogWriter` or stays a separate
   append-only JSON-Lines file with its own rotation/size cap
   (currently unbounded). `crowdb-cli` is short-lived so its logging
   may be console-only by design — the audit + design decide.
7. **`crowdb-rpc` C++ logging — wire diskdb/chunkdb** —
   `lib/crowdb-rpc/ffi/src/logging.rs` (FFI wrapper already exists),
   `app/crowdb-diskdb/src/main.rs`, `app/crowdb-chunkdb/src/main.rs`.
   The Rust FFI wrapper (`init_logging` / `flush_logging` /
   `shutdown_logging`) is built and called by `crowdb-kv-server` and
   `crowdb-web`. Remaining: `crowdb-diskdb` and `crowdb-chunkdb`
   must call `crowdb_rpc_ffi::init_logging` at startup with the same
   log dir, level, rotation settings, and per-server prefix. The
   C++ level must be driven by the same CLI flag / env as the Rust
   stack, not hardcoded.
8. **`crowdb-kv-server` logging cleanup** —
   `app/crowdb-kv-server/src/main.rs` ~lines 57-74. Drive the C++
   crowdb-tree AND crowdb-rpc level from the same source as the Rust
   level (CLI flag / `RUST_LOG`), not the hardcoded `"info"`. Adopt
   the unified `--log-dir` convention. crowdb-rpc logging is already
   initialized — only the level and log-dir are stale.
9. **Log content remediation** — every `tracing::*` / `CRB_LOG_*`
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
6. **Should the merged-file option (Q1) be rejected outright?** —
   per-stack files (`crowdb-kv-server.log` +
   `crowdb-kv-server-tree.log` + `crowdb-kv-server-rpc.log`) are
   already the landed pattern and work well; the operator
   interleaves by timestamp (both stacks use
   `{prefix}-{YYYYMMDD-HHMMSS.mmm}-{pid}.log` naming with PID
   suffix, so files from one process are grouped). Merging Rust +
   C++ into one file requires cross-FFI write coordination (a
   shared mutex or a single writer thread) for marginal operator
   convenience. Recommendation: reject the merged-file option,
   keep per-stack files, and document the interleaving workflow.
   Needs confirmation.
