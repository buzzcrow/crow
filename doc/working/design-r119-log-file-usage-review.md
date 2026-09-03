<!-- Copyright 2026-present Gian <crow.db@outlook.com -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Log File Usage Review & Unification (R119)

Backlog: [`doc/backlog/R119-cluster-log-file-usage-review.md`](../backlog/R119-cluster-log-file-usage-review.md)
Root design: [`doc/design/kv/design-crowdb-kv-observability.md`](../design/kv/design-crowdb-kv-observability.md) (new §3 Logging)

Architecture decisions and rationale are in the root design; this doc
does not repeat them.

## 1. Unified CLI Flag Surface

### 1.1 Why

Each server currently hardcodes its logging parameters or exposes a
partial subset of CLI flags. `crowdb-kv-server` has `--log`,
`--log-max-file-mb`, `--log-max-files` but no `--log-dir` and no
`--log-level`. `crowdb-diskdb` and `crowdb-chunkdb` have no logging
flags at all. `crowdb-web` has no logging flags. An operator cannot
tune logging uniformly across the fleet.

### 1.2 Shared flag set

Every server binary (`crowdb-kv-server`, `crowdb-diskdb`,
`crowdb-chunkdb`, `crowdb-web`) exposes the same logging CLI flags:

- `--log-dir <DIR>` — log directory. Default: `"log"` (relative to
  CWD, matching current behavior). For `crowdb-web`: default
  `~/.crowdb-kv/log` (matching current behavior).
- `--log-level <LEVEL>` — log level for BOTH Rust and C++ stacks.
  Default: `"info"`. Maps to `RUST_LOG` if set (Rust side) and to
  `spdlog::level::from_str` (C++ side). Valid values: `trace`,
  `debug`, `info`, `warn`, `error`, `off`.
- `--log-max-file-mb <N>` — max file size before rotation. Default:
  `30`.
- `--log-max-files <N>` — max rotated `.log.gz` files to keep.
  Default: `5`.
- `--log` / `-l` — console toggle (bool, default false). When true,
  console output is enabled at `warn` level (current split behavior).
- `--log-stderr <LEVEL>` — mirror `error`+`warn` (or the given level
  and above) to stderr via the C++ `add_log_stderr` bridge. Default:
  off. Enables D3's err-to-stderr mirror.

`crowdb-cli` keeps its existing `--log-root` flag (not
`--log-dir` — it uses a per-invocation directory structure).

### 1.3 C++ level derivation

The C++ log level is derived from the same `--log-level` flag (or
`RUST_LOG` env if set). The mapping is straightforward: the level
string (`"info"`, `"debug"`, etc.) is passed directly to
`ct_init_logging` and `crowdb_rpc_ffi::init_logging`. Both C++ bridges
accept the same level strings that `spdlog::level::from_str`
understands.

For `RUST_LOG` compatibility: if `RUST_LOG` is set, the Rust
`EnvFilter` uses it directly. For the C++ side, we extract the global
level from `RUST_LOG` (the first directive before any `=` — e.g.
`RUST_LOG=debug` → `"debug"`, `RUST_LOG=crowdb_kv=info` → `"info"`).
If `RUST_LOG` is not set, `--log-level` is used for both stacks.

Edge cases:
- `RUST_LOG` set to a complex directive like
  `crowdb_kv=debug,crowdb_rpc=info` → C++ level defaults to `"info"`
  (the first global directive found, or `"info"` if none).
- `--log-level` explicitly set → overrides `RUST_LOG` for the C++
  side (Rust still uses `RUST_LOG` if set, per `EnvFilter`).
- Invalid level string → startup fails with a clear error.

## 2. Server Logging Init Unification

### 2.1 `crowdb-kv-server` cleanup

`app/crowdb-kv-server/src/main.rs` lines 34-74:

a. Replace the literal `"log"` dir with `args.log_dir` (new CLI flag)
   or `config.log_dir` if `--log-dir` is not given. `CrowDBConfig.log_dir`
   (config.rs:515, default `"log"`, set to `root/log` by `apply_root`)
   is the fallback.
b. Replace the hardcoded `"info"` in `ct_init_logging` and
   `crowdb_rpc_ffi::init_logging` with the derived C++ level (§1.3).
c. Add `--log-stderr` support: if set, call
   `crowdb_tree_ffi::ct_add_log_stderr(level)` and
   `crowdb_rpc_ffi::add_log_stderr(level)` after init.
d. Add `--log-dir` and `--log-level` to `cli.rs`.

### 2.2 `crowdb-diskdb` CLI flags + crowdb-rpc wiring

`app/crowdb-diskdb/src/main.rs` lines 72-80 + `ddb_config.rs`:

a. Add `--log-dir`, `--log-level`, `--log-max-file-mb`,
   `--log-max-files`, `--log`, `--log-stderr` CLI flags to the diskdb
   CLI struct. Defaults: `"log"`, `"info"`, 30, 5, false, off.
b. Replace the hardcoded `init_file_and_console_logging_split("log",
   ...)` call with one that uses the CLI args.
c. Add `crowdb_rpc_ffi::init_logging(log_dir, cpp_level,
   max_file_mb, max_files, "crowdb-diskdb-rpc")` after the Rust init.
d. Add `crowdb_rpc_ffi::shutdown_logging()` to the shutdown path.
e. If `--log-stderr` is set, call `crowdb_rpc_ffi::add_log_stderr(level)`.

Note: diskdb does not link `crowdb-tree-ffi`, so `ct_init_logging` is
not called. Only `crowdb_rpc_ffi::init_logging` is added.

### 2.3 `crowdb-chunkdb` CLI flags + crowdb-rpc wiring

`app/crowdb-chunkdb/src/main.rs` lines 61-69 + chunkdb config:

Same shape as diskdb (§2.2). Prefix: `"crowdb-chunkdb-rpc"`.

### 2.4 `crowdb-web` CLI flags + rpc log dir fix

`app/crowdb-web/src/main.rs` lines 37-59:

a. Add `--log-dir`, `--log-level`, `--log-max-file-mb`,
   `--log-max-files`, `--log` CLI flags. Default log dir:
   `~/.crowdb-kv/log` (current behavior).
b. Fix the rpc log dir mismatch: replace
   `crowdb_rpc_ffi::init_logging("log", "info", 30, 5, ...)` with
   `crowdb_rpc_ffi::init_logging(&log_dir_str, &cpp_level,
   max_file_mb, max_files, "crowdb-web-rpc")` — using the same
   `log_dir` variable that the Rust tracing init uses.
c. Drive the C++ level from `--log-level` / `RUST_LOG` (§1.3).

Edge cases:
- `crowdb-web` does not link `crowdb-tree-ffi`, so `ct_init_logging`
  is not called. Only `crowdb_rpc_ffi::init_logging` is used.
- The `add_log_stderr("warn")` call at line 59 is currently
  unconditional; make it conditional on `--log-stderr`.

## 3. Observability Design Section

### 3.1 New §3 in `design-crowdb-kv-observability.md`

Add a "Logging" section after §2 (Metrics). Content:

- **Log directory convention** — `--log-dir` CLI flag / config field;
  default `"log"` (relative to CWD) for server binaries, `~/.crowdb-kv/log`
  for `crowdb-web`, per-invocation `cli-log/{command}-{ts}/` for
  `crowdb-cli`. All Rust and C++ logs for a process land under the same
  directory.
- **Rotation + compression** — `RotatingLogWriter` (Rust) and
  `compressing_file_sink` (C++) both use 30 MiB / 5 files defaults,
  gzip-compress rotated files. File naming:
  `{prefix}-{YYYYMMDD-HHMMSS.mmm}-{pid}.log`, rotated to `.log.gz`.
- **Per-stack files** (D1) — Rust and C++ write to separate files in
  the same directory. Prefixes: `{server}` (Rust), `{server}-tree`
  (crowdb-tree C++), `{server}-rpc` (crowdb-rpc C++),
  `{server}-metrics` (metrics log).
- **Metrics log** (D2) — single combined file per process, written by
  Rust via `MetricsRunner`.
- **Level unification** (D3) — `--log-level` drives both Rust and C++
  stacks; `RUST_LOG` overrides the Rust side. Default `info`.
  `--log-stderr` mirrors warn+error to stderr.
- **No separate ops_log** (D4) — ops lines folded into tracing via
  `log_ops_http`.
- **CLI logging** (D5) — `crowdb-cli` writes file logs for all commands
  to a per-invocation directory; `bench` additionally opens a metrics
  log.
- **Log content guidelines** (D6) — readable, rich, self-locating; no
  strict template; consensus-event logs carry `node_id`, `group_id`,
  `slot`, `term` where applicable.
- **C++ logger additivity** — when `ct_init_logging` runs first (kv-server),
  `crowdb_rpc_init_logging` calls `add_log_file` (adds a second file sink
  to the same logger); the rpc sink inherits the tree logger's level. The
  `--log-level` flag sets the tree logger's level, which the rpc sink
  inherits. This is the expected behavior — both C++ stacks run at the
  same level.
- **Extension path** — any future server (e.g. `diskio` server) follows
  the same scheme: `--log-dir` / `--log-level` / `--log-max-file-mb` /
  `--log-max-files` CLI flags, Rust tracing init + C++ FFI init for each
  linked C++ library.

## 4. E2E Log Verification

### 4.1 Test harness changes

`lib/crowdb-test-harness/src/{diskdb,chunkdb}.rs`:

The harness currently redirects child stdout/stderr to
`test-logs/crowdb-*-e2e-{pid}.log`. After R119, the child process also
writes its own rotated log files to the `--log-dir` it is started with.
The harness should:

a. Pass `--log-dir <test_log_dir>` to the child process so its file
   logs land in `test-logs/` alongside the redirected stderr.
b. After the test, assert the expected log file exists (e.g.
   `crowdb-diskdb-*.log`), is non-empty, and contains a "starting"
   line and a "ready" line (or equivalent).

### 4.2 Log content assertions

For each service e2e test, add assertions that grep the log file for:
- A startup line (e.g. "crowdb-diskdb starting" or equivalent).
- A ready line (e.g. "listening on" or "ready" or equivalent).
- For `crowdb-diskdb` / `crowdb-chunkdb`: a
  `crowdb-diskdb-rpc-*.log` / `crowdb-chunkdb-rpc-*.log` file exists
  (crowdb-rpc logging initialized).

## 5. Log Content Remediation

### 5.1 Approach

Per D6: no strict template, but every line should be clear, readable,
and carry enough context. The remediation is a pass over
`tracing::*` / `CRB_LOG_*` call sites, focusing on:

- Opaque error logs: add component + event + identifiers.
- Noisy info logs: downgrade to `debug`.
- Missing context on state transitions: add `node_id`, `group_id`,
  `slot`, `term` where applicable.
- Consensus-event logs: ensure they carry the mandatory signals from
  observability §1.

This is the most labor-intensive work item and may be deferred to a
follow-up if the infrastructure work (items 3-8) is substantial.

## Scope

- `app/crowdb-kv-server/src/cli.rs` — add `--log-dir`, `--log-level`,
  `--log-stderr` CLI flags.
- `app/crowdb-kv-server/src/main.rs` — use `--log-dir` / config
  `log_dir` instead of literal `"log"`; drive C++ level from
  `--log-level` / `RUST_LOG`; add `--log-stderr` support.
- `app/crowdb-diskdb/src/main.rs` — add logging CLI flags; wire
  `crowdb_rpc_ffi::init_logging`; use CLI args for Rust init.
- `app/crowdb-diskdb/src/ddb_config.rs` — no logging config fields
  needed (CLI flags suffice).
- `app/crowdb-chunkdb/src/main.rs` — same as diskdb.
- `app/crowdb-web/src/main.rs` — add logging CLI flags; fix rpc log
  dir mismatch; drive C++ level from `--log-level`.
- `doc/design/kv/design-crowdb-kv-observability.md` — new §3 Logging.
- `lib/crowdb-test-harness/src/diskdb.rs` — pass `--log-dir` to child;
  add log file assertions.
- `lib/crowdb-test-harness/src/chunkdb.rs` — same.
- `lib/crowdb-common/rust/src/logging.rs` — add helper to derive C++
  level from `RUST_LOG` / `--log-level`.

## Complexity

Medium. The infrastructure (`RotatingLogWriter`, `compressing_file_sink`,
FFI bridges, `init_file_and_console_logging_split`) all exist and work.
The work is wiring: adding CLI flags, replacing hardcoded values with
CLI args, adding `crowdb_rpc_ffi::init_logging` calls in diskdb/chunkdb,
fixing the web rpc log dir bug, and writing the observability design
section. The main challenge is the C++ level derivation from `RUST_LOG`
(parsing the first global directive) and the e2e test harness changes.
Log content remediation (item 9) is deferred — it is a separate pass
that does not block the infrastructure unification.

## Test Design

### Unit tests (UT)

- **C++ level derivation** — `logging.rs`: given `RUST_LOG=debug`,
  derive `"debug"`; given `RUST_LOG=crowdb_kv=info`, derive `"info"`;
  given `RUST_LOG=` (empty), derive `"info"` (default); given
  `RUST_LOG` unset and `--log-level warn`, derive `"warn"`. UT.
- **Invalid level rejection** — `--log-level bogus` → startup fails
  with a clear error. UT.

### End-to-end tests (E2E)

- **diskdb log file exists** — start `crowdb-diskdb` via test harness
  with `--log-dir <test_log_dir>` → after startup, a
  `crowdb-diskdb-*.log` file exists in `<test_log_dir>`, is non-empty,
  and contains a startup line. E2E.
- **diskdb rpc log file exists** — same setup → a
  `crowdb-diskdb-rpc-*.log` file exists in `<test_log_dir>`. E2E.
- **diskdb `--log-dir` override** — start with `--log-dir /tmp/r119-ddb`
  → log file lands under `/tmp/r119-ddb/`, not under default. E2E.
- **chunkdb equivalent** — same three assertions for chunkdb. E2E.
- **web rpc log dir fix** — start `crowdb-web` → the
  `crowdb-web-rpc-*.log` file lands in the same directory as the Rust
  `console-web-*.log` (not in `"log"`). E2E.
- **kv-server `--log-level debug`** — start with `--log-level debug`
  → C++ crowdb-tree log contains a debug-level line that would be
  suppressed at `info`. Integration.
- **kv-server `--log-dir` override** — start with `--log-dir /tmp/r119-kv`
  → all log files (Rust + tree + rpc + metrics) land under
  `/tmp/r119-kv/`. E2E.
- **kv-server `--log-stderr warn`** — start with `--log-stderr warn`
  → `warn`+`error` lines appear on stderr; `info` lines do not.
  Integration.
- **diskdb/chunkdb log dir creation failure** — start with
  `--log-dir /nonexistent/path` → startup fails with a clear error.
  Integration.

## Module Structure

```
app/crowdb-kv-server/src/cli.rs       — add --log-dir, --log-level, --log-stderr
app/crowdb-kv-server/src/main.rs      — use CLI args for log dir + C++ level + stderr mirror
app/crowdb-diskdb/src/main.rs         — add logging CLI flags + crowdb-rpc init
app/crowdb-chunkdb/src/main.rs        — add logging CLI flags + crowdb-rpc init
app/crowdb-web/src/main.rs            — add logging CLI flags + fix rpc log dir
lib/crowdb-common/rust/src/logging.rs — add cpp_level_from_rust_log() helper
lib/crowdb-test-harness/src/diskdb.rs — pass --log-dir + add log assertions
lib/crowdb-test-harness/src/chunkdb.rs — pass --log-dir + add log assertions
doc/design/kv/design-crowdb-kv-observability.md — new §3 Logging
```

## Config Extensions

No new config fields. `CrowDBConfig.log_dir` (config.rs:515) already
exists with default `"log"` and is set to `root/log` by `apply_root` —
it just needs to be actually used by the kv-server main.rs logging init.

## Server Wiring

### `crowdb-kv-server` startup (lines 34-74)

1. Parse CLI args (including new `--log-dir`, `--log-level`,
   `--log-stderr`).
2. Determine log dir: `--log-dir` if given, else `config.log_dir`,
   else `"log"`.
3. Derive C++ level: `--log-level` if given, else first global
   directive from `RUST_LOG`, else `"info"`.
4. Init Rust tracing: `init_file_and_console_logging_split(log_dir,
   "crowdb-kv-server", max_file_mb, max_files, rust_filter, "warn")`
   or `init_file_logging(...)` depending on `--log`.
5. Init crowdb-tree: `ct_init_logging(log_dir, cpp_level,
   max_file_mb, max_files, "crowdb-kv-server-tree")`.
6. Init crowdb-rpc: `crowdb_rpc_ffi::init_logging(log_dir, cpp_level,
   max_file_mb, max_files, "crowdb-kv-server-rpc")`.
7. If `--log-stderr`: `ct_add_log_stderr(level)` +
   `crowdb_rpc_ffi::add_log_stderr(level)`.
8. Init metrics: `open_metrics_log(log_dir, "crowdb-kv-server",
   max_file_mb, max_files)`.

### `crowdb-diskdb` startup (lines 72-80)

1. Parse CLI args (new logging flags).
2. Init Rust tracing (same shape as kv-server, prefix
   `"crowdb-diskdb"`).
3. Init crowdb-rpc: `crowdb_rpc_ffi::init_logging(log_dir, cpp_level,
   max_file_mb, max_files, "crowdb-diskdb-rpc")`.
4. If `--log-stderr`: `crowdb_rpc_ffi::add_log_stderr(level)`.
5. On shutdown: `crowdb_rpc_ffi::shutdown_logging()`.

### `crowdb-chunkdb` startup (lines 61-69)

Same as diskdb, prefix `"crowdb-chunkdb"` / `"crowdb-chunkdb-rpc"`.

### `crowdb-web` startup (lines 37-59)

1. Parse CLI args (new logging flags).
2. Determine log dir: `--log-dir` if given, else `~/.crowdb-kv/log`.
3. Init Rust tracing (prefix `"console-web"`).
4. Init crowdb-rpc: `crowdb_rpc_ffi::init_logging(&log_dir_str,
   cpp_level, max_file_mb, max_files, "crowdb-web-rpc")` — using
   `log_dir_str`, NOT the literal `"log"`.
5. If `--log-stderr`: `crowdb_rpc_ffi::add_log_stderr(level)`.

## Open Questions

- **Log content remediation scope (item 9)** — the full pass over all
  `tracing::*` / `CRB_LOG_*` call sites is labor-intensive and may
  not fit in one implementation cycle. Should it be deferred to a
  follow-up requirement, or done incrementally alongside the
  infrastructure work? Recommendation: defer to a follow-up; the
  infrastructure unification (items 3-8) is the prerequisite that
  makes the content pass meaningful (logs must exist and be tunable
  before reviewing their content).
