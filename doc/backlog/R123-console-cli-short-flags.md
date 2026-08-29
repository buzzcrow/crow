<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R123: console — CLI short flag aliases for all subcommands

**Problem**

The `crowdb-cli` has inconsistent short flag coverage. Only `bench rpc`
(partially: 9 of 13 args) and the global `--config` (`-p`) define short
aliases. All other subcommands — `bench kv` (29 args), `diskdb`, `disk`,
`server`, `paxos`, `node`, `cluster`, `rack`, `replica`, `store`, `kv`,
`disk_group`, and the global `--ip`/`--port`/`--json` — are long-only.
This makes common operations verbose for interactive users who type
commands frequently (e.g. `bench kv --duration-secs 60 --loader-num 64
--value-size 1024` vs `-d 60 -L 64 -s 1024`).

**Current behavior + impact**

- `bench rpc` has short flags on 9 of 13 args (`-d`, `-L`, `-c`,
  `-e`, `-t`, `-n`, `-s`, `-m`, `-P`). The remaining 4 (`mode`,
  `quickack`, `run_id`, `log_dir`) are long-only. These were added
  ad-hoc and are the precedent for the rest.
- `bench kv` has 29 arguments, all `#[arg(long)]` — no short aliases.
- `diskdb`, `disk`, `server`, `paxos`, `node`, `cluster`, `rack`,
  `replica`, `store`, `kv`, `disk_group` subcommands — all
  `#[arg(long)]`.
- `main.rs` global args: `--ip`, `--port`, `--json` are long-only;
  `--config` has `-p`.
- Total: ~154 `#[arg(long)]` args with no short alias across the CLI
  (165 total `#[arg]`s minus 10 with `short = `).

The impact is ergonomic: long flags are fine for scripts and
documentation, but interactive users benefit from short aliases for
frequently-used args.

**Design pointers**

No formal design doc covers CLI ergonomics. This is a self-contained
cleanup within `app/crowdb-cli/src`.

**Use scenarios**

- Operator runs a quick KV bench: `crowdb-cli bench kv -d 60 -L 64 -s 1024`
  instead of typing `--duration-secs 60 --loader-num 64 --value-size 1024`.
- Operator deploys a server: `crowdb-cli server deploy -n node1 -r 9920 -R 9930`
  instead of `--node node1 --rest-port 9920 --rpc-port 9930`.
- Operator adds a paxos group: `crowdb-cli paxos add -s 1 -g 1 -r 100 -N 1,2,3`
  instead of `--store-id 1 --group-id 1 --replica-id 100 --nodes 1,2,3`.
- Operator runs `--help` and sees both short and long forms for every
  argument, consistent across all subcommands.

**Solution**

Add `short = '<char>'` to `#[arg(...)]` attributes for all arguments
across all CLI subcommands. Use clap's `short` attribute (single char
only — clap does not support multi-char short flags). **Conflict rule:
if the natural mnemonic char for an arg is already taken (by a global
arg or another arg in the same subcommand), do NOT provide a short
alias for that arg — leave it long-only.** Do not reshuffle existing
shorts to free up a char, and do not force an awkward uppercase/
unrelated char just to give every arg a short. The goal is ergonomic
aliases for the common args, not 100% coverage.

**One-line summary:** Add single-char short flag aliases to every
`#[arg]` across all `crowdb-cli` subcommands, following the `bench rpc`
precedent.

**Numbered work items**

1. **Global args (`main.rs`)** — add short aliases to `--ip` (`-i`),
   `--port` (`-o`), `--json` (`-j`). `--config` already has `-p`.
   Avoid conflicts: `-p` is taken by `--config`, `-h` by `--help`.

2. **`bench kv` args (`commands/bench.rs` KvArgs)** — add short aliases
   to all 29 args. Map common ones to match `bench rpc` where the arg
   name overlaps (`-d` for `--duration-secs`, `-L` for `--loader-num`,
   `-c` for `--connections`, `-s` for `--value-size`, `-m` for
   `--metrics-interval`). Assign unique chars for the rest (`-w` for
   `--workload`, `-M` for `--mode`, `-k` for `--key-space`, etc.).
   Also complete `bench rpc`'s 4 remaining long-only args (`mode`,
   `quickack`, `run_id`, `log_dir`).

3. **`diskdb` args (`commands/diskdb.rs`)** — add short aliases to all
   args across all subcommands (Usage, ScanStatus, Scan, Recalc,
   Compact, Rebuild, SetStatus, SetDgStatus, Deploy, Restart, Stop,
   Delete).

4. **`disk` args (`commands/disk.rs`)** — add short aliases to all args
   across Add, Remove, List, Move.

5. **`server` args (`commands/server.rs`)** — add short aliases to all
   args across Deploy, Restart, Stop.

6. **`paxos` args (`commands/paxos.rs`)** — add short aliases to all
   args across Add, Remove, List, Inspect.

7. **`node` args (`commands/node.rs`)** — add short aliases to all args
   across Add, Remove.

8. **`cluster`, `rack`, `replica`, `store`, `kv`, `disk_group` args**
   (`commands/{cluster,rack,replica,store,kv,disk_group}.rs`) — these
   subcommands were not in the original work-item list but are present
   in the CLI and are all long-only. Add short aliases to all their
   args.

9. **Short flag conflict audit** — run `crowdb-cli <sub> --help` for every
   subcommand to verify no clap panic on duplicate short flags. Clap
   enforces uniqueness at parse time (panics in debug). Global args
   (`-p`, `-i`, `-o`, `-j`) are `global = true` so they occupy their
   chars across all subcommands — subcommand shorts must avoid these.
   Per the conflict rule, any arg whose natural mnemonic conflicts
   with a global or sibling char is left long-only (no short alias);
   the audit confirms no clap panic and documents which args were
   skipped and why.

**Edge cases at a glance**

- Short flag conflict between global and subcommand args → **skip the
  short alias for the subcommand arg** (leave it long-only); do not
  reshuffle globals. No clap panic because no duplicate is introduced.
- Short flag conflict between parent and child subcommand args → same:
  skip the short alias for the conflicting arg.
- Args with no obvious mnemonic (e.g. `coalesce_drain_threshold`) →
  pick the first unused consonant; if none is available, leave long-
  only (do not force an unrelated char).
- `--json` is a bool flag → `-j` works as a switch (no value needed).
- An arg whose natural mnemonic conflicts but has an established
  non-conflicting uppercase short already in use (e.g. `--server-port`
  `-P`) → keep the existing short; do not change it.

**Dependencies**

None. Self-contained within `app/crowdb-cli/src`.

**Acceptance**

- `crowdb-cli bench kv --help` shows short aliases for every arg whose
  natural mnemonic does not conflict with a global or sibling char;
  conflicting args are long-only (documented in the audit). Integration
  test (smoke).
- `crowdb-cli bench rpc --help` shows short aliases for the 9 existing
  args plus any of the 4 currently long-only (`mode`, `quickack`,
  `run_id`, `log_dir`) whose mnemonic does not conflict; conflicting
  ones stay long-only. Integration test (smoke).
- `crowdb-cli diskdb <sub> --help` shows short aliases for all
  non-conflicting args in every diskdb subcommand. Integration test
  (smoke).
- `crowdb-cli disk <sub> --help` shows short aliases for all
  non-conflicting args in every disk subcommand. Integration test
  (smoke).
- `crowdb-cli server <sub> --help` shows short aliases for all
  non-conflicting args in every server subcommand. Integration test
  (smoke).
- `crowdb-cli paxos <sub> --help` shows short aliases for all
  non-conflicting args in every paxos subcommand. Integration test
  (smoke).
- `crowdb-cli node <sub> --help` shows short aliases for all
  non-conflicting args in every node subcommand. Integration test
  (smoke).
- `crowdb-cli cluster <sub> --help`, `crowdb-cli rack <sub> --help`,
  `crowdb-cli replica <sub> --help`, `crowdb-cli store <sub> --help`,
  `crowdb-cli kv <sub> --help`, `crowdb-cli disk_group <sub> --help`
  show short aliases for all non-conflicting args. Integration test
  (smoke).
- `crowdb-cli --help` shows `-i`, `-o`, `-j`, `-p` for global args.
  Integration test (smoke).
- No clap panic (duplicate short flag) for any subcommand — verified by
  running `--help` on every subcommand. Integration test (smoke).
- The conflict audit (work item 9) documents every arg left long-only
  because of a char conflict, with the conflicting char named.
  Reviewer check (not a test).
- `pixi run cargo fmt --all -- --check` passes. Unit test.
- `pixi run cargo clippy -p crowdb-cli -- -D warnings` passes. Unit test.

**Decisions**

1. **Conflict rule — skip the short alias, do not reshuffle.** If an
   arg's natural mnemonic char is already taken (by a global arg or a
   sibling in the same subcommand), leave that arg long-only. Do not
   reshuffle existing global/subcommand shorts to free up a char, and
   do not force an unrelated or awkward char just to give every arg a
   short. Rationale: the goal is ergonomic aliases for common args,
   not 100% coverage; reshuffling breaks existing scripts and muscle
   memory. Established non-conflicting shorts (e.g. `--server-port`
   `-P`) are kept as-is.
2. **`--server-port` in `bench rpc` keeps `-P`.** `-P` is non-
   conflicting (uppercase, distinct from global `-p`); no reshuffle
   needed. Resolves the original open question.