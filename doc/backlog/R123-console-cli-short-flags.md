<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R123: console — CLI short flag aliases for all subcommands

**Problem**

The `crowdb-cli` has inconsistent short flag coverage. Only `bench rpc`
and the global `--config` (`-p`) define short aliases. All other
subcommands — `bench kv` (24 args), `diskdb`, `disk`, `server`,
`paxos`, `node`, and the global `--ip`/`--port`/`--json` — are long-only.
This makes common operations verbose for interactive users who type
commands frequently (e.g. `bench kv --duration-secs 60 --loader-num 64
--value-size 1024` vs `-d 60 -L 64 -s 1024`).

**Current behavior + impact**

- `bench rpc` already has short flags (`-d`, `-L`, `-c`, `-e`, `-t`,
  `-n`, `-s`, `-m`, `-P`). These were added ad-hoc and are the
  precedent for the rest.
- `bench kv` has 24 arguments, all `#[arg(long)]` — no short aliases.
- `diskdb`, `disk`, `server`, `paxos`, `node` subcommands — all
  `#[arg(long)]`.
- `main.rs` global args: `--ip`, `--port`, `--json` are long-only;
  `--config` has `-p`.

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
only — clap does not support multi-char short flags).

**One-line summary:** Add single-char short flag aliases to every
`#[arg]` across all `crowdb-cli` subcommands, following the `bench rpc`
precedent.

**Numbered work items**

1. **Global args (`main.rs`)** — add short aliases to `--ip` (`-i`),
   `--port` (`-o`), `--json` (`-j`). `--config` already has `-p`.
   Avoid conflicts: `-p` is taken by `--config`, `-h` by `--help`.

2. **`bench kv` args (`commands/bench.rs` KvArgs)** — add short aliases
   to all 24 args. Map common ones to match `bench rpc` where the arg
   name overlaps (`-d` for `--duration-secs`, `-L` for `--loader-num`,
   `-c` for `--connections`, `-s` for `--value-size`, `-m` for
   `--metrics-interval`). Assign unique chars for the rest (`-w` for
   `--workload`, `-M` for `--mode`, `-k` for `--key-space`, etc.).

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

8. **Short flag conflict audit** — run `crowdb-cli <sub> --help` for every
   subcommand to verify no clap panic on duplicate short flags. Clap
   enforces uniqueness at parse time (panics in debug). Global args
   (`-p`, `-i`, `-o`, `-j`) are `global = true` so they occupy their
   chars across all subcommands — subcommand shorts must avoid these.

**Edge cases at a glance**

- Short flag conflict between global and subcommand args → clap panics
  at startup; caught by `--help` smoke test.
- Short flag conflict between parent and child subcommand args → same;
  caught by smoke test.
- Args with no obvious mnemonic (e.g. `coalesce_drain_threshold`) →
  pick the first unused consonant; document in help text.
- `--json` is a bool flag → `-j` works as a switch (no value needed).

**Dependencies**

None. Self-contained within `app/crowdb-cli/src`.

**Acceptance**

- `crowdb-cli bench kv --help` shows short aliases for all 24 args.
  Integration test (smoke).
- `crowdb-cli bench rpc --help` still shows the existing short aliases
  (no regression). Integration test (smoke).
- `crowdb-cli diskdb <sub> --help` shows short aliases for all args in
  every diskdb subcommand. Integration test (smoke).
- `crowdb-cli disk <sub> --help` shows short aliases for all args in
  every disk subcommand. Integration test (smoke).
- `crowdb-cli server <sub> --help` shows short aliases for all args in
  every server subcommand. Integration test (smoke).
- `crowdb-cli paxos <sub> --help` shows short aliases for all args in
  every paxos subcommand. Integration test (smoke).
- `crowdb-cli node <sub> --help` shows short aliases for all args in
  every node subcommand. Integration test (smoke).
- `crowdb-cli --help` shows `-i`, `-o`, `-j`, `-p` for global args.
  Integration test (smoke).
- No clap panic (duplicate short flag) for any subcommand — verified by
  running `--help` on every subcommand. Integration test (smoke).
- `pixi run cargo fmt --all -- --check` passes. Unit test.
- `pixi run cargo clippy -p crowdb-cli -- -D warnings` passes. Unit test.

**Open Questions**

1. **Short flag for `--server-port` in `bench rpc`**: currently `-P`
   (uppercase) because `-p` is taken by global `--config`. Alternative:
   rename global `--config` short from `-p` to `-c` (but `-c` is used
   by `--connections` in `bench rpc`/`bench kv`). Keeping `-P` for
   `--server-port` is the least-disruptive option. Decision needed:
   keep `-P`, or reshuffle global shorts?
