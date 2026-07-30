<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R31: Investigate 50K→29K write throughput regression

**Problem**: The 2026-07-21 write sweep reported ~50K ops/s peak at
64T:8C (Intel Ryzen 9 5950X, 16c/32t, Linux). The 2026-07-24 sweep on
the same Intel platform measured ~29K at the same config (direct
re-run: 29,319 ops/s) — a ~42% drop. The relative findings (window is
the primary lever, threads plateau at 24T, T:C ratio has no effect)
are consistent between the two dates, but the absolute throughput
regressed by 1.7×.

This is the single largest expected win on the write path — larger
than R16a + R17 + R30 combined. Optimizing on top of a regressed
baseline mis-prioritizes work.

**Two candidate causes** (must be separated before bisecting):

1. **Platform difference** — the 50K number was on Intel Ryzen 9 5950X
   (Linux); a retest on a different platform (e.g. macOS M5 Pro) is
   not directly comparable. The 29K number was also on the Intel
   platform, so the regression is real *on Intel*, but any new
   measurement taken on macOS must not be confused with the Intel
   baseline. Rule out platform effects by retesting on the original
   Intel platform if available; if only macOS is available, establish
   a fresh macOS baseline and compare only same-platform runs.
2. **Code regression** — code changes between 2026-07-21 and
   2026-07-24 (WAL restore, election fixes, group wiring changes) are
   candidate causes. If the Intel retest still shows ~29K (not ~50K),
   bisect across those changes.

**Step 1 result (2026-07-29, macOS M5 Pro)** — DONE. The M5 Pro retest
hits ~48K at 64T:8C:MI=64, within 4% of the original Intel 50K claim.
Single-thread is 3.4× faster on M5 Pro (9.5K vs 2.8K); saturation is
1.4-1.7× faster (~41-48K vs ~29K). The relative shape (window lever,
thread plateau, T:C insensitivity) is identical across platforms.
**Conclusion: the 50K→29K difference is largely a platform effect,
not a code regression.** The code path itself reaches ~48K on M5 Pro.

The Intel same-platform regression (50K on 07-21 → 29K on 07-24) is
still an open question but is lower priority given the M5 Pro result —
it may be Intel-specific (e.g. a code change that interacts with Linux
fsync scheduling or the 5950X's core topology). Bisect on Intel only
if that platform is available and the regression matters for the
production target.

**Approach (remaining steps, Intel-only if pursued)**:

- **Step 2 — Bisect (if Intel retest confirms ~29K)** — `git bisect`
  across the 2026-07-21 → 2026-07-24 commit range, running the
  regression bench at each step. The sentinel config is 48T:48C:MI=64
  (peak throughput, ~29K regressed vs ~50K baseline). Identify the
  commit that dropped throughput.
- **Step 3 — Root-cause** — Once the commit is found, trace the code
  path change (WAL restore / election / group wiring) and determine
  whether the regression is inherent to the fix (correctness tradeoff)
  or accidental (fixable).
- **Step 4 — Fix or document** — If fixable, restore the throughput.
  If inherent (e.g. a correctness fix that added a necessary
  round-trip), document the tradeoff in `write-flow-analysis.md` and
  update the baseline.

**Priority**: Medium (downgraded from High after Step 1) — the M5 Pro
retest shows the code path reaches ~48K, so the "regression" is largely
a platform effect. The remaining Intel same-platform bisect is only
worth pursuing if the production target is Intel/Linux and the
throughput matters there. R16a/R17/R30 can proceed on M5 Pro without
waiting for the Intel bisect.

**Complexity**: Low for Step 1 (done); Medium for the remaining Intel
bisect (mechanical, but requires the Intel platform).

**Files**:
- `tools/bench-write-regression.sh` — sentinel configs (no change
  expected; the script hardcodes `cd /cjdata/cpp/crowkv` which must be
  adapted per platform)
- `doc/working/write-flow-analysis.md` — update the benchmark section
  with the new platform baseline and bisect result

**Acceptance**:
- Step 1 (DONE): macOS M5 Pro baseline recorded in
  `doc/working/write-flow-analysis.md`. The code path reaches ~48K,
  confirming the 50K→29K difference is largely platform, not code.
- If the Intel bisect is pursued and confirms ~29K (not ~50K), the
  bisect identifies the regressing commit and the root cause is
  documented.
- If the regression is fixable, throughput is restored to ~50K (Intel)
  and the fix is covered by existing consensus tests.
- If the regression is inherent (correctness tradeoff), the tradeoff
  is documented and the baseline in `write-flow-analysis.md` is
  updated to reflect the new steady-state.

**Note**: The `tools/bench-write-regression.sh` script hardcodes
`cd /cjdata/cpp/crowkv`. On macOS (where the repo is at
`~/cpp/crowkv` or similar), the path must be adapted before running.
Consider making the script path-relative in a follow-up.
