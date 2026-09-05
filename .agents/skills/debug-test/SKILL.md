---
name: debug-test
description: Diagnose a failing test from first divergence to root cause.
subagent: true
triggers:
  - user
  - model
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Debug Test

Do not increase timeouts, suppress errors, weaken assertions, add caller-side
retries, or otherwise make only the symptom pass.

1. Check proxy variables, stale processes, logs, state, and build artifacts.
2. Reproduce the single test. If it passes alone, inspect suite isolation,
   ports, and leaked processes.
3. List setup, action, and assertion steps. Find the first divergence using
   logs, APIs, persisted data, and timing.
4. Classify it as code/design, timing/order, or environment.
5. For an unexplained process exit, analyze its crash report before changing code.
6. Add focused temporary instrumentation only when evidence is insufficient;
   remove it afterward.
7. Fix the earliest upstream cause. Rerun the test, affected suite, and quality
   gate. Add a regression test for a code bug.

Useful console/KV-client signals:

- `new standalone instance created`: transport was not shared.
- `no mgmt seeds configured`: invalid empty seed set.
- `no KV servers deployed`: cluster is not initialized.
- `no rpc endpoint resolved` or `no group-0 endpoint found`: no usable
  group-0 endpoint.
- `group-0 query failed`: RPC failed and config fallback was used.
- `topology refresh failed` or `topology discovery failed`: inspect whether
  the seed is a store listen address or node RPC address.

For UI tests, follow `/console-ui-e2e`. During requirement work, follow
`/implement-requirement` retry and blocking rules.
