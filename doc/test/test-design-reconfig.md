# CrowKV - Test Design: Reconfiguration

Depends on: [`test-design.md`](test/test-design.md), [`design-reconfiguration.md`](design/design-reconfiguration.md)
Satisfies: [requirement.md §9.1](requirement.md#91-reconfiguration), [requirement.md §9.2](requirement.md#92-rolling-upgrade)

Invariants for membership change and rolling upgrade.

## 1. Invariants

| ID | Claim | Trigger | Ref |
|---|---|---|---|
| RC1 | Joint config both-quorum | Every decision during joint | [`design-reconfiguration.md`](design/design-reconfiguration.md) §2 |
| RC2 | New member catch-up before voting | `ConfigChange(C_new)` proposed | [`design-reconfiguration.md`](design/design-reconfiguration.md) §4 |
| RC3 | No split-brain during transition | Any two quorums intersect | [`design-reconfiguration.md`](design/design-reconfiguration.md) §8 |
| RC4 | Rolling upgrade one version step | Mixed-version cluster | [`requirement.md`](requirement.md) §9.2 |

## 2. Unit Tests

| Module | Test | Assertion |
|---|---|---|
| `reconfig` | `joint_quorum_requirement` | 3→5 needs both old and new majority |
| `reconfig` | `catchup_before_voting` | New node non-voting until contiguous_applied caught up |
| `reconfig` | `leader_transfer_before_removal` | Current leader transferred before final config |

## 3. Failure Injection

| Failure | Sim | Invariant | Assertion |
|---|---|---|---|
| Catch-up timeout | hold new member's apply | RC2 | Reconfig rolls back to `C_old`, no partial state |
| Leader crash mid-joint | crash leader after joint chosen, before final | RC1, RC3 | New leader resumes from `JointActive`, finalizes |
| Snapshot install network failure | drop chunks mid-stream | (snapshot resumability) | Resume from last successful offset |
| Mixed-version protocol mismatch | one node N+2 (out of compat window) | RC4 | Out-of-compat node refused; alert raised |

## 4. Integration Scenarios

**S-RC1 — 3 → 5 online:** add two members one at a time, zero write unavailability, `compare()` equal across all 5.

**S-RC2 — 5 → 3 online:** remove two members one at a time, including current leader (forces leader transfer).

**S-RC3 — Rolling upgrade:** restart one node at a time with N+1 binary, mixed-version traffic passes, `compare()` equal at end.

**S-RC4 — Failed catch-up rollback:** new member's apply blocked, `catchup_timeout` fires, reconfig rolls back, group remains 3.

## 5. Resolved Decisions

- **Rolling-upgrade test scope:** consensus protocol compatibility only (matches `plan-reconfig.md` M4 decision; no WAL/snapshot version compat in test scope).
- **Reconfig under load:** crowbench keeps writing during reconfig; verifying zero divergence under live traffic is the point of G5.
