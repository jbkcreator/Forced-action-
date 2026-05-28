# PRD — Attribution-driven Cora decisions behind a safe 10% rollout

> Companion docs: `CONTEXT.md` (glossary), `docs/adr/0002-attribution-rollout-and-autorollback.md` (architecture + rejected alternatives).

## Problem Statement

The merged attribution + revenue-signal system can now tell Cora *why* a
subscriber is likely to convert. But switching every Cora decision over to it
at once is dangerous: if the attribution-driven logic is worse than today's
behavior, we'd degrade conversions across the whole base before we noticed. We
need to expose the new logic to a small, fixed slice of traffic, prove it's not
hurting, and automatically retreat to safe behavior — fast — if it is.

## Solution

Run the attribution-driven path as a **single global rollout test**
(`cora_attribution_v1`) over **10%** of eligible Cora traffic; the other **90%**
stays on the existing control path. Assignment is deterministic per subscriber,
so a subscriber never flips arms mid-test. Every decision logs which arm it
took. A monitor compares the two arms' conversion rates over a rolling 48-hour
window and, if the attribution arm loses by more than 2 standard deviations,
auto-rolls-back by flipping the test's status — returning all traffic to control
within 5 minutes, through the existing A/B engine rather than code/DB surgery.

## User Stories

1. As a product owner, I want only 10% of eligible Cora traffic on the attribution-driven path, so that a bad change can't hurt the whole base.
2. As a product owner, I want the other 90% to stay on the proven control path, so that most revenue is unaffected during the test.
3. As a subscriber, I want to stay in the same arm for the whole test, so that my experience is consistent and not flip-flopping.
4. As a data analyst, I want every Cora decision tagged control vs attribution-driven, so that I can compare arm performance later.
5. As a data analyst, I want both arms' assignments recorded, so that I can compute a real control conversion rate (not just the variant's).
6. As a growth engineer, I want eligibility limited to subscribers who actually have attribution context, so that the comparison isn't diluted by subscribers the variant can't act on.
7. As Cora, I want a single `use_attribution_path` flag from the decision hierarchy, so that all decision points read one consistent source of truth.
8. As Cora, I want the attribution arm to feed attribution context + revenue signal into personalization, so that framing and message selection reflect why the subscriber converts.
9. As a reliability owner, I want a monitor that runs every 2–3 minutes, so that a losing variant is detected within 5 minutes.
10. As a reliability owner, I want the monitor to use a rolling 48-hour window, so that the comparison reflects current behavior, not stale history.
11. As a reliability owner, I want a minimum of 30 assignments per arm before judging, so that we don't roll back on statistical noise.
12. As a reliability owner, I want rollback to trigger only when the attribution arm's conversion rate is >2σ below control, so that the rule is objective and defensible.
13. As a reliability owner, I want rollback to set `AbTest.status='rolled_back'`, so that there's a single observable kill switch distinct from a normal test completion.
14. As a reliability owner, I want rollback to return every assigned subscriber to control on their next decision, so that the unsafe path stops immediately.
15. As a reliability owner, I want rollback to complete within 5 minutes of detection, so that exposure to a bad variant is bounded.
16. As an on-call engineer, I want a founder alert when rollback fires, so that a human is aware without watching dashboards.
17. As Cora, I want a learning card written on rollback, so that future decisions remember the lesson.
18. As an analyst, I want revenue and message-outcome metrics logged alongside the rollback decision, so that I have context even though they don't drive the trigger.
19. As a developer, I want the rollout to reuse the merged attribution/revenue-signal service for conversion labeling, so that no parallel tracking system is created.
20. As a developer, I want a dedicated assignment function for the rollout, so that the existing a/b message-swap engine and its tests are untouched.
21. As a QA engineer, I want a simulated bad-variant test, so that I can prove the rollback rule triggers and the kill switch flips.
22. As a QA engineer, I want the test to confirm new decisions stop using the attribution path after rollback, so that I know rollback actually changes runtime behavior.
23. As a developer, I want assignment to be deterministic via a hash, so that the split is reproducible and testable without randomness.
24. As an operator, I want the 10% cap enforced by the existing guardrail, so that traffic can never silently exceed the configured ceiling.

## Implementation Decisions

### Modules

**`assign_rollout_arm` (new deep module, in `ab_engine`)**
- Interface: `assign_rollout_arm(subscriber_id, test_name, db) -> 'variant' | 'control' | None`.
- Deterministic md5 hash `h = int(md5(f"{test_name}{subscriber_id}"),16) % 100`; `h < traffic_pct → 'variant'`, else `'control'`. Returns `None` only when the test is missing or not `active`.
- Records **both** arms as `AbAssignment` rows (`variant` column = `'variant'`/`'control'`).
- Idempotent: existing assignment returned as-is.
- `assign_variant` (the a/b message-swap function) is **not modified**.

**`should_rollback_rollout` (new, in `ab_engine`)**
- Interface: `should_rollback_rollout(test_name, db, *, window_hours=48, min_per_arm=30) -> bool`.
- Pulls `AbAssignment` rows for the test within the rolling window; computes per-arm conversion rate (`outcome == 'converted'`).
- Floor: both arms ≥ `min_per_arm` else `False` (fail-safe hold).
- One-sided proportion z-test; returns `True` only when `variant` rate < `control` rate and `|z| > 2.0`. Keeps the `p_pool ∈ {0,1}` / `se==0` guards.

**`decision_hierarchy` (modified)**
- Step 5 calls `assign_rollout_arm` for the rollout test; adds `use_attribution_path: bool` (= arm is `'variant'`) to state and `hierarchy_path` (`rollout:variant` / `rollout:control`).
- Step-6 kill-switch behavior unchanged (it is **not** the rollback mechanism).

**Personalization/context chokepoint (modified)**
- When `use_attribution_path` is true, `build_personalization_fields` receives `attribution_data`; otherwise `None`. First and only behavior consumer in this PRD.

**Attribution → outcome labeling (modified in attribution service path)**
- When the merged attribution service records a `conversion_attribution_event` for a subscriber, set that subscriber's active `cora_attribution_v1` `AbAssignment.outcome='converted'`.

**`cora_attribution_rollback_check` (new task/cron)**
- Scoped to `cora_attribution_v1`. Calls `should_rollback_rollout`; on trigger sets `AbTest.status='rolled_back'`, fires founder alert, writes a learning card.
- Cron cadence 120–180s.

**Rollout test registration**
- `get_or_create_test('cora_attribution_v1', traffic_pct=10, ...)`; capped by `ab_test_traffic_cap.max_pct=10`.

### Schema changes
- No new tables. `AbTest.status` gains a new allowed value `'rolled_back'` (terminal, distinct from `'completed'`). `AbAssignment.variant` carries `'variant'`/`'control'` for rollout-type tests.

### Architectural decisions (see ADR-0002)
- Single global subscriber-level test; eligibility = non-empty `get_attribution_context()`.
- Rollback = `AbTest.status='rolled_back'`; `assign_rollout_arm` then returns control for all (including already-assigned) on next decision.
- Conversion rate is the sole trigger metric; revenue/message outcomes monitored only.
- 5-min latency met by cron cadence; flip itself is instantaneous and read live by the hierarchy.

## Testing Decisions

Good tests assert **external behavior**, not internals: given seeded assignment
data, does the detector flip status and does a subsequent decision route to
control — not which private helper was called.

**Modules tested:**
- `assign_rollout_arm` — determinism (same subscriber → same arm), ~10/90 split shape, both arms recorded, `None` when test inactive. Mirrors `test_ab_engine.py` unit style (`mock_db`).
- `should_rollback_rollout` + `cora_attribution_rollback_check` — the simulated-drop scenario: seed an active `cora_attribution_v1` with ≥30 control + ≥30 variant assignments where variant ≪ control (>2σ), run the detector, assert `status='rolled_back'`, founder alert called, learning card written; below-floor case leaves it active; dry-run leaves it active. Mirrors `test_ab_rollback.py` using the `fresh_db` Postgres fixture and real `AbTest`/`AbAssignment`/`Subscriber` rows.
- Post-rollback routing — after the flip, a fresh `assign_rollout_arm` / `run_decision_hierarchy` returns control / `use_attribution_path=False`.

**Prior art:** `tests/test_ab_rollback.py` (seed/run/assert + cleanup), `tests/test_ab_engine.py` (deterministic-assignment unit tests), `tests/test_attribution.py` (attribution service).

## Out of Scope

- Wiring the `use_attribution_path` flag into routing/urgency/upsell/wallet/lock/AP timing graphs (this PRD covers the context/personalization chokepoint only; graphs adopt the flag incrementally later).
- Auto-promotion of the variant after a clean 48h (fail-safe hold only — no auto-expand).
- Composite/revenue-based rollback triggers.
- Ramp beyond 10% (cap is fixed at the guardrail).
- Per-decision-type tests.
- Dashboard/Redis mirroring of rollout state (optional, visibility-only).

## Further Notes

- ≤5-min rollback depends on the sub-5-min cron actually running — monitor cron liveness; a dead host degrades rollback latency.
- Very low-traffic 48h windows may never reach the ≥30/arm floor → variant keeps running (fail-safe hold), never auto-promoted.
- `gh` CLI is not installed locally; submit this as a GitHub issue on `jbkcreator/Forced-action-` manually (or install `gh` and run `gh issue create -F docs/prd_attribution_rollout.md`).
