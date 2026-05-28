# 0002 — Attribution-driven Cora rollout & auto-rollback

## Status
Accepted 2026-05-27.

## Context

The merged attribution + revenue-signal system must reach Cora decisions
behind a safe 10% rollout, with automatic rollback if the attribution-driven
path underperforms control by >2σ, completing within 5 minutes of detection.

The repo already has A/B machinery:
- `ab_engine.assign_variant` — deterministic md5 split into `a`/`b` inside a
  `traffic_pct` slice; everyone outside gets `None`.
- `ab_engine.should_rollback` — 2σ z-test, hardcoded "variant **a** is losing",
  gated on 200 cumulative assignments.
- `ab_rollback_check` — **daily** cron firing `should_rollback`.
- `decision_hierarchy` step 5 (`ab_variant_assign`) and step 6
  (`kill_switch_status`, which **blocks** an action — sets `action_allowed=False`).

None of these match the rollout shape (one variant vs an unrecorded control),
the time-windowed trigger, or the ≤5-min latency.

## Decision

1. **Single global rollout test** `cora_attribution_v1` (`AbTest`, `traffic_pct=10`,
   capped by `ab_test_traffic_cap.max_pct=10`). One subscriber-level assignment
   read by every Cora decision.

2. **Dedicated `assign_rollout_arm(subscriber_id, test_name, db)`** — same md5
   determinism, but `h < traffic_pct → 'variant'` else `'control'`, and **both
   arms are recorded** as `AbAssignment` rows. `assign_variant` and all
   message-swap tests are left untouched.

3. **Eligibility** = Cora-touched subscriber with non-empty
   `get_attribution_context()`. Both arms therefore have attribution signal;
   only the *use* of it differs.

4. **Branch point** = `decision_hierarchy` computes `use_attribution_path`
   (= arm is `variant`) as the single source of truth. First consumer is the
   context/personalization chokepoint (`build_personalization_fields`
   attribution_data passed vs `None`); routing/urgency/timing graphs adopt the
   flag incrementally.

5. **Outcome labeling** — an `AbAssignment` is marked `outcome='converted'` when
   the merged **attribution service** records a `conversion_attribution_event`
   for an in-test subscriber. No duplicate conversion tracking.

6. **Trigger metric** = per-arm conversion rate only. 2σ one-sided proportion
   z-test, variant is the suspect. Requires **≥30 per arm within the rolling
   48h window**; below the floor → hold (no rollback, no promote). Revenue and
   message-outcome metrics are logged for context but excluded from the trigger.

7. **Rollback** = set `AbTest.status='rolled_back'` (terminal, distinct from
   `completed`). `assign_rollout_arm` then returns `control` for everyone —
   including already-assigned subscribers — so the next decision is control.
   This is the engine's own state transition, not manual DB tampering.

8. **Detector** = new `cora_attribution_rollback_check` cron at **120–180s**
   cadence, scoped to `cora_attribution_v1`, rolling 48h window. Flip is
   instantaneous; worst-case detection lag < 5 min.

## Alternatives considered

- **Overload `assign_variant` to record control rows** — rejected: changes its
  contract and forces rewriting `test_ab_engine` + re-verifying every
  message-swap caller.
- **Reuse the daily `ab_rollback_check`** — rejected: up to 24h detection lag,
  fails the ≤5-min bound.
- **Use the step-6 kill switch as the off-switch** — rejected: it blocks the
  action entirely (`action_allowed=False`) instead of returning traffic to the
  control path.
- **Mark rollback as `status='completed'` via `complete_test`** — rejected:
  conflates auto-rollback with a normal test win and writes a spurious playbook
  recommendation.
- **Composite / revenue-per-arm trigger** — rejected: heavy-tailed, unstable
  under 2σ on small 48h samples.
- **Per-decision-type tests** — rejected: a subscriber could be variant for one
  decision and control for another, breaking "same group during the test".

## Consequences

- Control conversion rate is measurable (both arms recorded), so the 2σ test is
  well-defined.
- A new terminal status (`rolled_back`) lets dashboards/tests distinguish auto-
  rollback from a normal completion.
- The ≤5-min guarantee depends on a sub-5-min cron actually running; if the
  cron host is down, rollback latency degrades. Monitor cron liveness.
- The 48h-window + ≥30/arm floor means very low-traffic periods may never reach
  a verdict — the variant keeps running (fail-safe hold), not auto-promoted.
- Simulated-drop test (`fresh_db`, real rows, mirrors `test_ab_rollback.py`)
  seeds variant ≪ control and asserts `status='rolled_back'` + a fresh
  assignment returns `control`. Postgres-gated (skips without `DATABASE_URL`),
  consistent with existing ORM tests.
