# FA — Q9 Closed-Loop Scoring Automation — Implementation Plan

**Date:** 2026-07-13
**Branch analysed:** `dev` @ `5d0b301`
**Client question (Q9):** "Funnel analytics + outcome connectors are merged — walk me through the closed loop: conversion data → what changes automatically (copy, pricing, scoring) → measured again next week. If any link in that chain is still manual, name it and give me the fix date."

---

## 1. What Q9 actually asks

A self-improving loop with three adjustment links:

| Link | Auto today? | By design? |
|---|---|---|
| **Copy** | No | Manual by design — copy is a marketing decision, not data-driven auto-generation. |
| **Pricing** | No | Manual by design — pricing is a business decision. |
| **Scoring** | **Partially** — pipeline built, final apply + scheduling missing | Intended to be automatic. This is the only real gap. |

Measurement (funnel analytics, `webhook_events`) is already live and automatic. So the only engineering work Q9 implies is: **finish + schedule the scoring re-tune loop.**

---

## 2. What is already built (verified in code)

The CDS re-tune is a 6-stage pipeline. Five stages exist and work; they are just not scheduled and not wired end to end.

| Stage | Purpose | File / evidence | Status |
|---|---|---|---|
| **A** | Capture outcomes into `outcome_candidates` / `deal_outcomes` | `src/connectors/` | ✅ built, running (cron) |
| **B** | Build per-(property,vertical,date) training CSV with outcome label | [scoring_training_data.py](src/services/scoring_training_data.py) | ✅ built, **manual CLI** |
| **C** | Per-vertical logistic fit → JSON artifact of proposed `VERTICAL_WEIGHTS` | [scoring_fit.py](src/services/scoring_fit.py) | ✅ built, **manual CLI** |
| **D** | Shadow-rescore all properties with proposed weights → `distress_scores_shadow` | [cds_engine.py](src/services/cds_engine.py#L2815) `--shadow --fit-artifact` | ✅ built, **manual CLI** |
| **E** | Validate shadow vs live (3 numeric checks), exit 0/1/2 | [scoring_validation_report.py](src/tasks/scoring_validation_report.py) | ✅ built, **manual CLI** |
| **F** | Apply validated weights to **live** scoring | — | ❌ **missing (persistence + trigger)** |

### Critical architecture facts (drive the whole design)

1. **The apply logic already exists.** [`_apply_fit_artifact`](src/services/cds_engine.py#L2688) loads a Stage C artifact and overwrites `VERTICAL_WEIGHTS` in place. It works today — but only in-memory, and only when `--fit-artifact` is passed alongside `--shadow`.

2. **Live scoring is a batch process, not the API.** Weights are consumed by `python -m src.services.cds_engine --rescore-all`, a fresh cron process at 07:00. The FastAPI app serves *already-computed* `distress_scores` rows; it does not recompute scores. **Therefore no redeploy and no hot-reload is needed** — a fresh batch process re-reads config every run. (My earlier estimate assumed a DB-backed config reload; that is unnecessary.)

3. **Stage C artifact only proposes base signal weights** (`vertical_weights` per signal) and passes tier thresholds through unchanged ([`derive_lead_tier_thresholds`](src/services/scoring_fit.py#L225) is a no-op today). Stacking, recency, universals are untouched. Scope of automatic change is deliberately narrow — good.

4. **Stage E is a real numeric gate, not human judgment.** It returns PASS only if tier distribution, event-rate monotonicity, and cross-county parity all pass — and it needs ≥30 events per tier/county to even evaluate parity ([scoring_validation_report.py:294](src/tasks/scoring_validation_report.py#L294)). Thin data → WARN, never a false PASS. This *is* the data-sufficiency safety gate; we don't need to build a separate one.

5. **Nothing is scheduled.** `scripts/cron/crontab.txt` has zero entries for `scoring_training_data`, `scoring_fit`, shadow rescore, or `scoring_validation_report`. All four are manual.

---

## 3. What needs to be built

### F1 — Stage F: promote + persist the approved artifact  *(new, small)*

New module `src/tasks/scoring_cutover.py`:

- Read latest Stage E report (path passed in, or re-run `run_report()`).
- If `overall_status != "PASS"` → log, alert, exit non-zero, **do not promote.**
- Refuse promotion for any vertical whose proposal carries a `coverage_warning` (thin data) even if E passed overall — reuse the field already on [`KnobProposal`](src/services/scoring_fit.py#L164).
- On PASS → insert one row into a new `scoring_cutover_log` table:
  - `id`, `created_at`, `fit_artifact_path`, `validation_status`, `weights_snapshot` (JSONB — the full proposals block), `applied` (bool).
- "Active weights" = the most recent `scoring_cutover_log` row with `applied=true`. This table is both the **audit trail** and the **active pointer** — one table, one read.

Schema via new idempotent script `migrations/apply_scoring_cutover_log.py` (`CREATE TABLE IF NOT EXISTS`), plus model in `src/core/models.py`.

### F2 — Auto-apply the approved artifact on live rescore  *(new, tiny — reuses existing code)*

In `cds_engine.py`, on a **non-shadow** `--rescore-all` run: before scoring, look up the latest `applied=true` row in `scoring_cutover_log`; if present, call the existing `_apply_fit_artifact(row.fit_artifact_path)`. Add a `--no-active-weights` escape hatch to force the `config/scoring.py` baseline.

- Reuses `_apply_fit_artifact` verbatim — it already mutates the dict correctly.
- `config/scoring.py` stays the seed/fallback default; the approved artifact is the override, re-applied fresh each daily batch run.
- No redeploy, no file rewrite of the git-tracked config.

### F3 — Orchestration + scheduling  *(new, wiring)*

New thin runner `src/tasks/scoring_retune.py` that runs B→C→D→E→F in order with fail-fast (if B fails, C must not run on stale/absent CSV). Model it on the existing [`src/connectors/runner.py`](src/connectors/) bookkeeping pattern — no new scheduler framework.

Add one weekly entry to `scripts/cron/crontab.txt`, staggered **after** the daily 07:00 live rescore and skip-trace (per the hard-stagger rule in CLAUDE.md) so shadow and live never contend. Suggested: weekly, Sunday, post-pipeline.

### F4 — Alerting on WARN/FAIL/skip  *(new, small)*

When E returns WARN/FAIL or F skips a vertical for thin data, emit an ops alert (same channel the team already uses) instead of failing silently — CLAUDE.md forbids silent failure. If no ops-alert channel exists yet, log at `ERROR` and surface in the daily dashboard.

---

## 4. Blockers

### B1 — Outcome data volume (the real gate — nuanced)

Two distinct outcome signals feed the loop:

- **Primary (implemented, available now):** public-record transaction events — arms-length deed transfer or foreclosure filing after score date ([scoring_training_data.py:17-19](src/services/scoring_training_data.py#L17)). These come from the `deeds` / `foreclosures` tables populated by scrapers — **not** from subscribers. This signal exists today, independent of whether we have paying customers.
- **Secondary (test-only today):** subscriber-reported `DealOutcome`. Sparse — the training builder itself notes it's held for "future re-fits once that table accumulates real subscriber-reported volume." All 142 subscribers are currently internal/test accounts (`@heu.ai` / synthetic), so real subscriber conversion signal = 0.

**Implication:** the loop can be built and run against the primary public-record signal now. Whether it *acts* depends on Stage E passing, which needs ≥30 matured (90-day-window) events per tier/county. That may or may not be met yet — verify with a dry-run (§5 step 0). If not met, E returns WARN and F correctly refuses to promote until volume builds. The subscriber-conversion enrichment layers in automatically once real subscribers exist. **Nothing is blocked from being built; the gate is empirical and self-enforcing.**

### B2 — No ops-alert channel confirmed

F4 assumes an alert sink. If none exists, that's a small dependency to resolve (or fall back to dashboard/ERROR log). Not a blocker to building F1–F3.

**No hard code blockers.** The earlier-feared redeploy/config-reload blocker does not exist (see §2 fact 2).

---

## 5. Sequencing & effort

**Step 0 (before building):** dry-run B→C→D→E manually on current dev data. This tells us whether real event volume already clears Stage E's ≥30-event bar, and validates the artifact→shadow→report chain end to end on live data. ~half day. *Do this first — it de-risks everything.*

| Step | Work | Effort |
|---|---|---|
| 0 | Manual B→C→D→E dry-run on dev data; confirm artifact + shadow + report chain | 0.5 d |
| F1 | `scoring_cutover.py` + `scoring_cutover_log` table + model + migration | 1 d |
| F2 | Auto-apply latest approved artifact on live rescore (reuse `_apply_fit_artifact`) | 0.5 d |
| F3 | `scoring_retune.py` orchestrator + crontab entry | 0.5 d |
| F4 | Alerting on WARN/FAIL/skip | 0.25 d |
| — | Tests: cutover gate (PASS applies / WARN blocks / thin-data skips), auto-apply resolves latest row | 0.5 d |
| **Total** | | **~3–3.5 dev days** |

**Recommended order:** Step 0 → F1 → F2 → F3 → F4. F1/F2 are independent of F3/F4 and could parallelize.

---

## 6. Recommendation

Build it now. It's a **medium** task (~3 days), not large — five of six stages already exist and the apply logic is already written; this is persistence + one trigger + scheduling + a gate. The min-data gate (Stage E, already built) makes it safe to leave running unattended: it stays idle on thin data and begins re-tuning automatically as real transaction/subscriber outcomes accumulate. Once live subscribers arrive, the subscriber-conversion signal enriches the same loop with no further dev work.

**Not building:** copy and pricing auto-adjustment — those remain manual by design; confirm with client that's acceptable (it's the standard, correct boundary).
