# Churn Risk is the single source of risk truth; proactive_save consumes it

**Status:** accepted

## Decision

Subscriber churn risk is computed in **one** place — a nightly
`churn_scoring` job that writes a forward-looking **Churn Risk** score and a
`predicted_inactivity_at` timestamp to `user_segments`. The job is a
**deterministic weighted heuristic**, not a trained model, and it **sends
nothing**.

`proactive_save` is refactored to **read** `predicted_inactivity_at` (fire the
Data-Only offer when it is within the save horizon) instead of computing its
own hard-coded 5-day inactivity rule. The retention event producer reads the
same Churn Risk to suppress its soft summary when risk is high.

## Why a heuristic, not a model; why layered, not a third score

- **Heuristic, not a trained model:** Hillsborough is the only launched
  county and expansion is gated — there are not enough labeled Inactivity
  Onset events to train a classifier that generalizes. (The trained
  `scoring_fit` infra is for *property* distress, a different, high-volume
  dataset — it is not reusable here.) The job logs every prediction +
  backfilled outcome into `churn_predictions`, accumulating the labeled
  dataset that would justify a model in a later milestone.
- **Layered onto `user_segments`, not a third score:** the repo already
  carries two subscriber-risk artifacts (`revenue_signal_score`, the segment
  bucket). A third standalone score would fragment "why is this subscriber
  risky?" across three sources of truth. Churn Risk reuses the Revenue Signal
  Score's behavioral signals — but as **trends/slope** (am I dropping off),
  not **levels** (how valuable am I now), which is the actual distinction
  between the two scores.

## Why this is surprising (read before "fixing" it)

A future engineer will see `proactive_save` reading a `predicted_inactivity_at`
column instead of a self-contained "5 days since last wallet txn" check and
may try to re-inline the rule for simplicity. That re-fragments the risk
definition: the personalized baseline (risk measured against each
subscriber's own cadence) lives in the churn job, and a flat 5-day rule
misfires for bursty buyers (e.g. Weekend Bundle subscribers who are silent
Mon–Thu by design). Keep `proactive_save` as a consumer.

## Considered alternatives

- **Brand-new standalone churn model/score** independent of RSS — rejected
  (fragments risk truth; no data for a model).
- **Just lower proactive_save's day thresholds** — rejected (reactive, not
  predictive; no personalized baseline).
- **Churn job sends the offer itself** — rejected (duplicates send logic,
  splits compliance/suppression away from the existing sender path).

## Consequences

- **Cron ordering is now load-bearing:** `churn_scoring` (13:00 UTC) must
  finish before `proactive_save` (15:00 UTC) and `retention_event_producer`
  (16:00 UTC). There is no inter-job signaling (per CLAUDE.md convention) —
  the dependency is documented in CLAUDE.md's cron-ordering note. Overrun =
  consumers read stale predictions that day.
- Compliance stays in the sender path: `proactive_save` keeps routing sends
  through existing opt-in / vendor-pause / `sms_compliance` guards. The churn
  job has no send surface.
- `predicted_inactivity_at` is pinned to one definition of "inactive" (5-day
  no-wallet-debit) on purpose; see CONTEXT.md **Inactivity Onset**.
