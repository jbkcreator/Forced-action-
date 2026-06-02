---
status: accepted
---

# Outbound voice-drop SLA is measured from event dispatch; detection is a high-frequency sweep, not per-subscriber timers

## Context

The DoD requires the outbound Synthflow voice drop for a **High-Intent
Non-Converter** (RSS > 70, no conversion in 48h) to fire **within 60 seconds of
the trigger condition**. The trigger had two problems:

1. Detection was a **daily cron** (`synthflow_voice_drop_sweep`, `0 15 * * 1-5`),
   so an eligible subscriber could wait up to ~24 hours.
2. One half of the condition ("48h with no conversion") only becomes true by the
   *passage of time* — nothing emits an event at the moment hour-48 elapses, so a
   literal "60 seconds from when the condition became true" is unachievable
   without a per-subscriber scheduler.

## Decision

- **The 60-second SLA is measured from `high_intent_no_convert` event dispatch to
  `synthflow_client.initiate_call`** — not from the abstract moment the condition
  became true. The graph → Synthflow leg already runs in seconds.
- **Detection is a high-frequency sweep (~every 2 minutes)** running the same
  eligibility SQL, replacing the daily cron. This bounds condition-true →
  dispatch to roughly the sweep interval without introducing per-subscriber timer
  infrastructure.
- **Conversion is broadened** to any revenue/spend action in the trailing 48h
  (territory lock OR bundle purchase / paid lead unlock / wallet debit /
  credit-report purchase / subscription upgrade) — not territory lock alone.

## Considered Options

- **Per-subscriber scheduled timers / delayed tasks** at each subscriber's 48h
  mark — rejected: requires new scheduler + cancellation infrastructure and must
  reconcile two independent clocks (RSS-crossing vs 48h-since-last-conversion) for
  marginal latency gain over a 2-minute sweep.
- **Keep the daily cron** — rejected: violates the SLA by up to 24h.

## Consequences

- **The supervisor idempotency had to be fixed first.** `dispatch_event`'s
  `_already_handled` compares `AgentDecision.decision_id == idempotency_key`, but
  the sweep passed a random `decision_id` plus a separate date-bucketed
  `idempotency_key`, so the dedup never matched. Harmless once a day; a
  re-dispatch *storm* at 2-minute cadence. The fix (dispatch-time marker / aligning
  the persisted key) is a prerequisite for raising the sweep frequency, with the
  7-day `manual_action_log` row as the terminal guard.
- A subscriber who converts inside the 48h window exits this population and is
  routed to a separate upsell path, never the recovery voice drop.
