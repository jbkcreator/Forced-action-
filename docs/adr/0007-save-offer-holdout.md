# Save Offer Holdout — 10% of at-risk subscribers are withheld on purpose

**Status:** accepted

## Decision

The predictive churn job flags every subscriber it predicts will reach
**Inactivity Onset** (crossing the 5-day no-wallet-debit window). A stable
~10% **Save Offer Holdout** — selected by a deterministic hash of
`subscriber_id`, not re-rolled each night — is flagged but **never sent the
Data-Only save offer**. `proactive_save` skips them.

Their realized outcomes are the only intervention-free labels we have, and
they are what makes the "predictive" claim falsifiable: precision/recall of
the heuristic, and the *lift* of the save offer (Inactivity Onset rate of
flagged-and-saved vs. flagged-and-held-out).

## Why this is surprising (read before "fixing" it)

A future engineer will see `proactive_save` deliberately declining to send a
retention offer to ~10% of paying subscribers who were flagged as at-risk,
assume it's a bug, and "fix" it. **Do not.** Removing the holdout silently
destroys the only clean measurement of whether the save flow works at all —
without it, every non-churning flagged subscriber is ambiguous (was the
prediction wrong, or did the offer save them?). The cost of the holdout is
~10% of potential saves forgone; the benefit is a measurable system instead
of an unfalsifiable one.

## Considered alternatives

- **No holdout, accept confounded labels** — simpler, but the save flow's
  effectiveness becomes permanently unprovable.
- **No holdout, no backfill (trust the heuristic)** — the "predictive" claim
  is then unfalsifiable; rejected.

## Consequences

- The holdout membership must be **stable** across nights (hash-based), or a
  subscriber drifting in and out of the holdout corrupts both arms.
- The holdout is the substrate for the future trained model (ADR 0008): its
  rows carry uncontaminated outcome labels.
- Business sign-off recorded: withholding the offer from ~10% of at-risk
  subscribers is an accepted cost for clean measurement.
- Pattern mirrors the Cora 10% shadow holdout (ADR 0005).
