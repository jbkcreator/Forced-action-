# 32. Insurance-distress lead segment: FEMA-safe definition

Date: 2026-07-15

## Status

Accepted

## Context

Spec 3.4 (Mini-Build 2, Task #7) sells a premium lead pack of storm/flood-damaged
properties to investors flipping storm-damaged homes. The naive definition —
"any property with an insurance/damage tag" — is dangerous: on 2026-05-06,
`insurance_claim` was reverted from CDS weight 72 to stacking-only weight 10
(`config/scoring.py:130,228`) precisely because as a primary signal it
auto-qualified ~38k bulk/historical FEMA registrants. Reproducing that filter at
the *sales* layer would sell 38k junk leads.

The damage signals (`insurance_claim`, `storm_damage`, `flood_damage`) are all
stacking-only — they boost an existing distress score but cannot qualify a lead
alone. The buyer is a flipper who wants damage **plus** flip upside (equity /
investment distress), not any damaged home (that would be a public-adjuster
product, which this is not).

## Decision

Define the insurance-distress segment as properties where **both** hold:

1. Latest `DistressScore` has a `wholesalers` **or** `fix_flip` vertical score
   **≥ 40** (Silver floor) — a genuine flip/investment signal, not a bare tag.
2. At least one **`storm_damage` or `flood_damage`** incident within **180 days**
   (`STACKING_WINDOW_DAYS`).

`insurance_claim` is **excluded as a qualifier** — it is the FEMA bulk source
that caused the 2026-05-06 incident. It still stacks into scores normally; it
just does not gate this segment. The 180-day recency guard keeps stale storm
tags (e.g. a 2018 event) from qualifying today.

The segment is sold through the existing ZIP-scoped lead-pack pipeline via a new
`segment="insurance_distress"` parameter (reusing county gate, min-5, ADR-0015
contactability, ADR-0018 deferred fulfillment, `SentLead`), at a premium Stripe
price set by Josh in the dashboard.

## Consequences

- **Positive:** cannot reproduce the 38k-FEMA mass-qualify; every segment lead is
  a real flip candidate with fresh storm/flood damage. Reuses the whole proven
  pack pipeline — one param + one filter clause.
- **Negative:** narrower inventory — many ZIPs will have <5 qualifying leads and
  won't surface a pack (min-5 gate). Accepted: quality over volume.
- **Deferred:** county-wide (storm-path) packs, and partner referral-fee routing
  (legal check pending) — both out of scope; this ADR covers lead-data sale only.
