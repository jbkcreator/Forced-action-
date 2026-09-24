# WP-8A / WP-8B / WP-9 — Real End-to-End Evidence

Run date: 2026-09-22, against the live server. Continuation of the same
session's WP-1/WP-2/WP-5B and WP-T2-1/WP-T2-2 reports. All tests call the
real production compute/repository functions directly against the live DB.
No property/deed/borrower/opportunity data was fabricated or written for
WP-8A/8B — computations ran against real, existing, unmodified records
(read-only for properties/deeds throughout). WP-9's dial list ran against
real live Hillsborough county data. Real borrower names/phone numbers
surfaced by WP-9 are **redacted below** (aggregate stats and property IDs
only) — this report is committed to the repo and those are real people's
contact information, not synthetic test data.

---

## WP-8A — Scenario Builder: Deal Math & Program Matching

### Deal math (`compute_quote_ready`) — PASS

Complete inputs (purchase $300,000 + rehab $50,000, ARV $450,000, max
LTC 85%, max LTV 75%):

```
project_cost = 350,000        (purchase + rehab -- correct)
LTC cap      = 350,000 × 0.85 = 297,500
LTV cap      = 450,000 × 0.75 = 337,500
proposed_loan = min(LTC cap, LTV cap) = 297,500   ✓ matches actual output exactly
```

Every figure is a `Figure{raw, display, source, confidence}` object, not a
bare number — `ltc.confidence='low'` in this run because the ARV input
itself carried `arv_confidence='low'` (correctly propagated, not silently
upgraded). Ran twice with identical input → byte-identical output, confirmed
pure/reproducible.

**Missing-input handling** — an input with no `purchase_price`/`rehab_estimate`/`arv`
correctly returned `project_cost=None`, `proposed_loan=None`, and an explicit
`missing=['arv', 'ltc', 'ltv', 'project_cost', 'proposed_loan', 'purchase_price', 'rehab_estimate']`
list — no fabricated zero or guessed figure.

### Program matching (`lender_box.evaluate`) — PASS, against real production program rules

Real `lender_box_programs` table on this box (4 active programs: Fix & Flip,
New Construction, DSCR Rental, Home Equity), real LTC/LTV/loan-range/property-type
rules:

- A deal shaped to qualify → `status=in_box`, `matched_program_name='DSCR Rental'`
  (correctly the first program whose full rule set the deal satisfies).
- A $5M loan deal → `status=out_of_box` with **five explicit fail reasons**
  (one per program/check it failed, e.g. *"[dscr_rental] loan $5,000,000
  exceeds maximum $2,000,000"*) — nothing silently discarded.
- A deal missing purchase/rehab/ARV → `status=uncertain`,
  `uncertain_flags=['arv', 'purchase_price_or_rehab_estimate']`.

**Verdict: PASS.** Every outcome self-explains why, matching WP-8A's Done-When
("clear program-match result with visible assumptions") exactly.

### ⚠️ Data-quality note (not a code bug)

`lender_box_programs.notes` for 3 of the 4 active programs literally says
things like *"LTC/LTV synthetic pending confirmed rules"*. The
program-matching **mechanism** is real, correct, and tested above — but the
**business values** (the actual LTC/LTV/loan-range thresholds every real
deal on this box is being matched against right now) are explicitly still
placeholders awaiting client confirmation, per the programs' own `notes`
column. Worth flagging before this is treated as go-live-ready.

---

## WP-8B — Scenario Builder: Comparable Sales & ARV Engine

### Real ARV computation — PASS

`compute_arv_for_property()` called directly against real, unmodified
properties (read-only — no writes to `properties`/`deeds`):

- **Property with incomplete source data** (`property_use_code=NULL` in the
  real `properties` row): correctly returned `arv_unknown=True`,
  `unknown_reason='subject_unavailable'` — no fabricated ARV for a subject
  the engine can't actually characterize. (This is the engine working
  correctly, not a bug — confirmed by inspecting the real row's data.)
- **Property with complete data** (real Hillsborough single-family, 9 real
  comparable sales found): produced a full range —
  `low=$361,437 / point=$380,475 / high=$447,374`, `confidence='medium'`,
  `locality_tier='county'`, `weak_comp=True`. Every one of the 9 selected
  comps came back with its own `sale_price`, `price_per_sqft`,
  `sqft_adjustment`, `condition_adjustment`, and `adjusted_value` —
  **full per-comp assumption transparency**, not a black-box number.
- `weak_comp=True` fired *despite having 9 comps* — because they only
  matched at the `county` locality tier (the loosest), not
  `subdivision`/`neighborhood`/`zip`. This is the "avoid false precision"
  requirement working exactly as intended: comp count alone doesn't buy
  confidence, comp *quality* does.
- A nonexistent `property_id` correctly returned `arv_unknown=True` /
  `unknown_reason='subject_unavailable'`, never a guessed range.

**Verdict: PASS.** Matches WP-8B's Done-When: *"exposes comps, assumptions,
and ARV range clearly enough for a human to judge whether the estimate is
based on strong or weak evidence."*

### Persistence / idempotency — PASS

`persist_arv_result()` on an identical recompute returned the **same**
`arv_result_id` (true no-op, not a duplicate row) — confirmed against a real
`fa_max_arv_results` row that already existed for the test property before
this run (a legitimate prior computation, left untouched). `get_published_arv()`
correctly returned the projected `low`/`high`/`point` and correctly excludes
internal-only fields (`selected_comps`, `locality_tier`) from what it exposes.

### What's NOT verified

- The **manual-override-with-audit-trail** path (explicitly in scope) was
  located in code (`arv_persistence.py`) but not exercised end-to-end this
  session.
- The **"Quote Ready Slack dossier"** posting entry point was not located
  precisely during this session's research pass — worth a follow-up grep
  for `dossier` before assuming it's wired up; not confirmed either way.

---

## WP-9 — Dial List Engine

### Full real ranking pipeline — PASS

`generate_dial_list()` run against real, live Hillsborough county data for
2026-09-22: **30 ranked entries**, each with real detected triggers
(`cash_purchase`, `financing_intent`, `permits_no_financing`,
`auction_probate`), a resolved canonical borrower, a real property address,
a plain-English reason string (e.g. *"Cash purchase, no financing — may
want leverage next deal. Permits pulled, no recorded financing — funding
work out of pocket."*), and — where relationship history existed — talking
points (e.g. *"Owns 1 properties"*, *"Last sale 6 months ago"*).

**Expected-revenue math verified correct** (top entry): `probability=0.30`,
`expected_loan=$999,937.40`, `commission=$14,999.06` (exactly 1.5% of loan —
consistent), `urgency=1.667` → `expected_revenue=$7,499.53`, and the list is
correctly sorted descending by this score (entry 2 = $3,345.61, strictly
lower, etc.).

**Determinism confirmed**: ran twice with identical `as_of`/`county_id` —
**identical entry order** across both runs (first 10 property IDs matched
exactly). Not `date.today()`-drifted, not randomized.

**Stale-source flagging real**: `stale_sources=['deeds', 'foreclosures']` —
the engine honestly flags when its own underlying data sources are behind
SLA, rather than silently ranking against stale data as if it were fresh.

### Real Slack delivery — PASS

`generate_and_deliver()` posted a real digest — *"📞 Dial List — Tuesday,
September 22 — 30 calls"* — redirected via the function's own `channel=`
override parameter to the user's sandbox test workspace (never the real
production `DIAL_LIST_SLACK_CHANNEL`, which is the same real channel as
`FA_MAX_SLACK_CHANNEL_MONEY` used earlier in this session). Delivery
independently confirmed via a fresh `conversations.history` fetch, not just
the function's own return value.

Side effect noted (legitimate, not cleaned up): this call also wrote two
real `dial_list_snapshot` rows for hillsborough/2026-09-22 — these contain
genuine, correctly-computed production data (not synthetic), functioning
exactly as the real cached-fallback mechanism the code documents. Left in
place; deleting a valid snapshot would reduce, not restore, production
resilience.

### Disposition capture — PASS

`record_dial_disposition()`:
- Rejects `outcome='won'` with a `loss_code` set.
- Rejects `outcome='lost'` with `loss_code=None`.
- Rejects an unrecognized `loss_code` (validated against the exact 8-code
  `LOSS_REASON_CODES` enum before any DB write).
- First record on a synthetic thread → `inserted=True`. Identical rerun on
  the same (now-terminal) thread → `inserted=False` — confirmed idempotent,
  no duplicate disposition rows.

**Verdict: PASS** — matches WP-9's Done-When ("idempotent reruns", "capture
disposition back to durable state") and the broader requirement that the
list needs no manual reordering: real triggers, real expected-revenue
ranking, and a concise, correct reason are all present for every entry.

### What's NOT verified

- The **daily 7:00 AM cron trigger itself** was not fired — confirmed the
  `crontab.txt` entry exists (`45 10 * * *` UTC → runs
  `src.tasks.dial_list_daily`) and that the underlying function it calls
  works correctly when invoked directly; did not wait for or simulate the
  actual cron firing.
- **Interactive Slack card actions** on dial-list entries (`actions.py`,
  `action_listener.py`) were located but not click-tested this session (same
  category of manual click-through already proven for WP-2's Relay cards
  earlier — same mechanism, not re-verified here for time).
- **Construction/builder opportunity weighting** specifically was not
  isolated and verified numerically — the ranking as a whole was verified
  correct and deterministic, but this one specific weighting rule wasn't
  singled out.

---

## Summary

All three work packages' core Done-When criteria hold under real execution
against real production data:

| WP | Core mechanism verified | Real data used |
|---|---|---|
| WP-8A | Deal math (project cost, LTC/LTV, loan cap) + program matching | Real `lender_box_programs` (4 active programs) |
| WP-8B | ARV comp selection, adjustment, range, confidence tiering | Real Hillsborough property + 9 real comparable sales |
| WP-9 | Trigger detection, borrower resolution, expected-revenue ranking, delivery, disposition | Real Hillsborough dial-list generation (30 real entries) |

No bugs found in this batch (unlike the WP-1/WP-T2-2 batch, which found two).
The one substantive caveat is WP-8A's underlying program *values* being
self-documented as placeholders, not a defect in the matching engine itself.
