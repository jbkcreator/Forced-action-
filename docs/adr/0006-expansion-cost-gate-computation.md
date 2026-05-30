# 6. How the two cost-based expansion gates are computed

Date: 2026-05-30
Status: Proposed

## Context

Two of the seven **Expansion Gates** (`config/cora_guardrails.py:EXPANSION_GATES`)
are cost-based:

- `free_tier_cost_ratio` — free-tier cost ≤ 40% of revenue
- `county_profitability` — county net positive

Neither produces a real value today:

- `kill_switch_metric_ingest.py` hard-codes `free_tier_cost_ratio = None`
  ("no per-subscriber compute-cost allocation"). `_gate_color(None)` → `"red"`.
- `county_profitability` is a v1 proxy: `1.0 if any active paying subscriber
  exists, else 0.0`. That is presence-of-revenue, **not** net-positive.

Because `_all_green()` requires every gate green and `free_tier_cost_ratio` is
permanently `None`→red, **no expansion can ever fire** — neither a county
launch nor an Expansion ICP Channel. The central go/no-go machine is inert,
and gate #7 reports a value that does not mean what its name says.

We need real, queryable values. The data needed mostly exists:

- **Cost Ledger** (`api_usage_logs`, see [[0004-api-usage-logs-single-cost-ledger]])
  — one row per Claude/Telnyx/Stripe call, authoritative `cost_usd`, with a
  nullable `subscriber_id` FK (indexed). Per-subscriber attribution is possible
  where `subscriber_id` is set; county attribution via JOIN
  `subscribers.county_id`.
- **Revenue** — Stripe-backed subscription MRR on `subscribers`.

The gaps: rows with `subscriber_id IS NULL` (shared/system cost — scraping,
batch jobs) cannot be attributed to a subscriber, and fixed county operating
cost (infra, scraper proxies, headcount) is not in the ledger at all.

## Decision

Compute both gates from the **Cost Ledger + Stripe revenue**, using the cost
lines that exist there (Claude exact; Telnyx where logged), and **document the
approximations** rather than block on a full cost-allocation system.

### `county_profitability` ("county net positive")

```
revenue_30d        = Σ MRR of active paying subscribers in county (trailing 30d)
attributable_cost  = Σ api_usage_logs.cost_usd
                       JOIN subscribers ON subscriber_id
                       WHERE subscribers.county_id = :county
                         AND created_at >= now() - 30d
county_profitability = 1.0 if (revenue_30d - attributable_cost) > 0 else 0.0
```

Still binary (green/red), preserving the existing `_gate_color` special-case,
but now backed by a real revenue-minus-variable-cost comparison instead of
"someone paid."

### `free_tier_cost_ratio` ("free-tier cost ≤ 40% of revenue")

```
free_tier_cost = Σ api_usage_logs.cost_usd
                   JOIN subscribers ON subscriber_id
                   WHERE subscribers.county_id = :county
                     AND subscribers.tier IN ('free','data_only')
                     AND created_at >= now() - 30d
free_tier_cost_ratio = round(free_tier_cost / revenue_30d * 100, 1)   # red if revenue_30d == 0
```

Graded `lower_is_better` against the existing KILL_SWITCH thresholds
(green ≤ 40, red ≥ 50).

### In-scope vs out-of-scope cost

**In scope** (variable, ledger-tracked, subscriber-attributable): Claude API
(exact), Telnyx SMS (where ledger rows exist; otherwise approximated at the
flat $0.004/segment rate already referenced in `cora_guardrails.py`).

**Out of scope** (documented, not in v1): `subscriber_id IS NULL` shared cost,
scraping/proxy cost, fixed infra and headcount. Consequence: both gates are
**optimistic** — they understate true cost. This is acceptable for v1 because
the dominant *variable* cost is Claude + SMS, and the gates are tripwires, not
accounting. v2 closes the gap with a real cost-allocation table.

## Consequences

**Positive**

- All 7 gates can now be green simultaneously — the expansion machine can
  actually fire, and gate #7 means what its name says.
- Both gates read the single authoritative Cost Ledger, consistent with
  [[0004-api-usage-logs-single-cost-ledger]] — no summing of the derived
  `agent_decisions.cost_usd` / `chat_messages.tokens_*` copies.
- Per-subscriber `subscriber_id` attribution is reusable for future per-cohort
  cost reporting.

**Negative**

- Both gates are optimistic by a known, undocumented-in-the-number margin
  (fixed + unattributed cost excluded). A county could pass `county_profitability`
  while unprofitable once fixed cost is loaded in.
- Telnyx coverage in the ledger is unverified per-send; the flat-rate fallback
  is an approximation of an approximation.
- `revenue_30d == 0` forces both gates red — correct (a county with no revenue
  is neither profitable nor has a meaningful free-tier ratio), but means a
  brand-new county reads red until its first paying subscribers, which is fine
  because gates are evaluated against the **Source County**, not the candidate.

**Open questions deferred**

- Whether Telnyx sends are reliably logged to `api_usage_logs` or must always
  use the flat-rate fallback. Audit ledger coverage before trusting the SMS line.
- The MRR source of truth for `revenue_30d` — live Stripe vs a denormalized
  subscriber column. Resolve alongside the `Contractor MRR` ≥ $50K meta-gate,
  which needs the same revenue primitive.
- v2 cost-allocation table that captures shared/fixed cost and makes the gates
  pessimistic-correct instead of optimistic.
