# 0001 — County profitability gate definition

## Status
Accepted 2026-05-18.

## Context
`EXPANSION_GATES["county_profitability"]` requires `"net_positive_monthly"`, but
"net" was never defined and the metric was never computed. Without a concrete
definition, gate #7 can never be automated.

## Decision

**v1 (current):** profitable if any active paying subscribers exist in the county:

    profitable = count(Subscriber WHERE county_id=src AND status='active'
                       AND tier NOT IN ('free','data_only')) > 0

Cached as 1.0 (profitable) or 0.0 (not) under `fa:ks_metric:{county_id}:county_profitability`.

**v2 (target):** proper P&L formula once a `stripe_invoices` or `payments` table exists:

    revenue_30d_cents     = SUM(stripe_charges WHERE county_id=src AND age<=30d)
    cost_30d_cents        = sms_cost + voice_cost + skip_trace_cost
                          + paid_acquisition_spend (tagged to county)
                          + claude_api_cost (pro-rated by county subscriber fraction)
    profitable            = revenue_30d_cents - cost_30d_cents > 0

## Known gaps (v1)

- Revenue is proxied by subscriber presence, not actual billed amounts.
- Hosting/infra not attributed (treated as system-wide overhead).
- Refunds and chargebacks not netted.
- Claude API cost from `agent_decisions.cost_usd` not yet pro-rated per county.
- A new county with 0 subscribers will always be red — correct behavior
  (no data = not profitable).

## Consequences

- Gate flips green once the source county has at least one active paying subscriber.
  In practice, Hillsborough already has subscribers, so this gate will be green immediately.
- When v2 is implemented, a county may flip red if revenue doesn't cover costs.
  Revisit thresholds before launching county #3 with multiple live counties.

## Alternatives considered

- Manual override flag in admin UI — rejected (defeats autonomous decision-making).
- Drop the gate — rejected (spec requires all 7).
- Hardcoded price tiers × subscriber count — fragile; prices change without updating code.
