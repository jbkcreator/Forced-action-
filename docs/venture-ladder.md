# Venture ladder runbook (CLONE-v2.2 / CL4)

How a venture goes from "a market someone noticed" to "a business in the
portfolio", and what has to be true at each step.

`docs/venture-onboarding.md` covers *how* to provision a venture (CL3). This
covers *whether* to, and how the fleet decides that on its own.
Design rationale: [ADR 0033](adr/0033-venture-ladder-gate-evidence-model.md).

```
radar → probe → pilot → unit_economics → cell → spin_up → portfolio
```

Venture #1 (`hillsborough_distress`) is seeded at `portfolio` by the CL4
migration — it is already live, and starting it at `radar` would be a lie the
evaluator acts on.

## The rungs

| Rung | Means | Gates to leave it |
|---|---|---|
| `radar` | A scored candidate. Real `ventures` row, `is_active = false`. | `market_score` ≥ 60 · `geography_resolvable` · `county_overlap` = 0 |
| `probe` | Sources are reachable and county-specific. | `source_coverage_pct` = 100 · `scrape_sample_count` ≥ 1 · **presell gate** |
| `pilot` | Really sending, one cell, small ceiling. | `relay_ready` · `dispatched_count` ≥ 25 · `compliance_block_pct` ≤ 10 |
| `unit_economics` | The numbers hold up. | `platform_cost_per_acquisition_usd` ≤ 400 · `contribution_margin_usd` > 0 · `cost_per_reply_usd` ≤ 25 |
| `cell` | Expanded to the grid; auto-double lives here. | `cells_above_floor` ≥ 2 · `clean_auto_double_count` ≥ 1 · `send_failure_pct` ≤ 2 · **presell gate** |
| `spin_up` | Full Clone-Pack applied and verified. | `clone_pack_complete` · `harness_pass` · `clean_sweep_count` ≥ 3 |
| `portfolio` | Steady state. Terminal — no gates. | — |

Thresholds are in `config/venture_ladder.py`. Never inline them anywhere else.

### What each rung takes from and gives back to the Clone-Pack

`CLONE_PACK_IO` in `config/venture_ladder.py` is the machine-checked version of
this table — `validate_ladder_config()` fails if a stage is missing from it, so
a new rung cannot be added without declaring how it relates to the pack.

| Rung | Consumes from the pack | Produces into it |
|---|---|---|
| `radar` | `venture.state`, `bankruptcy_court_code`, `counties` | inactive `ventures` row, market-score evidence |
| `probe` | `source_coverage`, `template_county_id` | `county_sources` with county-specific URLs, scrape sample |
| `pilot` | `relay_ready`, `kill_switch_feature` | dispatched `relay_approval_queue` rows |
| `unit_economics` | `counties`, attributed revenue and cost | proven contribution margin |
| `cell` | `relay_daily_ceiling`, per-cell reply rates | raised ceiling, cell production multipliers |
| `spin_up` | **the complete `ClonePack`**, `cron_line_present`, harness result | a venture that runs unattended |
| `portfolio` | the complete `ClonePack` | fleet reporting |

Two of these are live reads, not documentation:

- **`probe`** gets its coverage from `clone_pack.source_coverage()` — the same
  function `assemble()` uses. The gate that permits an advance and the pack that
  reports whether the venture can run cannot disagree about what "covered"
  means, because there is only one query.
- **`spin_up`** computes `clone_pack_complete` by calling `assemble()` at
  evaluation time. There is deliberately **no** `cron_registered` evidence type:
  whether the cron line exists is checked against `scripts/cron/crontab.txt`
  live, because a recorded claim goes stale the moment someone edits the
  crontab. `harness_pass` is the one thing at this rung that is legitimately
  self-reported.

**A radar candidate is an inactive `ventures` row.** That is not bookkeeping: the
CL3 resolver falls back to env settings for an inactive venture, so an unproven
candidate structurally cannot govern real sends. One identity and one join key
all the way up.

**`advance()` activates the venture on the `spin_up → portfolio` transition** —
the explicitly-approved go-live rung. `is_active OR :activate` in the same
`UPDATE` as the stage change, so a candidate that reaches portfolio without
ever going through `venture_provisioning --apply` (which sets `is_active` on
its own) still ends up resolving to its own Relay identity rather than
silently falling back to venture #1's.

## Gate colours

`green` / `yellow` / `red`, and **only all-green advances**. Yellow is "nearly
there" for the digest, not a pass.

A gate with no value resolves to its declared `no_metric_behavior`, not
automatically to red. This is the whole lesson of
[ADR 0006](adr/0006-expansion-cost-gate-computation.md): the county-launch gate
machine treats `None` as red, requires all-green, and has therefore never once
permitted a launch. A result whose colour came from `no_metric_behavior` is
flagged `imputed` so the digest can distinguish "passing" from "not measured".

## Daily operation

The evaluator runs itself at 09:30 UTC:

```bash
30 9 * * * $PROJECT/scripts/cron/run.sh src.tasks.venture_ladder_evaluator
```

It walks every venture (including inactive radar candidates), advances the ones
that are all-green, runs the auto-double rule, and posts a digest.

```bash
# See where everything stands, change nothing
python -m src.tasks.venture_ladder_evaluator --dry-run

# One venture
python -m src.tasks.venture_ladder_evaluator --venture venture_two --dry-run

# Release the one transition the cron will not make unattended
python -m src.tasks.venture_ladder_evaluator --venture venture_two --advance-spin-up
```

**`cell → spin_up` is never advanced unattended.** Spin-up buys a domain, a
mailbox warmup, an Instantly seat and proxy capacity. The presell gate
*authorises* that spend; a person *releases* it. Same division
`county_launch_evaluator` already draws.

## The presell gate

Blocks `probe → pilot` and again at `cell → spin_up`. Needs **5 distinct Stripe
customers AND ≥ $2,500 total**, none older than 90 days.

A commitment is a **refundable Stripe deposit**. Real money, so real evidence;
refundable, so people actually agree to one; a Stripe object, so a webhook
verifies it with no human in the loop; and it carries an amount, so the gate can
require money and not just headcount.

Four rules, each closing a way the gate could be passed without real demand:

| Rule | Without it |
|---|---|
| `verified = true` only, set by machine check | a hand-entered claim passes |
| **distinct customers**, deduped on `stripe_customer_id` | one enthusiast's five deposits pass |
| count **and** sum both required | five $1 deposits pass, or a single whale does |
| existing subscribers of **another** venture excluded | the existing book buying again reads as new demand |

On the dedupe: `UNIQUE (venture_key, evidence_type, source_ref)` stops a webhook
*retry* re-inserting the same PaymentIntent, but says nothing about one buyer
depositing five times — those are five legitimately distinct PaymentIntents. The
headcount is therefore computed over unique customers, and only the first
commitment per customer contributes to the total, so repeat deposits cannot
clear the money threshold either. A commitment with no `stripe_customer_id`
falls back to `contact_ref`, then to its own `source_ref`, rather than
collapsing every such row into one bucket — the identity is built in explicit
branches (Stripe customer → non-empty contact ref → source ref), not a bare
`customer or f"contact:{...}"` chain, since the `"contact:"` prefix alone made
that chain always truthy and unreachable past the first fallback.

The exclusion is scoped to *other* ventures: someone who only ever subscribed to
**this** venture is still valid demand for it.

```python
from src.services import venture_ladder

venture_ladder.record_evidence(
    db, "venture_two",
    evidence_type="presell_commitment",
    stage="probe",
    payload={"kind": "deposit", "amount_cents": 50_000, "contact_ref": "..."},
    source_ref=payment_intent_id,   # idempotency: a retry cannot inflate the count
    verified=True,                  # only ever set by the webhook
    recorded_by="stripe_webhooks._on_deposit_succeeded",
)
```

Widening what counts (`first_month`, `saved_card`) is a one-line change to
`PRESELL_ACCEPTED_KINDS`, not a migration.

> **Not yet wired:** the Stripe webhook handler that writes these rows is a
> follow-up. Until it exists, deposits are recorded by whatever collects them,
> and only `verified=True` rows count — so an unwired webhook means a blocked
> gate, which is the safe direction.

## Auto-double

Reply rate above **8%** doubles sending volume. 8% is the same figure
`lifecycle_guardrails.KILL_SWITCH["sms_reply_rate"]` already calls green — that
guardrail only ever throttles down on failure, and this is the missing upward
response.

**Two levels, two different knobs.**

| | What doubles | Enforced by |
|---|---|---|
| Venture | `ventures.relay_daily_ceiling` — the real cap | `relay/guards.py:reserve_daily_slot()` via `relay_daily_sent:{venture}:{channel}:{date}` |
| Cell | That cell's target *production* count (`limit` in `target_producer.produce_targets`) | Nothing new — the venture ceiling still caps total sends |

There is exactly **one** send cap in this system. The cell rule shifts the mix
underneath it; it is not a second ceiling.

**Eligibility gates both levels first.** Neither rule looks at reply rate at
all until the venture is `is_active` AND at `cell` or later
(`AUTO_DOUBLE_ELIGIBLE_STAGES` in `config/venture_ladder.py`) — the evaluator
calls `maybe_auto_double()`/`maybe_auto_double_cell()` for every venture on
every pass regardless of what rung it is clear to *advance* to, so without
this a pilot- or unit_economics-stage venture with a qualifying reply rate
could have its ceiling (or a cell's production count) scaled up before it
cleared the gates that say scaling is safe. Checked in the service functions
themselves (never just the evaluator), so nothing that calls them directly —
the acceptance harness included — can skip it either.

The cell-level rule is wired into the evaluator too: it calls
`maybe_auto_double_cell()` for every cell `cell_reply_rates()` returns traffic
for, and `target_producer.produce_targets()` /
`produce_auction_fast_follow_targets()` multiply their `limit` by
`cell_production_multipliers()` before ranking — the multiplier is scoped to
the call's own `county_id` (one venture's sweep), or `DEFAULT_VENTURE_KEY` when
`county_id` is unset (the pre-CL3, fleet-wide sweep).

Every guard is load-bearing:

- **min sample 200 sends** — 8% of a dozen sends is one reply.
- **7-day cooldown**, read from the audit table not a Redis flag. Doubling
  cold-email volume on a warming domain is how a sender gets blacklisted, and
  lowering the number back does not undo it.
- **`send_failure_pct` ≤ 2%** — a high reply rate next to a high failure rate
  means the list is dirty, not that the copy is good.
- **max ceiling 2000**, multiplier clamped rather than overshooting.
- **config-cache flush** — `ventures` is read through a 5-minute cache; without
  the flush a raised ceiling silently does not apply and the venture under-sends.

```python
venture_ladder.maybe_auto_double(db, "venture_two")
venture_ladder.maybe_auto_double_cell(db, "venture_two", "founder_tier_blitz")
venture_ladder.cell_production_multipliers(db, "venture_two")   # {"founder_tier_blitz": 2}
```

Multipliers are derived from the audit log (`2 ** doublings`, capped), not
stored, so the cooldown, the idempotency guard and the multiplier all read the
same rows.

## Acceptance harness

The PASS/FAIL. Run it before believing a venture is ready.

```bash
python scripts/harness/venture_spinup_acceptance.py
python scripts/harness/venture_spinup_acceptance.py --verbose   # every gate value
```

It builds a stub venture from `VENTURE_TEMPLATE`, provisions it, seeds the
evidence and traffic each rung needs, walks `radar → portfolio` through the real
`advance()`, proves the presell gate blocks when its evidence is removed, fires
auto-double and proves it declines on the retry, then assembles a Clone-Pack and
asserts it is complete.

**Everything runs in one transaction that is always rolled back**, pass or fail.
No Slack, no Instantly, no Stripe.

Outcomes are `PASS` / `FAIL` / `WARN`. Only `FAIL` breaks the exit code. The one
expected `WARN` in a busy environment is cost attribution — if ambient
`subscriber_id IS NULL` spend dominates the window, the unit-economics gates
correctly report "no metric" and the harness force-advances that rung and says
so. That is the gate working, not a spin-up defect.

## Clone-Pack

`src/services/clone_pack.py:assemble()` is the single read-only answer to "can
this venture run?" — counties, per-county source coverage, Relay identity, cron
line, ladder stage, and a list of gaps. `is_complete()` is empty-gaps.

Two things it checks that a human eyeballing `--health` misses:

- **Inherited source URLs.** A URL still equal to the template county's was
  never overridden. The scraper resolves a real URL and quietly hits the wrong
  county's portal.
- **Relay identity from the row, not the resolver.** A NULL
  `relay_instantly_campaign_id` resolves to venture #1's campaign through the
  CL3 env fallback, so a fresh venture can look ready while only able to send
  into another business's sequence.

## Onboarding a second venture, end to end

```bash
# 1. CL3 — provision it (see docs/venture-onboarding.md for the detail)
python -m src.services.venture_provisioning --emit-template > venture2.json
$EDITOR venture2.json                       # answer every CHANGE ME
python -m src.services.venture_provisioning --config venture2.json --dry-run
python -m src.services.venture_provisioning --config venture2.json --apply

# 2. Its own Instantly campaign — never share one (ADR 0011)
python -m src.services.relay --setup-email-channel

# 3. Its own cron line — one sweep run = one venture
#    */30 * * * * $PROJECT/scripts/cron/run.sh src.services.relay --sweep --venture venture_two

# 4. Record demand, then climb
python -m src.tasks.venture_ladder_evaluator --venture venture_two --dry-run
```

## Troubleshooting

| Symptom | Cause |
|---|---|
| Every rung red, all values `N/A` | CL4 migration not applied. `PYTHONPATH=. python migrations/apply_cl4_venture_ladder.py` |
| `probe` stuck at `source_coverage_pct` < 100 | Source URLs inherited from the template county. Set `source_url_overrides` for every `REQUIRED_SIGNAL_TYPES` entry and re-apply |
| `pilot` stuck at `relay_ready` red | A NULL Relay column on the row. The resolver hides this; the gate does not |
| `unit_economics` reports "no metric" | Cost attribution below 40%. Expected on a busy fleet — see the harness WARN note |
| Auto-double never fires | Under 200 sends in 14 days, inside the 7-day cooldown, at the 2000 cap, or `send_failure_pct` > 2% |
| Ceiling raised but volume unchanged | Config cache. `maybe_auto_double` flushes it; a manual `UPDATE ventures` does not |
| A venture advanced with red gates | Someone ran with `--advance-spin-up` or `force=True`. Check `actor` in `venture_ladder_events` — a forced advance is recorded as `"<actor> (forced)"` |
